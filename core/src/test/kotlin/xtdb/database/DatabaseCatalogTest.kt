package xtdb.database

import clojure.lang.Keyword
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase
import xtdb.error.Conflict
import xtdb.error.Incorrect
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit.SECONDS

class DatabaseCatalogTest {

    private val ERROR_CODE = Keyword.intern("xtdb.error", "code")

    private fun Conflict.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")
    private fun Incorrect.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")

    @Test
    fun `reattach during detach returns transient conflict (#5613)`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            // Pin the closer to a single-thread dispatcher whose one thread we hold with `gate`, so the
            // detaching database is held mid-teardown (isClosing, not yet removed) for as long as we need
            // to observe the conflict. The teardown otherwise completes on a background thread and would
            // race the re-attach to win the observation.
            val gate = CountDownLatch(1)
            val closerExecutor = Executors.newSingleThreadExecutor()
            try {
                closerExecutor.execute {
                    try { gate.await() } catch (e: InterruptedException) { Thread.currentThread().interrupt() }
                }

                DatabaseCatalog.open(base, closerExecutor.asCoroutineDispatcher()).use { catalog ->
                    catalog.attach("test_db", Database.Config())
                    try {
                        // The teardown coroutine is queued behind the gate, so the database stays in
                        // `databases` with isClosing = true and the re-attach sees the transient conflict.
                        catalog.detach("test_db")

                        val ex = assertThrows<Conflict> {
                            catalog.attach("test_db", Database.Config())
                        }
                        assertEquals("xtdb/db-being-detached", ex.errCode())
                    } finally {
                        // Release before `use` closes the catalog — close() joins the closer's children.
                        gate.countDown()
                    }

                    // With teardown released, the name frees up and re-attach eventually succeeds.
                    val deadline = System.nanoTime() + SECONDS.toNanos(10)
                    while (true) {
                        try {
                            catalog.attach("test_db", Database.Config()); break
                        } catch (e: Conflict) {
                            check(System.nanoTime() < deadline) { "detach did not complete within 10s" }
                            Thread.sleep(10)
                        }
                    }
                }
            } finally {
                gate.countDown()
                closerExecutor.shutdownNow()
            }
        }
    }

    @Test
    fun `attach rejects partitions greater than one (#5557 unit 5)`() {
        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base, Dispatchers.Default).use { catalog ->
                val ex = assertThrows<Incorrect> {
                    catalog.attach("multi_db", Database.Config(partitions = 4))
                }
                // DatabaseCatalog wraps Database.open failures with its own code;
                // the multi-partition rejection rides on the cause.
                val cause = ex.cause as? Incorrect
                assertEquals("xtdb/multi-partition-not-yet-enabled", cause?.errCode())
            }
        }
    }

    @Test
    fun `Database#Config partitions round-trips through proto`() {
        val cfg = Database.Config(partitions = 7)
        val proto = cfg.serializedConfig
        assertEquals(7, proto.partitions)

        val restored = Database.Config.fromProto(proto)
        assertEquals(7, restored.partitions)
    }

    @Test
    fun `legacy proto with no partitions field reads back as single-partition`() {
        // Pre-#5557 proto bytes have partitions = 0 (the proto default); the Kotlin layer
        // must coerce that to 1 so persisted configs round-trip as single-partition.
        val legacyProto = Database.Config().serializedConfig.toBuilder()
            .clearPartitions()
            .build()
        assertEquals(0, legacyProto.partitions)
        assertEquals(1, Database.Config.fromProto(legacyProto).partitions)
    }

    @Test
    fun `Database#Config rejects partitions less than one`() {
        val ex = assertThrows<IllegalArgumentException> { Database.Config(partitions = 0) }
        assertTrue(ex.message!!.contains("partitions must be >= 1"))
    }
}
