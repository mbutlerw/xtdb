package xtdb.database

import clojure.lang.Keyword
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestCoroutineScheduler
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import xtdb.NodeBase
import xtdb.error.Conflict
import xtdb.error.Incorrect

class DatabaseCatalogTest {

    private val ERROR_CODE = Keyword.intern("xtdb.error", "code")

    private fun Conflict.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")
    private fun Incorrect.errCode(): String? = (data.valAt(ERROR_CODE) as? Keyword)?.toString()?.removePrefix(":")

    @Test
    fun `reattach during detach returns transient conflict (#5613)`() {
        val scheduler = TestCoroutineScheduler()
        val dispatcher = StandardTestDispatcher(scheduler)

        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base, dispatcher).use { catalog ->
                catalog.attach("test_db", Database.Config())
                // close is enqueued on the paused dispatcher
                catalog.detach("test_db")

                val ex = assertThrows<Conflict> {
                    catalog.attach("test_db", Database.Config())
                }
                assertEquals("xtdb/db-being-detached", ex.errCode())

                // drain the closer, then re-attach succeeds
                scheduler.advanceUntilIdle()
                catalog.attach("test_db", Database.Config())
            }
        }
    }

    @Test
    fun `attach rejects partitions greater than one (#5557 unit 5)`() {
        val scheduler = TestCoroutineScheduler()
        val dispatcher = StandardTestDispatcher(scheduler)

        NodeBase.openBase(openMeterRegistry = false).use { base ->
            DatabaseCatalog.open(base, dispatcher).use { catalog ->
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
