package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import xtdb.api.log.Log.AtomicProducer.Companion.withTx
import java.time.InstantSource
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.Executors
import kotlin.time.Duration.Companion.seconds

class InMemoryLogTest {

    private fun txMessage(id: Byte) = SourceMessage.LegacyTx(byteArrayOf(-1, id))

    @Test
    fun `readLastMessage always returns null`() = runTest {
        val log = InMemoryLog.Factory().openSourceLog(emptyMap())
        log.use {
            assertNull(log.readLastMessage())

            log.appendMessage(txMessage(1))

            // Still null because InMemoryLog has no persistence
            assertNull(log.readLastMessage())
        }
    }

    @Test
    fun `commit does not deadlock on a single-threaded dispatcher`() {
        // commit() uses runBlocking internally. The old Channel pipeline needed a
        // Dispatchers.Default thread to process the message, so a single-threaded
        // dispatcher would deadlock: runBlocking blocked the only thread while the
        // pipeline needed it to complete the deferred.
        val dispatcher = Executors.newSingleThreadExecutor().asCoroutineDispatcher()

        try {
            val log = InMemoryLog<ReplicaMessage>(InstantSource.system(), 0)

            runBlocking(dispatcher) {
                withTimeout(5.seconds) {
                    log.openAtomicProducer("test", 0).withTx { tx ->
                        tx.appendMessage(ReplicaMessage.NoOp())
                    }
                }
            }

            assertEquals(0, log.latestSubmittedOffset(0))
        } finally {
            dispatcher.close()
        }
    }

    @Test
    fun `appendMessage writes to its own partition only`() = runTest {
        val log = InMemoryLog<SourceMessage>(InstantSource.system(), 0, partitions = 3)

        log.appendMessage(txMessage(1), partition = 0)
        log.appendMessage(txMessage(2), partition = 2)

        assertEquals(0L, log.latestSubmittedOffset(0))
        assertEquals(-1L, log.latestSubmittedOffset(1))
        assertEquals(0L, log.latestSubmittedOffset(2))
    }

    @Test
    fun `atomic producer is partition-scoped`() = runTest {
        val log = InMemoryLog<SourceMessage>(InstantSource.system(), 0, partitions = 2)

        log.openAtomicProducer("txid", partition = 1).withTx { tx ->
            tx.appendMessage(txMessage(1))
            tx.appendMessage(txMessage(2))
        }

        assertEquals(-1L, log.latestSubmittedOffset(0))
        assertEquals(1L, log.latestSubmittedOffset(1))
    }

    @Test
    fun `openGroupSubscription delivers every partition to a single listener`() = runTest {
        val log = InMemoryLog<SourceMessage>(InstantSource.system(), 0, partitions = 3)

        val assignedPartitions = CopyOnWriteArrayList<Int>()
        val deliveredByPartition = (0 until 3).map { CopyOnWriteArrayList<SourceMessage>() }

        val sub = backgroundScope.launch {
            log.openGroupSubscription(object : Log.SubscriptionListener<SourceMessage> {
                override suspend fun onPartitionAssigned(partition: Int): Log.TailSpec<SourceMessage> {
                    assignedPartitions.add(partition)
                    return Log.TailSpec(-1L) { records ->
                        deliveredByPartition[partition].addAll(records.map { it.message })
                    }
                }
                override suspend fun onPartitionRevoked(partition: Int) {}
            })
        }

        // Let each partition's assignment land before publishing
        while (assignedPartitions.size < 3) yield()

        log.appendMessage(txMessage(10), partition = 0)
        log.appendMessage(txMessage(11), partition = 1)
        log.appendMessage(txMessage(12), partition = 2)

        while (deliveredByPartition.any { it.isEmpty() }) yield()

        assertEquals(setOf(0, 1, 2), assignedPartitions.toSet())
        assertEquals(1, deliveredByPartition[0].size)
        assertEquals(1, deliveredByPartition[1].size)
        assertEquals(1, deliveredByPartition[2].size)
        assertArrayEquals(byteArrayOf(-1, 10), (deliveredByPartition[0][0] as SourceMessage.LegacyTx).payload)
        assertArrayEquals(byteArrayOf(-1, 11), (deliveredByPartition[1][0] as SourceMessage.LegacyTx).payload)
        assertArrayEquals(byteArrayOf(-1, 12), (deliveredByPartition[2][0] as SourceMessage.LegacyTx).payload)

        sub.cancelAndJoin()
    }

    @Test
    fun `partitions less than one is rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            InMemoryLog<SourceMessage>(InstantSource.system(), 0, partitions = 0)
        }
    }
}
