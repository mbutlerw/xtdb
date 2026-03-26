package xtdb.indexer

import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.apache.arrow.memory.RootAllocator
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import xtdb.RepeatableSimulationTest
import xtdb.SimulationTestBase
import xtdb.api.TransactionKey
import xtdb.api.log.*
import xtdb.api.log.Log
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import xtdb.tx.TxOp
import xtdb.tx.toArrowBytes
import xtdb.util.debug
import xtdb.util.logger
import java.time.Instant
import java.time.ZoneId
import kotlin.time.Duration.Companion.seconds

private val LOG = LogProcessorSimTest::class.logger

@Tag("property")
class LogProcessorSimTest : SimulationTestBase(), LogProcessor.ProcessorFactory {

    private lateinit var allocator: RootAllocator
    private lateinit var bp: BufferPool
    private lateinit var srcLog: SimLog<SourceMessage>
    private lateinit var replicaLog: SimLog<ReplicaMessage>
    private lateinit var dbStorage: DatabaseStorage
    private lateinit var dbState: DatabaseState
    private lateinit var watchers: Watchers
    private lateinit var indexer: Indexer.ForDatabase
    private lateinit var blockUploader: BlockUploader

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        bp = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }

        val dbName = "test-db"

        this.srcLog = SimLog("src", dispatcher, rand)
        this.replicaLog = SimLog("replica", dispatcher, rand)
        this.dbStorage = DatabaseStorage(srcLog, replicaLog, null, bp, null)

        // isFull returns true every N txs, triggering block finishing
        val fullEvery = rand.nextInt(2, 5)
        var txCount = 0

        val liveIndex = mockk<LiveIndex>(relaxed = true) {
            every { latestCompletedTx } returns null
            every { isFull() } answers { ++txCount % fullEvery == 0 }
        }

        this.dbState = DatabaseState(
            dbName,
            BlockCatalog(dbName, null),
            mockk<TableCatalog>(relaxed = true),
            mockk<TrieCatalog>(relaxed = true),
            liveIndex
        )

        this.watchers = Watchers(-1)
        this.indexer = simIndexer()
        this.blockUploader = BlockUploader(dbStorage, dbState, mockk(relaxed = true), null)
    }

    @AfterEach
    fun tearDown() {
        indexer.close()
        replicaLog.close()
        srcLog.close()
        allocator.close()
    }

    /**
     * Simulates the Indexer — always commits, no actual SQL indexing.
     */
    private fun simIndexer() = object : Indexer.ForDatabase {
        override fun indexTx(
            msgId: MessageId, msgTimestamp: Instant, txOps: xtdb.arrow.VectorReader?,
            systemTime: Instant?, defaultTz: ZoneId?, user: String?, userMetadata: Any?
        ): ReplicaMessage.ResolvedTx =
            ReplicaMessage.ResolvedTx(
                txId = msgId,
                systemTime = systemTime ?: msgTimestamp,
                committed = true,
                error = null,
                tableData = emptyMap()
            )

        override fun addTxRow(txKey: TransactionKey, error: Throwable?): ReplicaMessage.ResolvedTx =
            ReplicaMessage.ResolvedTx(
                txId = txKey.txId,
                systemTime = txKey.systemTime,
                committed = error == null,
                error = error,
                tableData = emptyMap()
            )

        override fun close() {}
    }

    override fun openLeaderSystem(
        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        afterSourceMsgId: MessageId,
        afterReplicaMsgId: MessageId,
    ): LogProcessor.LeaderSystem {
        val proc = LeaderLogProcessor(
            allocator, dbStorage, replicaProducer,
            dbState, indexer, watchers,
            emptySet(), null, blockUploader,
            afterSourceMsgId, afterReplicaMsgId
        )
        return object : LogProcessor.LeaderSystem {
            override val proc get() = proc
            override fun close() = proc.close()
        }
    }

    override fun openTransition(
        replicaProducer: Log.AtomicProducer<ReplicaMessage>,
        afterSourceMsgId: MessageId,
        afterReplicaMsgId: MessageId,
    ): LogProcessor.TransitionProcessor =
        TransitionLogProcessor(
            allocator, bp, dbState, dbState.liveIndex,
            blockUploader,
            replicaProducer, watchers, null,
            afterSourceMsgId, afterReplicaMsgId
        )

    override fun openFollower(
        pendingBlock: PendingBlock?,
        afterSourceMsgId: MessageId,
        afterReplicaMsgId: MessageId,
    ): LogProcessor.FollowerProcessor =
        FollowerLogProcessor(
            allocator, bp, dbState,
            mockk<Compactor.ForDatabase>(relaxed = true),
            watchers, null, pendingBlock,
            afterSourceMsgId, afterReplicaMsgId
        )

    fun openLogProcessor() = LogProcessor(this, dbStorage, dbState, blockUploader)

    private fun emptyTx(): SourceMessage.Tx =
        SourceMessage.Tx(
            txOps = emptyList<TxOp>().toArrowBytes(allocator),
            systemTime = null,
            defaultTz = ZoneId.of("UTC"),
            user = null,
            userMetadata = null
        )

    @RepeatableSimulationTest
    fun `single node processes txs and flush-blocks with rebalances`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            openLogProcessor().use { logProc ->
                srcLog.openGroupSubscription(logProc).use {
                    launch(dispatcher) {
                        val totalActions = rand.nextInt(5, 20)
                        LOG.debug("test: will perform $totalActions actions")
                        repeat(totalActions) { _ ->
                            yield()

                            when (rand.nextInt(3)) {
                                0 -> srcLog.appendMessage(emptyTx())
                                1 -> srcLog.appendMessage(SourceMessage.FlushBlock(null))
                                2 -> srcLog.rebalanceTrigger.send(Unit)
                            }
                        }
                    }.join()
                }
            }
        }
}
