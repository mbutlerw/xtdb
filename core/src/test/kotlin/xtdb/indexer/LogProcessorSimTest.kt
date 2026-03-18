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
import xtdb.api.log.Log.Companion.tailAll
import xtdb.api.log.MessageId
import xtdb.api.log.ReplicaMessage
import xtdb.api.log.Watchers
import xtdb.catalog.BlockCatalog
import xtdb.catalog.TableCatalog
import xtdb.compactor.Compactor
import xtdb.database.DatabaseState
import xtdb.database.DatabaseStorage
import xtdb.storage.BufferPool
import xtdb.trie.TrieCatalog
import xtdb.tx.TxOp
import xtdb.tx.TxOpts
import xtdb.tx.toBytes
import xtdb.util.closeOnCatch
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
    private lateinit var srcWatchers: Watchers
    private lateinit var indexer: Indexer.ForDatabase
    private lateinit var blockFinisher: BlockFinisher

    @BeforeEach
    fun setUp() {
        allocator = RootAllocator()
        bp = mockk<BufferPool>(relaxed = true) { every { epoch } returns 0 }

        val dbName = "test-db"

        this.srcLog = SimLog("src", dispatcher, rand)
        this.replicaLog = SimLog("replica", dispatcher, rand)
        this.dbStorage = DatabaseStorage(srcLog, replicaLog, bp, null)

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

        this.srcWatchers = Watchers(-1, dispatcher)
        this.indexer = simIndexer()
        this.blockFinisher = BlockFinisher(dbStorage, dbState, mockk(relaxed = true), null)
    }

    @AfterEach
    fun tearDown() {
        indexer.close()
        srcWatchers.close()
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
        replicaProducer: Log.AtomicProducer<ReplicaMessage>, replicaWatchers: Watchers, afterMsgId: MessageId
    ) =
        LeaderLogProcessor(
            allocator, dbStorage, replicaProducer,
            dbState, indexer, srcWatchers, replicaWatchers,
            emptySet(), null, blockFinisher
        ).closeOnCatch { proc ->
            val sub = dbStorage.sourceLog.tailAll(afterMsgId, proc)

            object : LogProcessor.LeaderSystem {
                override val pendingBlock: PendingBlock? get() = proc.pendingBlock

                override fun close() {
                    sub.close()
                    proc.close()
                }
            }
        }

    override fun openTransition(replicaProducer: Log.AtomicProducer<ReplicaMessage>, replicaWatchers: Watchers) =
        TransitionLogProcessor(
            allocator, bp, dbState, dbState.liveIndex,
            BlockFinisher(dbStorage, dbState, mockk<Compactor.ForDatabase>(relaxed = true), null),
            replicaProducer, replicaWatchers, srcWatchers, dbCatalog = null
        )

    override fun openFollower(replicaWatchers: Watchers, pendingBlock: PendingBlock?) =
        FollowerLogProcessor(
            allocator, bp, dbState,
            mockk<Compactor.ForDatabase>(relaxed = true),
            srcWatchers, replicaWatchers, null, pendingBlock
        )

    fun openLogProcessor() =
        LogProcessor(this, dbStorage, dbState, srcWatchers, blockFinisher, coroutineContext = dispatcher)

    private fun emptyTx(): SourceMessage.Tx =
        SourceMessage.Tx(emptyList<TxOp>().toBytes(allocator, TxOpts(defaultTz = ZoneId.of("UTC"))))

    // endregion

    @RepeatableSimulationTest
    fun `single node processes txs and flush-blocks with rebalances`(@Suppress("unused") iteration: Int) =
        runTest(timeout = 5.seconds) {
            openLogProcessor().use { logProc ->
                srcLog.openGroupConsumer(logProc).use {
                    launch(dispatcher) {
                        val totalActions = rand.nextInt(5, 20)
                        LOG.debug("test: will perform $totalActions actions")
                        repeat(totalActions) { _ ->
                            yield()

                            when (rand.nextInt(3)) {
                                0 -> srcLog.appendMessage(emptyTx()).await()
                                1 -> srcLog.appendMessage(SourceMessage.FlushBlock(null)).await()
                                2 -> srcLog.rebalanceTrigger.send(Unit)
                            }
                        }
                    }.join()
                }
            }
        }
}
