package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.selects.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.database.proto.inMemoryLog
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit.MICROS
import kotlin.time.Duration.Companion.minutes

class InMemoryLog<M> @JvmOverloads constructor(
    private val instantSource: InstantSource,
    override val epoch: Int,
    val partitions: Int = 1,
) : Log<M> {

    init {
        require(partitions >= 1) { "partitions must be >= 1" }
    }

    @SerialName("!InMemory")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
    ) : Log.Factory {
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<SourceMessage>(instantSource, epoch, partitions)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openSourceLog(remotes, partitions))

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            InMemoryLog<ReplicaMessage>(instantSource, epoch, partitions)

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLog(openReplicaLog(remotes, partitions))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.inMemoryLog = inMemoryLog { }
        }
    }

    companion object {
        private const val REPLAY_BUFFER_SIZE = 4096
    }

    private inner class PartitionState {
        val mutex = Mutex()
        val committedCh = MutableSharedFlow<Record<M>>(replay = REPLAY_BUFFER_SIZE)

        @Volatile
        var latestSubmittedOffset: LogOffset = -1
    }

    private val partitionStates: List<PartitionState> = List(partitions) { PartitionState() }

    override fun latestSubmittedOffset(partition: Int): LogOffset = partitionStates[partition].latestSubmittedOffset

    // Mutex ensures offset assignment + emission are atomic,
    // so subscribers always see records in offset order.
    // We only use the instantSource for Tx messages so that the tests
    // that check files can be deterministic.
    override suspend fun appendMessage(message: M, partition: Int): MessageMetadata {
        val state = partitionStates[partition]
        return state.mutex.withLock {
            val ts = if (message is SourceMessage.Tx || message is SourceMessage.LegacyTx) instantSource.instant() else Instant.now()
            val record = Record(epoch, ++state.latestSubmittedOffset, ts.truncatedTo(MICROS), message)
            state.committedCh.emit(record)
            MessageMetadata(epoch, record.logOffset, ts.truncatedTo(MICROS))
        }
    }

    override fun openAtomicProducer(transactionalId: String, partition: Int) = object : AtomicProducer<M> {
        override fun openTx() = object : AtomicProducer.Tx<M> {
            private val buffer = mutableListOf<Pair<M, CompletableDeferred<MessageMetadata>>>()
            private var isOpen = true

            override fun appendMessage(message: M): CompletableDeferred<MessageMetadata> {
                check(isOpen) { "Transaction already closed" }
                return CompletableDeferred<MessageMetadata>()
                    .also { buffer.add(message to it) }
            }

            override fun commit() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                runBlocking {
                    for ((message, res) in buffer) {
                        res.complete(this@InMemoryLog.appendMessage(message, partition))
                    }
                }
            }

            override fun abort() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                buffer.clear()
            }

            override fun close() {
                if (isOpen) abort()
            }
        }

        override fun close() {}
    }

    override fun readLastMessage(partition: Int): M? = null

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        for (rec in partitionStates[partition].committedCh.replayCache) {
            if (rec.logOffset >= toOffset) break
            if (rec.logOffset >= fromOffset) yield(rec)
        }
    }

    override suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: RecordProcessor<M>) = coroutineScope {
        var latestCompletedOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

        val ch = partitionStates[partition].committedCh
            .filter {
                val logOffset = it.logOffset
                check(logOffset <= latestCompletedOffset + 1) {
                    "InMemoryLog emitted out-of-order record (expected ${latestCompletedOffset + 1}, got $logOffset)"
                }
                logOffset > latestCompletedOffset
            }
            .onEach { latestCompletedOffset = it.logOffset }
            .buffer(100)
            .produceIn(this)

        while (isActive) {
            val records = select {
                ch.onReceiveCatching { if (it.isClosed) emptyList() else listOf(it.getOrThrow()) }

                @OptIn(ExperimentalCoroutinesApi::class)
                onTimeout(1.minutes) { emptyList() }
            }

            processor.processRecords(records)
        }
    }

    override suspend fun openGroupSubscription(listener: SubscriptionListener<M>) = coroutineScope {
        for (p in 0 until partitions) {
            val spec = listener.onPartitionAssigned(p) ?: continue
            launch {
                try { tailAll(p, spec.afterMsgId, spec.processor) }
                finally { withContext(NonCancellable) { listener.onPartitionRevoked(p) } }
            }
        }
    }

    override fun close() {}
}
