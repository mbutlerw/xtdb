@file:UseSerializers(DurationSerde::class, PathSerde::class)

package xtdb.api.log

import kotlinx.coroutines.*
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import kotlinx.serialization.UseSerializers
import xtdb.DurationSerde
import xtdb.api.PathSerde
import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.log.Log.*
import xtdb.database.proto.DatabaseConfig
import xtdb.util.MsgIdUtil
import xtdb.util.MsgIdUtil.msgIdToOffset
import xtdb.database.proto.localLog
import xtdb.time.InstantUtil.asMicros
import xtdb.time.InstantUtil.fromMicros
import java.io.DataInputStream
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ClosedByInterruptException
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.*
import java.time.Instant
import java.time.InstantSource
import kotlin.coroutines.CoroutineContext
import kotlin.io.path.createParentDirectories
import kotlin.io.path.exists
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.Int.Companion.SIZE_BYTES as INT_BYTES
import kotlin.Long.Companion.SIZE_BYTES as LONG_BYTES

class LocalLog<M>(
    private val rootPath: Path,
    private val codec: MessageCodec<M>,
    private val instantSource: InstantSource,
    override val epoch: Int,
    val useInstantSourceForNonTx: Boolean,
    val partitions: Int = 1,
    coroutineContext: CoroutineContext = Dispatchers.IO,
    private val baseFileName: String = "LOG"
) : Log<M> {

    init {
        require(partitions >= 1) { "partitions must be >= 1" }
    }

    private val scope = CoroutineScope(coroutineContext)
    companion object {
        private fun messageSizeBytes(size: Int) = 1 + INT_BYTES + LONG_BYTES + size + LONG_BYTES

        private const val RECORD_SEPARATOR = 0x1E.toByte()

        private fun readLatestSubmittedOffset(logFilePath: Path): LogOffset {
            if (!logFilePath.exists()) return -1

            return FileChannel.open(logFilePath).use { ch ->
                val chSize = ch.size()

                if (chSize == 0L) return -1

                try {
                    val buf = ByteBuffer.allocateDirect(LONG_BYTES)

                    check(ch.read(buf, chSize - LONG_BYTES) == LONG_BYTES) {
                        "Failed to read last offset in log file"
                    }

                    buf.flip().getLong()
                        .also { offset ->
                            check(offset in 0..<chSize) { "Invalid offset in log file: $offset" }
                            ch.position(offset)
                            DataInputStream(Channels.newInputStream(ch)).use { dataStream ->
                                check(dataStream.readByte() == RECORD_SEPARATOR) {
                                    "log file corrupted - expected record separator at $offset"
                                }

                                val size = dataStream.readInt()
                                check(chSize == offset + messageSizeBytes(size)) {
                                    "log file corrupted - record at $offset specifies size $size, but file size is $chSize"
                                }
                            }
                        }
                } catch (e: Exception) {
                    throw IllegalStateException("Failed to read log file", e)
                }
            }
        }

    }

    private fun FileChannel.readMessage(): Record<M>? {
        val pos = position()
        val headerBuf = ByteBuffer.allocateDirect(1 + INT_BYTES + LONG_BYTES)
            .also { read(it); it.flip() }

        check(headerBuf.get() == RECORD_SEPARATOR) { "log file corrupted at $pos - expected record separator" }
        val size = headerBuf.getInt()

        val message =
            codec.decode(ByteBuffer.allocate(size).also { read(it); it.flip() }.array())
                ?: return null

        return Record(epoch, pos, fromMicros(headerBuf.getLong()), message)
            .also { position(pos + messageSizeBytes(size)) }
    }

    internal data class NewMessage<M>(
        val message: M,
        val onCommit: CompletableDeferred<Record<M>>
    )

    // Single-partition keeps the existing LOG filename for back-compat; multi-partition splits to LOG-0, LOG-1, …
    private fun partitionFilePath(partition: Int) =
        rootPath.resolve(if (partitions == 1) baseFileName else "$baseFileName-$partition")

    private inner class PartitionState(val partition: Int) {
        val logFilePath: Path = partitionFilePath(partition)
        val logFileChannel: FileChannel =
            FileChannel.open(logFilePath.createParentDirectories(), CREATE, WRITE, APPEND)

        val appendCh = Channel<NewMessage<M>>(capacity = 10)
        val mutex = Mutex()

        @Volatile
        var committedCh = MutableSharedFlow<Record<M>>(extraBufferCapacity = 100)

        @Volatile
        var latestSubmittedOffset: LogOffset = readLatestSubmittedOffset(logFilePath)

        fun writeMessages(msgs: List<NewMessage<M>>): Array<Record<M>> {
            val initialOffset = logFileChannel.position()

            try {
                val res = Array(msgs.size) { idx ->
                    val (msg) = msgs[idx]
                    // we only use the instantSource for Tx messages so that the tests
                    // that check files can be deterministic
                    val ts = if (msg is SourceMessage.Tx || msg is SourceMessage.LegacyTx || useInstantSourceForNonTx) instantSource.instant() else Instant.now()
                    val payload = codec.encode(msg)
                    val size = payload.size
                    val offset = logFileChannel.position()

                    logFileChannel.write(
                        ByteBuffer
                            .allocateDirect(messageSizeBytes(size))
                            .run {
                                put(RECORD_SEPARATOR)
                                putInt(size)
                                putLong(ts.asMicros)
                                put(payload)
                                putLong(offset)
                                flip()
                            })

                    Record(epoch, offset, ts, msg)
                }

                logFileChannel.force(true)

                return res
            } catch (t: Throwable) {
                logFileChannel.truncate(initialOffset)
                throw t
            }
        }

        fun start() {
            scope.launch {
                try {
                    while (true) {
                        val msgs = mutableListOf(appendCh.receive())

                        while (true) {
                            if (msgs.size >= 10) break
                            msgs.add(appendCh.tryReceive().getOrNull() ?: break)
                        }

                        val records = writeMessages(msgs)

                        msgs.forEachIndexed { idx, msg ->
                            records[idx].also {
                                mutex.withLock {
                                    committedCh.emit(it)
                                    latestSubmittedOffset = it.logOffset
                                }
                                msg.onCommit.complete(it)
                            }
                        }
                    }
                } catch (_: ClosedByInterruptException) {
                    cancel()
                } catch (_: InterruptedException) {
                    cancel()
                }
            }
        }

        fun close() {
            logFileChannel.close()
        }
    }

    private val partitionStates: List<PartitionState> =
        List(partitions) { PartitionState(it) }
            .also { states -> states.forEach { it.start() } }

    override fun latestSubmittedOffset(partition: Int): LogOffset = partitionStates[partition].latestSubmittedOffset

    override suspend fun appendMessage(message: M, partition: Int): MessageMetadata {
        val state = partitionStates[partition]
        return CompletableDeferred<MessageMetadata>()
            .also { res ->
                scope.launch {
                    val onCommit = CompletableDeferred<Record<M>>()
                    state.appendCh.send(NewMessage(message, onCommit))
                    val record = onCommit.await()
                    res.complete(MessageMetadata(epoch, record.logOffset, record.logTimestamp))
                }
            }
            .await()
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
                        res.complete(this@LocalLog.appendMessage(message, partition))
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

    override fun readLastMessage(partition: Int): M? {
        val state = partitionStates[partition]
        if (state.latestSubmittedOffset < 0) return null

        return FileChannel.open(state.logFilePath).use { ch ->
            ch.position(state.latestSubmittedOffset)
            ch.readMessage()?.message
        }
    }

    override fun readRecords(partition: Int, fromMsgId: MessageId, toMsgId: MessageId) = sequence {
        if (MsgIdUtil.msgIdToEpoch(fromMsgId) != epoch || MsgIdUtil.msgIdToEpoch(toMsgId) != epoch) return@sequence
        val state = partitionStates[partition]
        val fromOffset = msgIdToOffset(fromMsgId)
        val toOffset = msgIdToOffset(toMsgId)
        if (fromOffset > state.latestSubmittedOffset || fromOffset >= toOffset) return@sequence

        FileChannel.open(state.logFilePath).use { ch ->
            ch.position(fromOffset)
            while (ch.position() < ch.size()) {
                val record = ch.readMessage() ?: continue
                if (record.logOffset >= toOffset) break
                yield(record)
            }
        }
    }

    override suspend fun tailAll(partition: Int, afterMsgId: MessageId, processor: RecordProcessor<M>) = coroutineScope {
        val state = partitionStates[partition]
        var latestCompletedOffset = MsgIdUtil.afterMsgIdToOffset(epoch, afterMsgId)

        val ch = Channel<Record<M>>(100)

        launch {
            state.committedCh
                .onSubscription {
                    val targetOffset = state.mutex.withLock { state.latestSubmittedOffset }
                    if (targetOffset < 0) return@onSubscription

                    val catchUpRecords = runInterruptible {
                        FileChannel.open(state.logFilePath).use { fileCh ->
                            val latestCompleted = latestCompletedOffset
                            if (latestCompleted >= 0) {
                                fileCh.position(latestCompleted)
                                fileCh.readMessage()
                            }

                            buildList {
                                while (fileCh.position() <= targetOffset) {
                                    fileCh.readMessage()?.let { add(it) }
                                }
                            }
                        }
                    }

                    for (record in catchUpRecords) {
                        ch.send(record)
                    }

                    latestCompletedOffset = targetOffset
                }
                .onEach {
                    if (it.logOffset > latestCompletedOffset) {
                        latestCompletedOffset = it.logOffset
                        ch.send(it)
                    }
                }
                .onCompletion { ch.close() }
                .catch {
                    try {
                        throw it
                    } catch (_: ClosedByInterruptException) {
                        throw CancellationException()
                    } catch (_: InterruptedException) {
                        throw CancellationException()
                    }
                }
                .collect()
        }

        while (isActive) {
            val msg = withTimeoutOrNull(1.minutes) {
                ch.receiveCatching().let { if (it.isClosed) null else it.getOrThrow() }
            }
            if (msg != null) processor.processRecords(listOf(msg))
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

    override fun close() {
        runBlocking { withTimeout(5.seconds) { scope.coroutineContext.job.cancelAndJoin() } }
        partitionStates.forEach { it.close() }
    }

    /**
     * Used to set configuration options for a local directory based XTDB Log.
     *
     * Example usage, as part of a node config:
     * ```kotlin
     * Xtdb.openNode {
     *    log = localLog(Path("test-path")) {
     *      instantSource = InstantSource.system()
     *      bufferSize = 4096
     *      pollSleepDuration = Duration.ofMillis(100)
     *    }
     *    ...
     * }
     * ```
     */
    @SerialName("!Local")
    @Serializable
    data class Factory @JvmOverloads constructor(
        val path: Path,
        @Transient var instantSource: InstantSource = InstantSource.system(),
        var epoch: Int = 0,
        var useInstantSourceForNonTx: Boolean = false,
        @Transient var coroutineContext: CoroutineContext = Dispatchers.IO
    ) : Log.Factory {

        @Suppress("unused")
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun epoch(epoch: Int) = apply { this.epoch = epoch }
        fun useInstantSourceForNonTx() = apply { this.useInstantSourceForNonTx = true }
        fun coroutineContext(coroutineContext: CoroutineContext) = apply { this.coroutineContext = coroutineContext }

        override fun openSourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            LocalLog(path, SourceMessage.Codec, instantSource, epoch, useInstantSourceForNonTx, partitions, coroutineContext)

        override fun openReadOnlySourceLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLocalLog(path, SourceMessage.Codec, epoch, partitions, coroutineContext)

        override fun openReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            LocalLog(path, ReplicaMessage.Codec, instantSource, epoch, useInstantSourceForNonTx, partitions, coroutineContext, baseFileName = "REPLICA_LOG")

        override fun openReadOnlyReplicaLog(remotes: Map<RemoteAlias, Remote>, partitions: Int) =
            ReadOnlyLocalLog(path, ReplicaMessage.Codec, epoch, partitions, coroutineContext, baseFileName = "REPLICA_LOG")

        override fun writeTo(dbConfig: DatabaseConfig.Builder) {
            dbConfig.localLog = localLog {
                this.path = this@Factory.path.toString()
            }
        }
    }
}
