package xtdb.test.log

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.Transient
import xtdb.api.log.Log
import xtdb.api.log.LogClusterAlias
import xtdb.api.log.LogOffset
import xtdb.api.log.ReadOnlyLog
import xtdb.database.proto.DatabaseConfig
import java.time.Instant
import java.time.InstantSource
import java.time.temporal.ChronoUnit
import java.util.concurrent.CompletableFuture

class RecordingLog<M>(private val instantSource: InstantSource, messages: List<M>) : Log<M> {
    override val epoch = 0
    val messages = messages.toMutableList()

    @SerialName("!Recording")
    @Serializable
    data class Factory(
        @Transient var instantSource: InstantSource = InstantSource.system()
    ) : Log.Factory {
        var messages: List<Log.Message> = emptyList()
        fun instantSource(instantSource: InstantSource) = apply { this.instantSource = instantSource }
        fun messages(messages: List<Log.Message>) = apply { this.messages = messages }

        override fun openLog(clusters: Map<LogClusterAlias, Log.Cluster>) = RecordingLog(instantSource, messages)

        override fun openReadOnlyLog(clusters: Map<LogClusterAlias, Log.Cluster>) =
            ReadOnlyLog(openLog(clusters))

        override fun writeTo(dbConfig: DatabaseConfig.Builder) = Unit
    }

    @Volatile
    override var latestSubmittedOffset: LogOffset = -1
        private set

    override fun appendMessage(message: M): CompletableFuture<Log.MessageMetadata> {
        messages.add(message)

        val ts = if (message is Log.Message.Tx) instantSource.instant() else Instant.now()

        return CompletableFuture.completedFuture(
            Log.MessageMetadata(
                ++latestSubmittedOffset,
                ts.truncatedTo(ChronoUnit.MICROS)
            )
        )
    }

    override fun readLastMessage(): M? = messages.lastOrNull()

    override fun openAtomicProducer(transactionalId: String) = object : Log.AtomicProducer<M> {
        override fun openTx() = object : Log.AtomicProducer.Tx<M> {
            private val buffer = mutableListOf<Pair<M, CompletableFuture<Log.MessageMetadata>>>()
            private var isOpen = true

            override fun appendMessage(message: M): CompletableFuture<Log.MessageMetadata> {
                check(isOpen) { "Transaction already closed" }
                val future = CompletableFuture<Log.MessageMetadata>()
                buffer.add(message to future)
                return future
            }

            override fun commit() {
                check(isOpen) { "Transaction already closed" }
                isOpen = false
                for ((message, future) in buffer) {
                    future.complete(this@RecordingLog.appendMessage(message).join())
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

    override fun tailAll(
        subscriber: Log.Subscriber<M>,
        latestProcessedOffset: LogOffset
    ): Log.Subscription = error("tailAll")

    override fun subscribe(subscriber: Log.GroupSubscriber<M>): Log.Subscription {
        val offsets = subscriber.onPartitionsAssigned(listOf(0))
        val nextOffset = offsets[0] ?: 0L
        val subscription = tailAll(subscriber, nextOffset - 1)
        return Log.Subscription {
            subscription.close()
            subscriber.onPartitionsRevoked(listOf(0))
        }
    }

    override fun close() = Unit
}
