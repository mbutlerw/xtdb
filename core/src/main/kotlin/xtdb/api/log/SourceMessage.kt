package xtdb.api.log

import com.google.protobuf.ByteString
import xtdb.database.Database
import xtdb.database.DatabaseName
import xtdb.log.proto.SourceLogMessage
import xtdb.log.proto.TrieDetails
import xtdb.log.proto.attachDatabase
import xtdb.log.proto.blockBoundary
import xtdb.log.proto.blockUploaded
import xtdb.log.proto.detachDatabase
import xtdb.log.proto.flushBlock
import xtdb.log.proto.sourceLogMessage
import xtdb.log.proto.resolvedTx
import xtdb.log.proto.triesAdded
import xtdb.storage.StorageEpoch
import xtdb.trie.BlockIndex
import java.nio.ByteBuffer

sealed interface SourceMessage {

    fun encode(): ByteArray

    companion object Codec : MessageCodec<SourceMessage> {
        private const val TX_HEADER: Byte = -1
        private const val LEGACY_FLUSH_BLOCK_HEADER: Byte = 2
        private const val PROTOBUF_HEADER: Byte = 3

        override fun encode(message: SourceMessage): ByteArray = message.encode()

        override fun decode(bytes: ByteArray): SourceMessage? = parse(bytes)

        @JvmStatic
        fun parse(bytes: ByteArray) =
            when (bytes[0]) {
                TX_HEADER -> Tx(bytes)
                LEGACY_FLUSH_BLOCK_HEADER -> null
                PROTOBUF_HEADER -> ProtobufMessage.parse(ByteBuffer.wrap(bytes).position(1))

                else -> throw IllegalArgumentException("Unknown message type: ${bytes[0]}")
            }
    }

    class Tx(val payload: ByteArray) : SourceMessage {
        override fun encode(): ByteArray = payload
    }

    sealed class ProtobufMessage : SourceMessage {
        abstract fun toLogMessage(): SourceLogMessage

        final override fun encode(): ByteArray =
            toLogMessage().let {
                ByteBuffer.allocate(1 + it.serializedSize).apply {
                    put(PROTOBUF_HEADER)
                    put(it.toByteArray())
                    flip()
                }.array()
            }

        companion object {
            fun parse(buffer: ByteBuffer): ProtobufMessage? =
                SourceLogMessage.parseFrom(buffer.duplicate().position(1))
                    .let { msg ->
                        when (msg.messageCase) {
                            SourceLogMessage.MessageCase.FLUSH_BLOCK ->
                                msg.flushBlock
                                    .takeIf { it.hasExpectedBlockIdx() }
                                    ?.expectedBlockIdx
                                    ?.let { FlushBlock(it) }

                            SourceLogMessage.MessageCase.TRIES_ADDED -> msg.triesAdded.let {
                                TriesAdded(it.storageVersion, it.storageEpoch, it.triesList)
                            }

                            SourceLogMessage.MessageCase.ATTACH_DATABASE -> msg.attachDatabase.let {
                                AttachDatabase(it.dbName, Database.Config.fromProto(it.config))
                            }

                            SourceLogMessage.MessageCase.DETACH_DATABASE -> DetachDatabase(msg.detachDatabase.dbName)

                            SourceLogMessage.MessageCase.BLOCK_UPLOADED -> msg.blockUploaded.let {
                                BlockUploaded(it.blockIndex, it.latestProcessedMsgId, it.storageEpoch)
                            }

                            SourceLogMessage.MessageCase.BLOCK_BOUNDARY -> BlockBoundary(msg.blockBoundary.blockIndex)

                            SourceLogMessage.MessageCase.RESOLVED_TX -> msg.resolvedTx.let {
                                val dbOp = when (it.dbOpCase) {
                                    xtdb.log.proto.ResolvedTx.DbOpCase.ATTACH_DATABASE ->
                                        it.attachDatabase.let { a -> DbOp.Attach(a.dbName, Database.Config.fromProto(a.config)) }
                                    xtdb.log.proto.ResolvedTx.DbOpCase.DETACH_DATABASE ->
                                        DbOp.Detach(it.detachDatabase.dbName)
                                    else -> null
                                }
                                ResolvedTx(
                                    it.txId, it.systemTimeMicros, it.committed,
                                    it.error.toByteArray(),
                                    it.tableDataMap.mapValues { (_, v) -> v.toByteArray() },
                                    dbOp
                                )
                            }

                            else -> null
                        }
                    }
        }
    }

    data class FlushBlock(val expectedBlockIdx: BlockIndex?) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            flushBlock = flushBlock { this@FlushBlock.expectedBlockIdx?.let { expectedBlockIdx = it } }
        }
    }

    data class TriesAdded(
        val storageVersion: Int, val storageEpoch: StorageEpoch, val tries: List<TrieDetails>
    ) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            triesAdded = triesAdded {
                storageVersion = this@TriesAdded.storageVersion
                storageEpoch = this@TriesAdded.storageEpoch
                tries.addAll(this@TriesAdded.tries)
            }
        }
    }

    data class AttachDatabase(val dbName: DatabaseName, val config: Database.Config) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            attachDatabase = attachDatabase {
                this.dbName = this@AttachDatabase.dbName
                this.config = this@AttachDatabase.config.serializedConfig
            }
        }
    }

    data class DetachDatabase(val dbName: DatabaseName) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            detachDatabase = detachDatabase {
                this.dbName = this@DetachDatabase.dbName
            }
        }
    }

    data class BlockUploaded(val blockIndex: BlockIndex, val latestProcessedMsgId: MessageId, val storageEpoch: StorageEpoch) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            blockUploaded = blockUploaded {
                this.blockIndex = this@BlockUploaded.blockIndex
                this.latestProcessedMsgId = this@BlockUploaded.latestProcessedMsgId
                this.storageEpoch = this@BlockUploaded.storageEpoch
            }
        }
    }

    data class BlockBoundary(val blockIndex: BlockIndex) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            blockBoundary = blockBoundary { this.blockIndex = this@BlockBoundary.blockIndex }
        }
    }

    sealed interface DbOp {
        data class Attach(val dbName: DatabaseName, val config: Database.Config) : DbOp
        data class Detach(val dbName: DatabaseName) : DbOp
    }

    data class ResolvedTx(
        val txId: MessageId,
        val systemTimeMicros: Long,
        val committed: Boolean,
        val error: ByteArray,
        val tableData: Map<String, ByteArray>,
        val dbOp: DbOp? = null
    ) : ProtobufMessage() {
        override fun toLogMessage() = sourceLogMessage {
            resolvedTx = resolvedTx {
                this.txId = this@ResolvedTx.txId
                this.systemTimeMicros = this@ResolvedTx.systemTimeMicros
                this.committed = this@ResolvedTx.committed
                this.error = ByteString.copyFrom(this@ResolvedTx.error)
                this@ResolvedTx.tableData.forEach { (k, v) ->
                    this.tableData[k] = ByteString.copyFrom(v)
                }
                when (val op = this@ResolvedTx.dbOp) {
                    is DbOp.Attach -> attachDatabase = attachDatabase {
                        this.dbName = op.dbName
                        this.config = op.config.serializedConfig
                    }

                    is DbOp.Detach -> detachDatabase = detachDatabase {
                        this.dbName = op.dbName
                    }

                    null -> {}
                }
            }
        }
    }
}