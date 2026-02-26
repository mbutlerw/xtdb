package xtdb.indexer

import xtdb.api.log.MessageId

interface LogProcessor : AutoCloseable {
    val latestProcessedMsgId: MessageId
    val latestProcessedOffset: Long
    val ingestionError: Throwable?

    override fun close()
}
