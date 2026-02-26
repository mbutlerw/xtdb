package xtdb.database

import xtdb.api.log.Log
import xtdb.api.log.SourceMessage
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool

data class DatabaseStorage(
    val sourceLogOrNull: Log<SourceMessage>?,
    val replicaLogOrNull: Log<SourceMessage>?,
    val bufferPoolOrNull: BufferPool?,
    val metadataManagerOrNull: PageMetadata.Factory?,
) {
    val sourceLog: Log<SourceMessage> get() = sourceLogOrNull ?: error("no source-log")
    val replicaLog: Log<SourceMessage> get() = replicaLogOrNull ?: error("no replica-log")
    val bufferPool: BufferPool get() = bufferPoolOrNull ?: error("no buffer-pool")
    val metadataManager: PageMetadata.Factory get() = metadataManagerOrNull ?: error("no metadata-manager")
}
