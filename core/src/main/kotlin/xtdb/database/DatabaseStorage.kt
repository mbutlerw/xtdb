package xtdb.database

import xtdb.api.log.Log
import xtdb.metadata.PageMetadata
import xtdb.storage.BufferPool

data class DatabaseStorage(
    val sourceLogOrNull: Log<Log.Message>?,
    val replicaLogOrNull: Log<Log.Message>?,
    val bufferPoolOrNull: BufferPool?,
    val metadataManagerOrNull: PageMetadata.Factory?,
) {
    val sourceLog: Log<Log.Message> get() = sourceLogOrNull ?: error("no source-log")
    val replicaLog: Log<Log.Message> get() = replicaLogOrNull ?: error("no replica-log")
    val bufferPool: BufferPool get() = bufferPoolOrNull ?: error("no buffer-pool")
    val metadataManager: PageMetadata.Factory get() = metadataManagerOrNull ?: error("no metadata-manager")
}
