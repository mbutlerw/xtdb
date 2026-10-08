package xtdb.api.storage

import xtdb.api.Remote
import xtdb.api.RemoteAlias
import xtdb.api.storage.ObjectStore.Companion.throwMissingKey
import xtdb.api.storage.ObjectStore.StoredObject
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Path
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.Predicate

/**
 * One bucket that outlives the nodes opening it, with S3/GCS/Azure put-if-absent writes and a hook that
 * fails chosen puts.
 */
class SharedObjectStore : ObjectStore.Factory {

    val objects = ConcurrentSkipListMap<Path, ByteBuffer>()

    @Volatile
    var failPut: Predicate<Path> = Predicate { false }

    override val configProto: com.google.protobuf.Any get() = com.google.protobuf.Any.newBuilder().build()

    override fun openObjectStore(storageRoot: Path, remotes: Map<RemoteAlias, Remote>): ObjectStore =
        object : ObjectStore {
            private fun abs(k: Path) = storageRoot.resolve(k)

            override suspend fun getObject(k: Path): ByteBuffer = (objects[abs(k)] ?: throwMissingKey(k)).slice()

            override suspend fun getObject(k: Path, outPath: Path): Path {
                val buf = objects[abs(k)] ?: throwMissingKey(k)
                FileChannel.open(outPath, WRITE, TRUNCATE_EXISTING, CREATE).use { it.write(buf.slice()) }
                return outPath
            }

            override suspend fun putObject(k: Path, buf: ByteBuffer) {
                if (failPut.test(k)) error("injected failure putting $k")
                val copy = ByteBuffer.allocate(buf.remaining()).put(buf.duplicate()).flip()
                objects.putIfAbsent(abs(k), copy)
            }

            private fun list(prefix: Path) =
                objects.tailMap(prefix).entries
                    .takeWhile { it.key.startsWith(prefix) }
                    .map { (k, buf) -> StoredObject(storageRoot.relativize(k), buf.capacity().toLong()) }

            override fun listAllObjects(): Iterable<StoredObject> = list(storageRoot)
            override fun listAllObjects(dir: Path): Iterable<StoredObject> = list(abs(dir))

            override suspend fun copyObject(src: Path, dest: Path) {
                objects.putIfAbsent(abs(dest), (objects[abs(src)] ?: throwMissingKey(src)).slice())
            }

            override suspend fun deleteIfExists(k: Path) {
                objects.remove(abs(k))
            }

            override fun close() {}
        }
}
