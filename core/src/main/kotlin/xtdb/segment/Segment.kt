package xtdb.segment

import kotlinx.coroutines.runBlocking
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.arrow.RelationReader
import xtdb.log.proto.TemporalMetadata
import xtdb.trie.HashTrie
import xtdb.trie.RecencyMicros

interface Segment<L> : AutoCloseable {
    val part: ByteArray?

    val schema: Schema

    fun openMetadataSync(): Metadata<L> = runBlocking { openMetadata() }
    suspend fun openMetadata(): Metadata<L>

    interface Metadata<L> : AutoCloseable {
        val trie: HashTrie<L>

        fun page(leaf: L): PageMeta<L>
        fun testPage(leaf: L): Boolean
        fun temporalMetadata(leaf: L): TemporalMetadata
        fun recency(leaf: L): RecencyMicros
    }

    interface Page<L> {
        suspend fun loadDataPage(al: BufferAllocator): RelationReader
    }

    fun createPage(leaf: L): Page<L> = object : Page<L> {
        override suspend fun loadDataPage(al: BufferAllocator) = loadDataPage(al, leaf)
    }

    interface PageMeta<L> {
        val page: Page<L>
        val temporalMetadata: TemporalMetadata
        val recency: RecencyMicros

        fun testMetadata(): Boolean

        companion object {
            fun <L> pageMeta(seg: Segment<L>, meta: Metadata<L>, leaf: L, temporalMetadata: TemporalMetadata, recency: RecencyMicros) =
                object : PageMeta<L> {
                    override val page get() = seg.createPage(leaf)
                    override val temporalMetadata get() = temporalMetadata
                    override val recency get() = recency

                    private var testMetadata: Boolean? = null

                    override fun testMetadata() =
                        testMetadata ?: meta.testPage(leaf).also { testMetadata = it }
                }

            /**
             * Creates a PageMeta with all values pre-extracted - no reference to Metadata.
             * This breaks the reference chain allowing Metadata to be garbage collected.
             */
            fun <L> detachedPageMeta(
                page: Page<L>,
                temporalMetadata: TemporalMetadata,
                recency: RecencyMicros,
                testMetadataResult: Boolean
            ) = object : PageMeta<L> {
                override val page get() = page
                override val temporalMetadata get() = temporalMetadata
                override val recency get() = recency
                override fun testMetadata() = testMetadataResult
            }
        }
    }

    suspend fun loadDataPage(al: BufferAllocator, leaf: L): RelationReader
}