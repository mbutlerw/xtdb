package xtdb.segment

import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.types.pojo.Schema
import xtdb.log.proto.TemporalMetadata
import xtdb.trie.RecencyMicros

private val TEST_TEMPORAL_METADATA: TemporalMetadata =
    TemporalMetadata.newBuilder()
        .setMinValidFrom(Long.MIN_VALUE)
        .setMaxValidFrom(Long.MAX_VALUE)
        .setMinValidTo(Long.MIN_VALUE)
        .setMaxValidTo(Long.MAX_VALUE)
        .setMinSystemFrom(Long.MIN_VALUE)
        .setMaxSystemFrom(Long.MAX_VALUE)
        .build()

class TestSegment(val name: String, val trie: TestTrie) : Segment<TestTrie.Leaf> {
    override val part = null
    override val schema: Schema get() = error("schema")

    class Page(val name: String, val pageIdx: Int) : Segment.Page<TestTrie.Leaf>, Segment.PageMeta<TestTrie.Leaf> {
        override suspend fun loadDataPage(al: BufferAllocator) = error("loadDataPage")
        override val page: Segment.Page<TestTrie.Leaf> get() = this
        override val temporalMetadata get() = TEST_TEMPORAL_METADATA
        override val recency: RecencyMicros get() = Long.MAX_VALUE
        override fun testMetadata() = true
    }

    override fun createPage(leaf: TestTrie.Leaf) = Page(name, leaf.pageIdx)

    override suspend fun openMetadata() = object : Segment.Metadata<TestTrie.Leaf> {
        override val trie = this@TestSegment.trie

        override fun page(leaf: TestTrie.Leaf) = Page(name, leaf.pageIdx)

        override fun testPage(leaf: TestTrie.Leaf) = true
        override fun temporalMetadata(leaf: TestTrie.Leaf) = TEST_TEMPORAL_METADATA
        override fun recency(leaf: TestTrie.Leaf) = Long.MAX_VALUE
        override fun close() = Unit
    }

    override suspend fun loadDataPage(al: BufferAllocator, leaf: TestTrie.Leaf) = error("loadDataPage")
    override fun close() = Unit

    companion object {
        fun seg(name: String, trie: TestTrie.Companion.Builder) =
            TestSegment(name, TestTrie(trie.build()))
    }
}