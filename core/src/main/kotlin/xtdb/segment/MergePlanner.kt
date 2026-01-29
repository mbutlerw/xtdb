package xtdb.segment

import com.carrotsearch.hppc.ObjectStack
import kotlinx.coroutines.runBlocking
import xtdb.segment.Segment.PageMeta
import xtdb.segment.Segment.PageMeta.Companion.detachedPageMeta
import xtdb.trie.DEFAULT_LEVEL_WIDTH
import xtdb.trie.HashTrie
import xtdb.trie.conjPath
import java.util.function.Predicate

object MergePlanner {

    /**
     * Lightweight snapshot of a trie structure extracted from segment metadata.
     * Captures the trie shape and PageMeta at leaves, allowing metadata files
     * to be closed immediately after extraction.
     *
     * Note: Path is not stored here - it's computed dynamically during traversal
     * via conjPath() and tracked in WorkTask.
     */
    private sealed interface TrieSnapshot {
        data class Branch(val children: List<TrieSnapshot?>) : TrieSnapshot
        data class Leaf(val pageMeta: PageMeta<*>) : TrieSnapshot
    }

    /**
     * Extracts a lightweight TrieSnapshot from segment metadata.
     * Creates DetachedPageMeta objects with all values pre-extracted, breaking
     * the reference chain to Metadata and allowing it to be garbage collected.
     *
     * All metadata-dependent values (temporalMetadata, recency, testMetadata result)
     * are extracted while metadata is still open.
     */
    private fun <L> extractSnapshotFromNode(seg: Segment<L>, segMeta: Segment.Metadata<L>, node: HashTrie.Node<L>): TrieSnapshot {
        val children = node.hashChildren
        return if (children != null) {
            TrieSnapshot.Branch(
                children.map { child -> child?.let { extractSnapshotFromNode(seg, segMeta, it) } }
            )
        } else {
            @Suppress("UNCHECKED_CAST")
            val leaf = node as L
            // Create detached PageMeta with all values pre-extracted - no reference to metadata
            val detachedMeta = detachedPageMeta(
                page = seg.createPage(leaf),
                temporalMetadata = segMeta.temporalMetadata(leaf),
                recency = segMeta.recency(leaf),
                testMetadataResult = segMeta.testPage(leaf)
            )
            TrieSnapshot.Leaf(detachedMeta)
        }
    }

    /**
     * Extension function to extract snapshot from segment.
     * Captures the type parameter L properly to avoid wildcard type issues.
     */
    private suspend fun <L> Segment<L>.extractSnapshot(): TrieSnapshot? =
        openMetadata().use { segMeta ->
            segMeta.trie.rootNode?.let { node -> extractSnapshotFromNode(this, segMeta, node) }
        }

    private class WorkTask(val snapshots: List<TrieSnapshot>, val path: ByteArray)

    @FunctionalInterface
    fun interface PagesFilter {
        fun filterPages(pages: List<PageMeta<*>>): List<PageMeta<*>>?
    }

    @JvmStatic
    fun planSync(
        segments: List<Segment<*>>,
        pathPred: Predicate<ByteArray>?,
        filterPages: PagesFilter = PagesFilter { it }
    ) = runBlocking { plan(segments, pathPred, filterPages) }

    suspend fun plan(
        segments: List<Segment<*>>,
        pathPred: Predicate<ByteArray>?,
        filterPages: PagesFilter = PagesFilter { it }
    ): List<MergeTask> {
        // Filter segments by partition and extract snapshots
        val filteredSegments = segments.filter {
            val part = it.part
            if (part == null || pathPred == null) true
            else pathPred.test(part)
        }

        // Extract snapshots from each segment, closing metadata immediately after
        val snapshots = filteredSegments.mapNotNull { segment ->
            segment.extractSnapshot()
        }

        if (snapshots.isEmpty()) return emptyList()

        val result = mutableListOf<MergeTask>()
        val stack = ObjectStack<WorkTask>()

        stack.push(WorkTask(snapshots, ByteArray(0)))

        while (!stack.isEmpty) {
            val workTask = stack.pop()
            val currentSnapshots = workTask.snapshots
            if (pathPred != null && !pathPred.test(workTask.path)) continue

            val nodeChildren = currentSnapshots.map { snapshot ->
                snapshot to (snapshot as? TrieSnapshot.Branch)?.children
            }

            if (nodeChildren.any { (_, children) -> children != null }) {
                // At least one snapshot is a branch - traverse children
                // Do these in reverse order so they're on the stack in path-prefix order
                for (bucketIdx in (0..<DEFAULT_LEVEL_WIDTH).reversed()) {
                    val newSnapshots = nodeChildren.mapNotNull { (snapshot, children) ->
                        // children == null iff this is a leaf
                        // children[bucketIdx] is null iff this is a branch but without values in this bucket
                        if (children != null) children[bucketIdx] else snapshot
                    }

                    if (newSnapshots.isNotEmpty())
                        stack.push(WorkTask(newSnapshots, conjPath(workTask.path, bucketIdx.toByte())))
                }
            } else {
                // All snapshots are leaves - create merge task
                val pages = currentSnapshots.map { (it as TrieSnapshot.Leaf).pageMeta }
                filterPages.filterPages(pages)
                    ?.takeIf { it.isNotEmpty() }
                    ?.let { filteredPages -> result += MergeTask(filteredPages.map { it.page }, workTask.path) }
            }
        }

        return result
    }
}
