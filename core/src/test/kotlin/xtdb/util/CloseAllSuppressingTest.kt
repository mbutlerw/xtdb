package xtdb.util

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class CloseAllSuppressingTest {

    private class Spy(val onClose: () -> Unit = {}) : AutoCloseable {
        var closed = false
        override fun close() {
            closed = true
            onClose()
        }
    }

    @Test
    fun `closes every element`() {
        val a = Spy(); val b = Spy(); val c = Spy()
        listOf(a, b, c).closeAllSuppressing()
        assertTrue(a.closed && b.closed && c.closed)
    }

    @Test
    fun `closes the rest even after one throws, rethrowing first with the others suppressed`() {
        val boomA = RuntimeException("a")
        val boomC = RuntimeException("c")
        val a = Spy { throw boomA }
        val b = Spy()
        val c = Spy { throw boomC }

        val thrown = assertThrows<RuntimeException> { listOf(a, b, c).closeAllSuppressing() }

        assertTrue(a.closed && b.closed && c.closed, "every element should be closed despite failures")
        assertSame(boomA, thrown, "first failure is rethrown")
        assertEquals(listOf(boomC), thrown.suppressed.toList(), "later failures are suppressed onto the first")
    }
}
