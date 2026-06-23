package xtdb.database

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import xtdb.diagnostics.TeardownStall
import xtdb.diagnostics.TeardownStallProbe
import java.time.Duration
import java.util.concurrent.TimeUnit

class TeardownEscapeTest {

    private val originalProbe = TeardownStall.probe

    @AfterEach
    fun restoreProbe() {
        TeardownStall.probe = originalProbe
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `joins cleanly when the job tree honours cancellation`() {
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        val job = scope.launch { awaitCancellation() }

        assertFalse(awaitJobTeardown(job, Duration.ofSeconds(10), "clean-db"))
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    fun `escapes and fires the stall probe when the tree ignores cancellation`() {
        var reason: String? = null
        // recorder rather than the real dump, so the assertion is clean and CI logs stay quiet
        TeardownStall.probe = TeardownStallProbe { reason = it }

        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
        // NonCancellable awaitCancellation never completes — cancelAndJoin alone would hang forever
        val job = scope.launch { withContext(NonCancellable) { awaitCancellation() } }

        assertTrue(awaitJobTeardown(job, Duration.ofMillis(250), "wedged-db"))
        assertNotNull(reason)
        assertTrue(reason!!.contains("wedged-db"), "stall reason should name the database: $reason")
    }
}
