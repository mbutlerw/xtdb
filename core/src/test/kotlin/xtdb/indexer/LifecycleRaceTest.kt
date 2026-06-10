package xtdb.indexer

import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * Diagnostic only — not part of the regression suite.
 *
 * #5703 root-cause probe.
 *
 * The mechanism (now grounded in the kotlinx source at JobSupport.kt):
 *
 *  - `tryFinalizeFinishingState` performs the state CAS to a boxed-incomplete final state
 *    BEFORE invoking `completeStateFinalization`, which iterates `invokeOnCompletion` handlers.
 *  - `joinInternal` checks `state !is Incomplete` and, if true, returns `false` (no need to wait).
 *  - So `cancelAndJoin` on a job whose state is ALREADY past the CAS returns immediately,
 *    even if the handler iteration is still running on the completing thread.
 *
 * Production scenario: at shutdown, the database job's cancellation cascades to LP's supervisor,
 * which begins completing on worker A. LP.onPartitionsRevoked then calls cancelAndJoin from
 * worker B — but the state CAS has already happened on worker A. join returns immediately,
 * LP continues to openFollowerSystem, all while worker A is still mid-handler closing
 * extSource/replicaProducer. By the time worker A reaches allocator.close, the database's
 * outer cancelAndJoin has returned and the parent allocator has been closed → assertion fires.
 *
 * This test reproduces that race deterministically.
 */
class LifecycleRaceTest {

    @Test
    fun `cancelAndJoin returns immediately when state is already final, while handler is still running`() = runBlocking {
        val supervisor = SupervisorJob()
        val handlerEntered = CountDownLatch(1)
        val joinerMayResume = CountDownLatch(1)
        val handlerCompleted = AtomicBoolean(false)
        val joinReturnedWhileHandlerRunning = AtomicBoolean(false)

        // Handler registered FIRST, like LP's init does.
        // The handler signals when it has entered, then waits for the joiner to act.
        supervisor.invokeOnCompletion {
            handlerEntered.countDown()
            // Block long enough that the joiner has time to call cancelAndJoin and see state-final.
            joinerMayResume.await()
            // Simulate the slow tail of LP's handler (extSource.close + replicaProducer.close + allocator.close)
            Thread.sleep(50)
            handlerCompleted.set(true)
        }

        // One child whose completion will trigger the supervisor's state CAS.
        // Cancel from a separate worker thread so the completing thread (worker A)
        // is distinct from the joining thread (worker B).
        val childScope = CoroutineScope(Dispatchers.Default + supervisor)
        childScope.launch { awaitCancellation() }

        // Worker A: cancel the supervisor. This triggers the cancellation flow that ends in
        // the state CAS and starts the handler iteration.
        CoroutineScope(Dispatchers.Default).launch {
            supervisor.cancel()
        }

        // Wait until the handler has STARTED running (state has been CAS'd to final by now).
        assertTrue(handlerEntered.await(2, java.util.concurrent.TimeUnit.SECONDS),
            "Handler should have started — supervisor cancellation triggered the flow")

        // NOW call cancelAndJoin from another coroutine. State is already final.
        // Per the kotlinx contract: this should return immediately, without waiting for the handler.
        val joinStart = System.nanoTime()
        val joiner = CoroutineScope(Dispatchers.Default).launch {
            supervisor.cancelAndJoin()
            val joinReturnedAt = System.nanoTime()
            // Did join return while the handler is still running?
            joinReturnedWhileHandlerRunning.set(!handlerCompleted.get())
            println("[race] join returned ${(joinReturnedAt - joinStart) / 1_000_000}ms after we started, handler completed by then: ${handlerCompleted.get()}")
        }
        joiner.join()

        // Let the handler complete.
        joinerMayResume.countDown()
        delay(200)

        assertTrue(handlerCompleted.get(), "Handler must eventually complete")
        assertTrue(
            joinReturnedWhileHandlerRunning.get(),
            "RACE REPRODUCED: cancelAndJoin returned while the invokeOnCompletion handler was still running. " +
                    "This is the #5703 mechanism: when state CAS has already landed (because cancellation " +
                    "is already in progress on another thread), cancelAndJoin sees state-final and bails " +
                    "without waiting for handlers to complete. Code calling cancelAndJoin will proceed " +
                    "concurrently with the still-running handler."
        )
    }

    /**
     * Same race, expressed at the database-shutdown shape: the OUTER cancelAndJoin (Database.close())
     * also returns while LP's handler is still running. So closeAll(parent allocator) runs concurrently
     * with the handler, which is the actual production failure mode (handler then hits a closed parent).
     */
    @Test
    fun `outer cancelAndJoin returns before inner handler finishes`() = runBlocking {
        val outerJob = Job()
        val outerScope = CoroutineScope(Dispatchers.Default + outerJob)
        val innerSupervisor = SupervisorJob(outerJob)

        val handlerEntered = CountDownLatch(1)
        val parentClosed = AtomicBoolean(false)
        val handlerCompleted = AtomicBoolean(false)
        val parentClosedWhileHandlerRunning = AtomicBoolean(false)

        // Inner supervisor's handler — like LP's invokeOnCompletion. Slow, modeling
        // extSource.close + replicaProducer.close before the final allocator.close.
        innerSupervisor.invokeOnCompletion {
            handlerEntered.countDown()
            Thread.sleep(200)   // slow real-world work
            handlerCompleted.set(true)
        }

        // Children of the inner supervisor — analogous to persisterJob, extJob, etc.
        outerScope.launch(innerSupervisor) { awaitCancellation() }
        outerScope.launch(innerSupervisor) { awaitCancellation() }

        // Cancel the OUTER job — like Database.close() running runBlocking { job.cancelAndJoin() }.
        // The cascading cancel propagates to innerSupervisor; its children cancel and complete;
        // innerSupervisor enters state finalization on whichever worker finishes last; LP's handler
        // begins running. THEN the outer job's cancelAndJoin completes — and the "parent close"
        // (in Database.close, this is closeAll(allocator)) runs concurrently with the handler.
        runBlocking { outerJob.cancelAndJoin() }

        // Mark "parent allocator closed" the moment outer cancelAndJoin returns.
        parentClosed.set(true)
        parentClosedWhileHandlerRunning.set(!handlerCompleted.get())

        // Let the handler complete.
        delay(500)

        println("[outer] handler completed: ${handlerCompleted.get()}")
        println("[outer] parent was closed while handler still running: ${parentClosedWhileHandlerRunning.get()}")

        assertTrue(handlerCompleted.get(), "Handler must eventually complete")
        assertTrue(
            parentClosedWhileHandlerRunning.get(),
            "RACE REPRODUCED at the database-shutdown shape: the outer cancelAndJoin returned " +
                    "before the inner supervisor's invokeOnCompletion handler finished. In Database.close(), " +
                    "this lets closeAll(allocator) run concurrently with the LP handler, which then " +
                    "calls leader.allocator.close() on a parent that's already been closed."
        )
    }
}
