//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for {@link BaseTable#awaitUpdate()} and {@link BaseTable#awaitUpdate(long)}.
 */
public class BaseTableAwaitUpdateTest {

    /**
     * Timeout used for waits that we expect to elapse. Short enough to keep these tests fast, long enough that we can
     * reliably distinguish "waited for the timeout" from "returned immediately".
     */
    private static final long ELAPSING_TIMEOUT_MILLIS = 250;

    /**
     * Timeout used for waits that we expect to be satisfied by a notification. Long enough that a slow machine won't
     * cause a spurious timeout.
     */
    private static final long SATISFIED_TIMEOUT_MILLIS = 60_000;

    /**
     * Timeout used when joining threads that we expect to have finished, or to finish promptly.
     */
    private static final long JOIN_TIMEOUT_MILLIS = 30_000;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private ControlledUpdateGraph updateGraph;
    private QueryTable source;
    private QueryTable unrelated;
    private long nextRowKey;

    @Before
    public void setUp() {
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        source = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Sentinel", 0));
        unrelated = TstUtils.testRefreshingTable(i(0).toTracking(), intCol("Sentinel", 0));
        nextRowKey = 1;
    }

    @Test
    public void testTimeoutElapsesWithoutNotification() {
        final Waiter waiter = Waiter.timed(source, ELAPSING_TIMEOUT_MILLIS);
        waiter.join();
        assertEquals(Boolean.FALSE, waiter.result);
        assertWaitedForTimeout(waiter, ELAPSING_TIMEOUT_MILLIS);
    }

    @Test
    public void testNonPositiveTimeoutDoesNotWait() {
        for (final long timeoutMillis : new long[] {0, -1}) {
            final Waiter waiter = Waiter.timed(source, timeoutMillis);
            waiter.join();
            assertEquals("timeoutMillis=" + timeoutMillis, Boolean.FALSE, waiter.result);
            assertTrue("timeoutMillis=" + timeoutMillis + ", elapsedNanos=" + waiter.elapsedNanos,
                    waiter.elapsedNanos < TimeUnit.MILLISECONDS.toNanos(ELAPSING_TIMEOUT_MILLIS));
        }
    }

    @Test
    public void testTimedWaitReturnsOnNotification() {
        final Waiter waiter = Waiter.timed(source, SATISFIED_TIMEOUT_MILLIS);
        waiter.awaitBlocked();
        tick(source);
        waiter.join();
        assertEquals(Boolean.TRUE, waiter.result);
    }

    @Test
    public void testUntimedWaitReturnsOnNotification() {
        final Waiter waiter = Waiter.untimed(source);
        waiter.awaitBlocked();

        // An update to some other table on the same update graph must not terminate the wait.
        tick(unrelated);
        assertTrue("waiter finished without an update to source", waiter.isAlive());

        tick(source);
        waiter.join();
    }

    @Test
    public void testTimedWaitIgnoresCyclesWithoutNotification() {
        final Waiter waiter = Waiter.timed(source, ELAPSING_TIMEOUT_MILLIS);
        waiter.awaitBlocked();
        updateGraph.runWithinUnitTestCycle(() -> {
        });
        tick(unrelated);
        waiter.join();
        assertEquals(Boolean.FALSE, waiter.result);
        assertWaitedForTimeout(waiter, ELAPSING_TIMEOUT_MILLIS);
    }

    /**
     * A notification delivered while the waiter is still trying to acquire the exclusive lock must not be missed. This
     * is the common case for the unit test update graph, which holds the exclusive lock for the duration of a cycle.
     */
    @Test
    public void testNotificationWhileAcquiringLockIsNotMissed() {
        final Waiter waiter;
        updateGraph.startCycleForUnitTests();
        boolean cycleCompleted = false;
        try {
            waiter = Waiter.timed(source, SATISFIED_TIMEOUT_MILLIS);
            // The cycle holds the exclusive lock, so the waiter cannot be doing anything but waiting for it.
            waiter.awaitBlocked();
            addRow(source);
            updateGraph.completeCycleForUnitTests();
            cycleCompleted = true;
        } finally {
            if (!cycleCompleted) {
                updateGraph.completeCycleForUnitTests();
            }
        }
        waiter.join();
        assertEquals(Boolean.TRUE, waiter.result);
    }

    /**
     * Time spent waiting for the exclusive lock must be charged against the caller's timeout, rather than blocking
     * indefinitely before beginning to wait for a notification.
     */
    @Test
    public void testLockWaitIsChargedAgainstTimeout() throws InterruptedException {
        final CountDownLatch locked = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final Thread holder = new Thread(() -> {
            try {
                updateGraph.exclusiveLock().doLockedInterruptibly(() -> {
                    locked.countDown();
                    release.await();
                });
            } catch (InterruptedException ignored) {
            }
        }, "exclusive-lock-holder");
        holder.setDaemon(true);
        holder.start();
        try {
            locked.await();
            // The holder thread will keep the exclusive lock until we count down release, below, so the waiter can
            // never acquire it. It must nonetheless return (false) once its timeout has elapsed.
            final Waiter waiter = Waiter.timed(source, ELAPSING_TIMEOUT_MILLIS);
            waiter.join();
            assertEquals(Boolean.FALSE, waiter.result);
            assertWaitedForTimeout(waiter, ELAPSING_TIMEOUT_MILLIS);
        } finally {
            release.countDown();
            holder.join(JOIN_TIMEOUT_MILLIS);
            assertFalse(holder.isAlive());
        }
    }

    /**
     * The caller's exclusive lock must be intact when {@code awaitUpdate} returns, even though awaiting releases it.
     */
    @Test
    public void testWaitWhileHoldingExclusiveLock() {
        final boolean[] heldBefore = new boolean[1];
        final boolean[] heldAfter = new boolean[1];
        final Waiter waiter = new Waiter("await-holding-lock",
                (resultSink) -> updateGraph.exclusiveLock().doLockedInterruptibly(() -> {
                    heldBefore[0] = updateGraph.exclusiveLock().isHeldByCurrentThread();
                    resultSink.accept(source.awaitUpdate(SATISFIED_TIMEOUT_MILLIS));
                    heldAfter[0] = updateGraph.exclusiveLock().isHeldByCurrentThread();
                }));
        waiter.awaitBlocked();
        tick(source);
        waiter.join();
        assertEquals(Boolean.TRUE, waiter.result);
        assertTrue("exclusive lock was not held before awaitUpdate", heldBefore[0]);
        assertTrue("exclusive lock was not held after awaitUpdate returned", heldAfter[0]);
    }

    @Test
    public void testUntimedWaitReturnsOnFailure() {
        final Waiter waiter = Waiter.untimed(source);
        waiter.awaitBlocked();
        failSource();
        waiter.join();
    }

    @Test
    public void testTimedWaitReturnsOnFailure() {
        final Waiter waiter = Waiter.timed(source, SATISFIED_TIMEOUT_MILLIS);
        waiter.awaitBlocked();
        failSource();
        waiter.join();
        assertEquals(Boolean.TRUE, waiter.result);
    }

    @Test
    public void testWaitOnFailedTableDoesNotBlock() {
        failSource();
        assertTrue(source.isFailed());

        final Waiter timedWaiter = Waiter.timed(source, SATISFIED_TIMEOUT_MILLIS);
        timedWaiter.join();
        assertEquals(Boolean.TRUE, timedWaiter.result);
        assertTrue("elapsedNanos=" + timedWaiter.elapsedNanos,
                timedWaiter.elapsedNanos < TimeUnit.MILLISECONDS.toNanos(SATISFIED_TIMEOUT_MILLIS));

        final Waiter untimedWaiter = Waiter.untimed(source);
        untimedWaiter.join();
    }

    @Test
    public void testInterruptDuringWaitReleasesLock() {
        final Waiter waiter = Waiter.timed(source, SATISFIED_TIMEOUT_MILLIS);
        waiter.awaitBlocked();
        waiter.thread.interrupt();
        waiter.joinIgnoringError();
        assertNull(waiter.result);
        assertTrue("error=" + waiter.error, waiter.error instanceof InterruptedException);
        // The exclusive lock must not have been leaked; if it was, this cycle would never start.
        tick(source);
    }

    private void addRow(final QueryTable table) {
        final long rowKey = nextRowKey++;
        TstUtils.addToTable(table, i(rowKey), intCol("Sentinel", (int) rowKey));
        table.notifyListeners(i(rowKey), i(), i());
    }

    private void tick(final QueryTable table) {
        updateGraph.runWithinUnitTestCycle(() -> addRow(table));
    }

    private void failSource() {
        updateGraph.runWithinUnitTestCycle(
                () -> source.notifyListenersOnError(new RuntimeException("test failure"), null));
    }

    private static void assertWaitedForTimeout(final Waiter waiter, final long timeoutMillis) {
        assertTrue("elapsedNanos=" + waiter.elapsedNanos + ", timeoutMillis=" + timeoutMillis,
                waiter.elapsedNanos >= TimeUnit.MILLISECONDS.toNanos(timeoutMillis));
    }

    private static void waitForState(final Thread thread, final Thread.State... acceptable) {
        final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(JOIN_TIMEOUT_MILLIS);
        while (true) {
            final Thread.State state = thread.getState();
            for (final Thread.State candidate : acceptable) {
                if (state == candidate) {
                    return;
                }
            }
            if (System.nanoTime() - deadlineNanos > 0) {
                fail("Thread " + thread.getName() + " never reached an acceptable state, state=" + state);
            }
            Thread.yield();
        }
    }

    private interface WaitAction {
        void await(Consumer<Boolean> resultSink) throws InterruptedException;
    }

    /**
     * Runs an {@code awaitUpdate} invocation on its own thread, recording the outcome.
     */
    private static class Waiter {

        private final Thread thread;

        /**
         * The result of {@link Table#awaitUpdate(long)}, or {@code null} if the wait is unfinished, threw, or was the
         * {@link Table#awaitUpdate()} overload.
         */
        private volatile Boolean result;
        private volatile long elapsedNanos = -1;
        private volatile Throwable error;

        private static Waiter untimed(final QueryTable table) {
            return new Waiter("await-update", (resultSink) -> table.awaitUpdate());
        }

        private static Waiter timed(final QueryTable table, final long timeoutMillis) {
            return new Waiter("await-update-" + timeoutMillis,
                    (resultSink) -> resultSink.accept(table.awaitUpdate(timeoutMillis)));
        }

        private Waiter(final String name, final WaitAction action) {
            thread = new Thread(() -> {
                final long startNanos = System.nanoTime();
                try {
                    action.await((theResult) -> result = theResult);
                } catch (Throwable t) {
                    error = t;
                } finally {
                    elapsedNanos = System.nanoTime() - startNanos;
                }
            }, name);
            thread.setDaemon(true);
            thread.start();
        }

        /**
         * Wait until this waiter's thread is parked, which guarantees that it has observed the table's notification
         * step, and is either waiting for the exclusive lock or waiting for a notification.
         */
        private void awaitBlocked() {
            waitForState(thread, Thread.State.WAITING, Thread.State.TIMED_WAITING);
        }

        private boolean isAlive() {
            return thread.isAlive();
        }

        /**
         * Join this waiter's thread, and re-throw any error it encountered.
         */
        private void join() {
            joinIgnoringError();
            if (error != null) {
                throw new AssertionError("Waiter " + thread.getName() + " failed", error);
            }
        }

        /**
         * Join this waiter's thread, without regard for whether it completed normally.
         */
        private void joinIgnoringError() {
            try {
                thread.join(JOIN_TIMEOUT_MILLIS);
            } catch (InterruptedException e) {
                throw new AssertionError("Interrupted while joining waiter " + thread.getName(), e);
            }
            if (thread.isAlive()) {
                fail("Waiter " + thread.getName() + " did not finish within " + JOIN_TIMEOUT_MILLIS + " ms");
            }
        }
    }
}
