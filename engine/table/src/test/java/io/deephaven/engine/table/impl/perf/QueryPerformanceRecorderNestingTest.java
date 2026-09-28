//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.perf;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import org.junit.Rule;
import org.junit.Test;

public class QueryPerformanceRecorderNestingTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    /**
     * A suspended query may be resumed on a thread that is already running another query: an RPC that fails an export
     * synchronously inside {@code submit()} completes an unrelated request, whose recorder resumes to finish. The inner
     * query must run under its own recorder and hand the thread back to the outer query afterwards.
     */
    @Test
    public void testResumeWhileAnotherQueryOwnsTheThread() {
        final QueryPerformanceRecorder outer =
                QueryPerformanceRecorder.newQuery("outer", null, QueryPerformanceNugget.DEFAULT_FACTORY);
        final QueryPerformanceRecorder inner =
                QueryPerformanceRecorder.newQuery("inner", null, QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = inner.startQuery()) {
            inner.suspendQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);

        try (final SafeCloseable ignored = outer.startQuery()) {
            assertCurrentRecorder(outer);
            try (final SafeCloseable ignored2 = inner.resumeQuery()) {
                assertCurrentRecorder(inner);
                inner.endQuery();
                // ending the inner query hands the thread back at once, not when its scope closes: the outer query is
                // still running, so no new query may start here in the meantime
                assertCurrentRecorder(outer);
                assertCannotStartQuery("a new query cannot start while the outer query owns the thread");
                assertCurrentRecorder(outer);
            }
            // the outer query owns the thread again, so it can be suspended and ended as usual
            assertCurrentRecorder(outer);
            outer.suspendQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);

        try (final SafeCloseable ignored = outer.resumeQuery()) {
            assertCurrentRecorder(outer);
            outer.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    @Test
    public void testSuspendingTheInnerQueryHandsTheThreadBackAtOnce() {
        final QueryPerformanceRecorder outer = newQuery("outer");
        final QueryPerformanceRecorder inner = suspendedQuery("inner");

        try (final SafeCloseable ignored = outer.startQuery()) {
            try (final SafeCloseable ignored2 = inner.resumeQuery()) {
                assertCurrentRecorder(inner);
                inner.suspendQuery();
                assertCurrentRecorder(outer);
                assertCannotStartQuery("a new query cannot start while the outer query owns the thread");
            }
            assertCurrentRecorder(outer);
            outer.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);

        // the inner query is still suspended and finishes as usual
        try (final SafeCloseable ignored = inner.resumeQuery()) {
            assertCurrentRecorder(inner);
            inner.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    @Test
    public void testNestedResumesHandTheThreadBackInOrder() {
        final QueryPerformanceRecorder a = newQuery("a");
        final QueryPerformanceRecorder b = suspendedQuery("b");
        final QueryPerformanceRecorder c = suspendedQuery("c");

        try (final SafeCloseable ignored = a.startQuery()) {
            try (final SafeCloseable ignored2 = b.resumeQuery()) {
                assertCurrentRecorder(b);
                try (final SafeCloseable ignored3 = c.resumeQuery()) {
                    assertCurrentRecorder(c);
                    c.endQuery();
                    assertCurrentRecorder(b);
                }
                assertCurrentRecorder(b);
                b.endQuery();
                assertCurrentRecorder(a);
            }
            assertCurrentRecorder(a);
            a.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    @Test
    public void testLeavingTheInnerScopeWithoutEndingItRestoresTheOuterQuery() {
        final QueryPerformanceRecorder outer = newQuery("outer");
        final QueryPerformanceRecorder inner = suspendedQuery("inner");

        try (final SafeCloseable ignored = outer.startQuery()) {
            try (final SafeCloseable ignored2 = inner.resumeQuery()) {
                assertCurrentRecorder(inner);
                // neither ended nor suspended, as when an exception escapes the resumed work
            }
            assertCurrentRecorder(outer);
            outer.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    @Test
    public void testStaleCloseableDoesNotUninstallALaterResume() {
        final QueryPerformanceRecorder outer = newQuery("outer");
        final QueryPerformanceRecorder inner = suspendedQuery("inner");

        try (final SafeCloseable ignored = outer.startQuery()) {
            final SafeCloseable first = inner.resumeQuery();
            inner.suspendQuery();
            try (final SafeCloseable ignored2 = inner.resumeQuery()) {
                assertCurrentRecorder(inner);
                // belongs to the earlier installation; the current one must be left alone
                first.close();
                assertCurrentRecorder(inner);
                inner.endQuery();
                assertCurrentRecorder(outer);
            }
            assertCurrentRecorder(outer);
            outer.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    /**
     * A running query belongs to the thread it is installed on; suspending or ending it from another thread is
     * rejected, and must leave it exactly as it was so the owning thread can still finish it.
     */
    @Test
    public void testSuspendingOrEndingFromAnotherThreadIsRejectedWithoutChangingState() throws InterruptedException {
        final QueryPerformanceRecorder query = newQuery("query");
        try (final SafeCloseable ignored = query.startQuery()) {
            assertRejectedOnAnotherThread(query::suspendQuery);
            Assert.eq(query.getState(), "query.getState()", QueryState.RUNNING);
            assertCurrentRecorder(query);

            assertRejectedOnAnotherThread(query::endQuery);
            Assert.eq(query.getState(), "query.getState()", QueryState.RUNNING);
            assertCurrentRecorder(query);

            query.endQuery();
            Assert.eq(query.getState(), "query.getState()", QueryState.FINISHED);
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    /**
     * An abort may come from any thread and does not touch the thread-local: the aborted query stays installed until
     * its owner ends it or leaves its scope, and either way the outer query gets the thread back.
     */
    @Test
    public void testAbortedQueryStillHandsTheThreadBack() throws InterruptedException {
        final QueryPerformanceRecorder outer = newQuery("outer");
        final QueryPerformanceRecorder inner = suspendedQuery("inner");
        final QueryPerformanceRecorder other = suspendedQuery("other");

        try (final SafeCloseable ignored = outer.startQuery()) {
            try (final SafeCloseable ignored2 = inner.resumeQuery()) {
                assertCurrentRecorder(inner);
                final Thread aborter = new Thread(inner::abortQuery, "aborter");
                aborter.start();
                aborter.join();
                Assert.eq(inner.getState(), "inner.getState()", QueryState.INTERRUPTED);
                assertCurrentRecorder(inner);
                // ending an interrupted query reports nothing to log and leaves the thread to the scope
                Assert.eqFalse(inner.endQuery(), "inner.endQuery()");
                assertCurrentRecorder(inner);
            }
            assertCurrentRecorder(outer);

            // the same without an explicit end: leaving the scope is enough
            try (final SafeCloseable ignored2 = other.resumeQuery()) {
                other.abortQuery();
                assertCurrentRecorder(other);
            }
            assertCurrentRecorder(outer);
            outer.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    @Test
    public void testTransitionsFromTheWrongStateAreRejected() {
        final QueryPerformanceRecorder query = newQuery("query");
        // NOT_STARTED
        assertIllegalState(query::resumeQuery);
        assertIllegalState(query::suspendQuery);
        assertIllegalState(query::endQuery);
        query.abortQuery(); // a no-op
        Assert.eq(query.getState(), "query.getState()", QueryState.NOT_STARTED);

        // SUSPENDED
        try (final SafeCloseable ignored = query.startQuery()) {
            query.suspendQuery();
        }
        assertIllegalState(query::startQuery);
        assertIllegalState(query::suspendQuery);
        assertIllegalState(query::endQuery);
        query.abortQuery(); // a no-op
        Assert.eq(query.getState(), "query.getState()", QueryState.SUSPENDED);

        // FINISHED
        try (final SafeCloseable ignored = query.resumeQuery()) {
            query.endQuery();
        }
        assertIllegalState(query::startQuery);
        assertIllegalState(query::resumeQuery);
        assertIllegalState(query::suspendQuery);
        assertIllegalState(query::endQuery);
        query.abortQuery(); // a no-op
        Assert.eq(query.getState(), "query.getState()", QueryState.FINISHED);
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    @Test
    public void testStartingWhileAnotherQueryOwnsTheThreadIsAnError() {
        final QueryPerformanceRecorder outer =
                QueryPerformanceRecorder.newQuery("outer", null, QueryPerformanceNugget.DEFAULT_FACTORY);
        final QueryPerformanceRecorder inner =
                QueryPerformanceRecorder.newQuery("inner", null, QueryPerformanceNugget.DEFAULT_FACTORY);
        try (final SafeCloseable ignored = outer.startQuery()) {
            try {
                inner.startQuery();
                Assert.statementNeverExecuted("a new query cannot start while another query owns the thread");
            } catch (final IllegalStateException expected) {
                // only a suspended query may take over a busy thread
            }
            assertCurrentRecorder(outer);
            outer.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    @Test
    public void testStartingTheRunningQueryIsAnError() {
        final QueryPerformanceRecorder recorder =
                QueryPerformanceRecorder.newQuery("query", null, QueryPerformanceNugget.DEFAULT_FACTORY);
        try (final SafeCloseable ignored = recorder.startQuery()) {
            try {
                recorder.startQuery();
                Assert.statementNeverExecuted("a running query cannot be started again");
            } catch (final IllegalStateException expected) {
                // the query is already running
            }
            recorder.endQuery();
        }
        assertCurrentRecorder(QueryPerformanceRecorderState.DUMMY_RECORDER);
    }

    private static QueryPerformanceRecorder newQuery(final String description) {
        return QueryPerformanceRecorder.newQuery(description, null, QueryPerformanceNugget.DEFAULT_FACTORY);
    }

    /** A query that has been started and suspended, so that it may be resumed on a busy thread. */
    private static QueryPerformanceRecorder suspendedQuery(final String description) {
        final QueryPerformanceRecorder query = newQuery(description);
        try (final SafeCloseable ignored = query.startQuery()) {
            query.suspendQuery();
        }
        return query;
    }

    private static void assertCannotStartQuery(final String why) {
        final QueryPerformanceRecorder fresh = newQuery("fresh");
        try {
            fresh.startQuery();
            Assert.statementNeverExecuted(why);
        } catch (final IllegalStateException expected) {
            // the thread is owned by a running query
        }
    }

    private static void assertIllegalState(final Runnable transition) {
        try {
            transition.run();
            Assert.statementNeverExecuted("the transition must be rejected");
        } catch (final IllegalStateException expected) {
            // rejected
        }
    }

    private static void assertRejectedOnAnotherThread(final Runnable transition) throws InterruptedException {
        final Throwable[] failure = new Throwable[1];
        final Thread other = new Thread(() -> {
            try {
                transition.run();
            } catch (final Throwable t) {
                failure[0] = t;
            }
        }, "other");
        other.start();
        other.join();
        Assert.eqTrue(failure[0] instanceof IllegalStateException, "failure[0] instanceof IllegalStateException");
    }

    private static void assertCurrentRecorder(final QueryPerformanceRecorder expected) {
        Assert.eq(QueryPerformanceRecorder.getInstance(), "QueryPerformanceRecorder.getInstance()", expected,
                "expected");
    }
}
