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

    private static void assertCurrentRecorder(final QueryPerformanceRecorder expected) {
        Assert.eq(QueryPerformanceRecorder.getInstance(), "QueryPerformanceRecorder.getInstance()", expected,
                "expected");
    }
}
