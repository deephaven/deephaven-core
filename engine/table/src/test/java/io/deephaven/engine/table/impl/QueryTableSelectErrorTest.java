//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.exceptions.TableInitializationException;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.thread.ThreadInitializationFactory;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test that an {@link Error} thrown while initializing a select or update fails the caller instead of leaving it
 * waiting forever for a completion that will never arrive.
 */
public class QueryTableSelectErrorTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final int TEST_SIZE = 100;
    private static final int THREAD_COUNT = 4;
    private static final long SELECT_TIMEOUT_MILLIS = 60_000;

    /**
     * An Error that a select formula can throw cheaply. The failure this stands in for is an OutOfMemoryError from a
     * column source allocation, which we cannot provoke without exhausting the heap for every other test sharing this
     * JVM.
     */
    public static final class SelectTestError extends Error {

        public SelectTestError(final String message) {
            super(message);
        }
    }

    public static int throwSelectError(final long rowPosition) {
        throw new SelectTestError("Intentional Error from row position " + rowPosition);
    }

    /**
     * The column is small enough that {@link SelectColumnLayer} evaluates it with a single job, submitted directly to
     * the {@link io.deephaven.engine.table.impl.util.OperationInitializerJobScheduler}.
     */
    @Test
    public void testErrorInSingleJobSelect() {
        doErrorSelectTest(QueryTable.MINIMUM_PARALLEL_SELECT_ROWS);
    }

    /**
     * Lowering the minimum splits the column across several jobs, so the failure happens under the iteration machinery
     * of {@link io.deephaven.engine.table.impl.util.JobScheduler.IterationManager} rather than in a directly submitted
     * job.
     */
    @Test
    public void testErrorInIteratedSelect() {
        doErrorSelectTest(1);
    }

    private void doErrorSelectTest(final long minimumParallelSelectRows) {
        final boolean oldForceParallel = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        final long oldMinimumRows = QueryTable.MINIMUM_PARALLEL_SELECT_ROWS;
        final OperationInitializationThreadPool threadPool =
                new OperationInitializationThreadPool(ThreadInitializationFactory.NO_OP, THREAD_COUNT);
        final ExecutionContext executionContext =
                ExecutionContext.getContext().withOperationInitializer(threadPool);
        try (final SafeCloseable ignored = executionContext.open();
                final SafeCloseable ignored2 = threadPool::shutdown) {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = true;
            QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = minimumParallelSelectRows;
            ExecutionContext.getContext().getQueryLibrary().importStatic(QueryTableSelectErrorTest.class);

            final Table source = TableTools.emptyTable(TEST_SIZE);
            final Throwable thrown = updateExpectingFailure(executionContext, source);
            assertTrue("TableInitializationException, but was " + thrown,
                    thrown instanceof TableInitializationException);
            assertNotNull("SelectTestError in cause chain of " + thrown, findTestError(thrown));
        } finally {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = oldForceParallel;
            QueryTable.MINIMUM_PARALLEL_SELECT_ROWS = oldMinimumRows;
        }
    }

    /**
     * Perform the failing update on a thread of our own, so that the caller waiting forever on the update's completion
     * fails this test rather than hanging the entire suite.
     */
    private static Throwable updateExpectingFailure(
            final ExecutionContext executionContext,
            final Table source) {
        final MutableObject<Throwable> thrown = new MutableObject<>();
        final Thread updateThread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                source.update("A = throwSelectError(ii)");
            } catch (Throwable t) {
                thrown.setValue(t);
            }
        }, "QueryTableSelectErrorTest-update");
        updateThread.setDaemon(true);
        updateThread.start();
        try {
            updateThread.join(SELECT_TIMEOUT_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for the update to fail", e);
        }
        if (updateThread.isAlive()) {
            // Interrupt so that a hung update does not keep the thread (and the engine state it holds) alive for the
            // rest of the suite.
            updateThread.interrupt();
            fail("Update did not complete within " + SELECT_TIMEOUT_MILLIS + " ms");
        }
        assertNotNull("Update failed", thrown.getValue());
        return thrown.getValue();
    }

    private static SelectTestError findTestError(final Throwable throwable) {
        for (Throwable cause = throwable; cause != null; cause = cause.getCause()) {
            if (cause instanceof SelectTestError) {
                return (SelectTestError) cause;
            }
        }
        return null;
    }
}
