//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.assertTrue;

/**
 * Test that a select or update layer failing during a refresh logs which operation failed.
 */
public class SelectOrUpdateListenerErrorTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    public static int throwOnNegative(final int value) {
        if (value < 0) {
            throw new IllegalStateException("Intentional failure for value " + value);
        }
        return value;
    }

    @Test
    public void testParallelLayerFailureLogsDescription() {
        doTestLayerFailureLogsDescription(true);
    }

    @Test
    public void testSerialLayerFailureLogsDescription() {
        doTestLayerFailureLogsDescription(false);
    }

    /**
     * A layer's failure is delivered to the listener through the job scheduler rather than thrown from onUpdate; the
     * log must still identify the failing operation.
     */
    private void doTestLayerFailureLogsDescription(final boolean parallel) {
        final boolean oldForceParallel = QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE;
        final boolean oldEnableParallel = QueryTable.ENABLE_PARALLEL_SELECT_AND_UPDATE;
        final PrintStream oldErr = System.err;
        final ByteArrayOutputStream captured = new ByteArrayOutputStream();
        try {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = parallel;
            QueryTable.ENABLE_PARALLEL_SELECT_AND_UPDATE = parallel;
            ExecutionContext.getContext().getQueryLibrary().importStatic(SelectOrUpdateListenerErrorTest.class);

            final QueryTable source = testRefreshingTable(i(2, 4, 6).toTracking(),
                    intCol("X", 1, 2, 3), intCol("Y", 4, 5, 6));
            final Table result = source.update("Boom = throwOnNegative(X)", "Twice = Y * 2", "Sum = X + Y");

            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            try (final RefreshingTableTestCase.ExpectingError ignored = base.new ExpectingError()) {
                System.setErr(new PrintStream(captured, true, StandardCharsets.UTF_8));
                updateGraph.runWithinUnitTestCycle(() -> {
                    addToTable(source, i(4), intCol("X", -1), intCol("Y", 5));
                    source.notifyListeners(i(), i(), i(4));
                });
            } finally {
                System.setErr(oldErr);
            }

            assertTrue("result.isFailed()", result.isFailed());
            final String log = captured.toString(StandardCharsets.UTF_8);
            assertTrue("log identifies the listener: " + log,
                    log.contains("Uncaught exception for entry ") && log.contains("Update([Boom, Twice, Sum])"));
            assertTrue("log includes the update: " + log, log.contains("modified.size()=1"));
            assertTrue("log includes the exception: " + log, log.contains("Intentional failure for value -1"));
        } finally {
            QueryTable.FORCE_PARALLEL_SELECT_AND_UPDATE = oldForceParallel;
            QueryTable.ENABLE_PARALLEL_SELECT_AND_UPDATE = oldEnableParallel;
        }
    }
}
