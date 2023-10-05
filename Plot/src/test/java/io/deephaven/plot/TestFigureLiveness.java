package io.deephaven.plot;

import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.SafeCloseable;

public class TestFigureLiveness extends RefreshingTableTestCase {
    public void testFigureLiveness() {
        // Scope that represents the incoming grpc call
        LivenessScope reqScope = new LivenessScope();

        // Scope that the work is done in
        LivenessScope scope = new LivenessScope();
        Table t;
        Figure plot;
        try (SafeCloseable ignored = LivenessScopeStack.open(scope, false)) {
            t = TableTools.timeTable("PT1s").updateView("Y=i", "X=i", "Z=i");
            plot = PlottingConvenience.plot("series", t, "X", "Y")
                    .plotBy("multieseries", t, "X", "Y", "Z")
                    .show();
            reqScope.manage((LivenessReferent) plot);// explicitly manage outside of the scope
        }

        // Assert that the tables are retained while the scope is still open (then drop the new reference)
        assertTrue(t.tryRetainReference());
        t.dropReference();

        // Drop the inner scope
        scope.release();

        // Assert that the table reference is retained now by virtue of the plot that holds it
        assertTrue(t.tryRetainReference());
        t.dropReference();

        // Simulate the user releasing their export
        reqScope.release();

        // Assert that the table reference is no longer held after the figure is released
        assertFalse(t.tryRetainReference());
    }
}
