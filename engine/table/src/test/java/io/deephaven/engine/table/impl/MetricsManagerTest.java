package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.metrics.MetricsManager;
import org.junit.Test;

import static io.deephaven.engine.table.impl.TstUtils.c;
import static io.deephaven.engine.util.TableTools.intCol;
import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class MetricsManagerTest {

    private static Table someJoinResult() {
        final Table left = TableTools.newTable(
                c("LeftString", "A", "B", "A", "C", "D", "A"),
                intCol("LeftInt", 1, 10, 50, 3, 4, 60));
        final Table right = TableTools.newTable(
                c("RightString", "A", "B", "A", "B", "A", "D", "E"),
                intCol("RightInt", 1, 5, 10, 25, 50, 5, 3));

        return left.join(right, "LeftString=RightString");
    }

    private static final boolean verbose = false;

    private static void maybeShowTable(final Table t) {
        if (!verbose) {
            return;
        }
        TableTools.show(t);
    }

    private static void maybeShowMetrics(final String varName, final String metrics) {
        if (!verbose) {
            return;
        }
        final String halfHeader = "=============";
        System.out.println(halfHeader + " " + varName + " " + halfHeader);
        System.out.println(metrics);
    }

    @Test
    public void testReset() {
        if (!MetricsManager.enabled) {
            if (verbose) {
                System.out.println("MetricsManager is disabled, not running test.");
            }
            return;
        }
        if (verbose) {
            System.out.println("MetricsManager enabled, running test.");
        }
        Table t = someJoinResult();
        maybeShowTable(t);
        assertEquals(12, t.size());
        MetricsManager.resetCounters();
        final String c0 = MetricsManager.getCounters();
        maybeShowMetrics("c0", c0);
        t = someJoinResult();
        maybeShowTable(t);
        assertEquals(12, t.size());
        assertEquals(12, t.size());
        final String c1 = MetricsManager.getCounters();
        maybeShowMetrics("c1", c1);
        MetricsManager.resetCounters();
        final String c2 = MetricsManager.getCounters();
        maybeShowMetrics("c2", c2);
        assertEquals(c0, c2);
        assertNotEquals(c0, c1);
    }
}
