//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.testutil.*;
import io.deephaven.gui.table.QuickFilterMode;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Function;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class TestConcurrentInstantiationIterativeFilter extends TestConcurrentInstantiationIterativeBase {
    @Test
    public void testChain() throws ExecutionException, InterruptedException, TimeoutException {
        final QueryTable table = TstUtils.testRefreshingTable(i(2, 4, 6).toTracking(),
                col("x", 1, 2, 3), col("y", "a", "b", "c"), col("z", true, false, true));
        final Table tableStart = TstUtils.testRefreshingTable(i(1, 3).toTracking(),
                col("x", 3, 1), col("y", "c", "a"), col("z", true, true), col("u", 12, 4));

        final Table tableUpdate = TstUtils.testRefreshingTable(i(1, 2, 4).toTracking(),
                col("x", 4, 3, 1), col("y", "d", "c", "a"), col("z", true, true, true), col("u", 16, 12, 4));

        final Callable<Table> callable =
                () -> LivenessScopeStack.computeEnclosed(() -> table.updateView("u=x*4").where("z").sortDescending("x"),
                        true, Table::isRefreshing);

        updateGraph.startCycleForUnitTests(false);

        final Table chain1 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        assertTableEquals(chain1, tableStart);

        TstUtils.addToTable(table, i(3), col("x", 4), col("y", "d"), col("z", true));

        final Table chain2 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        TstUtils.assertTableEquals(tableStart, prevTable(chain1));
        TstUtils.assertTableEquals(tableStart, prevTable(chain2));

        table.notifyListeners(i(3), i(), i());
        updateGraph.markSourcesRefreshedForUnitTests();

        final Table chain3 = pool.submit(callable).get(TIMEOUT_LENGTH, TIMEOUT_UNIT);

        showWithRowSet(chain3);

        TstUtils.assertTableEquals(tableStart, prevTable(chain1));
        TstUtils.assertTableEquals(tableStart, prevTable(chain2));

        updateGraph.completeCycleForUnitTests();

        TstUtils.assertTableEquals(tableUpdate, chain1);
        TstUtils.assertTableEquals(tableUpdate, chain2);
        TstUtils.assertTableEquals(tableUpdate, chain3);
    }

    @Test
    public void testIterativeQuickFilter() {
        final List<Function<Table, Table>> transformations = new ArrayList<>();
        transformations.add(t -> t.where("boolCol2"));
        transformations.add(t -> t.where(DisjunctiveFilter.makeDisjunctiveFilter(
                WhereFilterFactory.expandQuickFilter(t.getDefinition(), "10", QuickFilterMode.NORMAL))));
        transformations.add(t -> t.sortDescending("doubleCol"));
        transformations.add(Table::flatten);
        testIterative(transformations);
    }

    @Test
    public void testIterativeDisjunctiveCondition() {
        final List<Function<Table, Table>> transformations = new ArrayList<>();
        transformations.add(
                t -> t.where(DisjunctiveFilter.makeDisjunctiveFilter(
                        ConditionFilter.createConditionFilter("false"),
                        ConditionFilter.createConditionFilter("true"))));
        testIterative(transformations);
    }
}
