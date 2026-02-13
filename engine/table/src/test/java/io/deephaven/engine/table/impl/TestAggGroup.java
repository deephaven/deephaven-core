//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.by.AggregationProcessor;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.table.impl.by.RollupConstants.ROLLUP_COLUMN_SUFFIX;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.*;

@Category(OutOfBandTest.class)
public class TestAggGroup extends RefreshingTableTestCase {
    @Test
    public void testGroupModifications() {
        final QueryTable source = TstUtils.testRefreshingTable(
                stringCol("Key1", "Alpha", "Bravo", "Alpha", "Charlie", "Charlie", "Bravo", "Bravo"),
                stringCol("Key2", "Delta", "Delta", "Echo", "Echo", "Echo", "Echo", "Echo"),
                intCol("Sentinel", 1, 2, 3, 4, 5, 6, 7),
                intCol("Sentinel2", 101, 102, 103, 104, 105, 106, 107));

        final List<Aggregation> aggs =
                List.of(AggGroup("Sentinel"), AggSum("Sum=Sentinel"), AggGroup("Sentinel2"), AggSum("Sum2=Sentinel2"));

        final QueryTable normal = source.aggNoMemo(AggregationProcessor.forAggregation(aggs), false, null,
                List.of(ColumnName.of("Key1")));
        final QueryTable normalNoKey =
                source.aggNoMemo(AggregationProcessor.forAggregation(aggs), false, null, List.of());
        final ColumnName rollupColumn = ColumnName.of(ROLLUP_COLUMN_SUFFIX);
        final QueryTable base = source.aggNoMemo(AggregationProcessor.forRollupBase(aggs, false, rollupColumn), false,
                null, List.of(ColumnName.of("Key1"), ColumnName.of("Key2")));
        final QueryTable reaggregated = base.aggNoMemo(AggregationProcessor.forRollupReaggregated(aggs,
                List.of(ColumnDefinition.ofString("Key2")), rollupColumn, source), false, null,
                List.of(ColumnName.of("Key1")));
        final QueryTable reaggregated2 = reaggregated.aggNoMemo(AggregationProcessor.forRollupReaggregated(aggs,
                List.of(ColumnDefinition.ofString("Key1"), ColumnDefinition.ofString("Key2")), rollupColumn, source),
                false, null,
                List.of());

        TableTools.show(normal);
        TableTools.show(base);
        TableTools.show(reaggregated);
        TableTools.show(reaggregated2);

        doCheck(normal, base, reaggregated, normalNoKey, reaggregated2);

        final SimpleListener normalListener = new SimpleListener(normal);
        normal.addUpdateListener(normalListener);
        final SimpleListener baseListener = new SimpleListener(base);
        base.addUpdateListener(baseListener);
        final SimpleListener reaggListener = new SimpleListener(reaggregated);
        reaggregated.addUpdateListener(reaggListener);
        final SimpleListener reaggListener2 = new SimpleListener(reaggregated2);
        reaggregated2.addUpdateListener(reaggListener2);

        final ControlledUpdateGraph cug = ExecutionContext.getContext().getUpdateGraph().cast();
        // modify the value of a Sentinel; check the updates
        cug.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(source, i(0), stringCol("Key1", "Alpha"), stringCol("Key2", "Delta"),
                    intCol("Sentinel", 8), intCol("Sentinel2", 101));
            final ModifiedColumnSet mcs = source.getModifiedColumnSetForUpdates();
            mcs.clear();
            mcs.setAll("Sentinel");
            source.notifyListeners(new TableUpdateImpl(i(), i(), i(0), RowSetShiftData.EMPTY, mcs));
        });

        TableTools.show(normal);
        TableTools.show(base);
        TableTools.show(reaggregated);

        // make sure the aggregation is still consistent
        doCheck(normal, base, reaggregated, normalNoKey, reaggregated2);

        // we should have gotten an update from each of our listeners
        checkModified(normalListener, normal, "Sentinel", "Sentinel2");
        checkModified(baseListener, base, "Sentinel", "Sentinel2");
        checkModified(reaggListener, reaggregated, "Sentinel", "Sentinel2");
        checkModified(reaggListener2, reaggregated2, "Sentinel", "Sentinel2");
    }

    private static void checkModified(SimpleListener listener, QueryTable table, final String modColumn,
            final String noModColumn) {
        System.out.println("update = " + listener.update);
        assertEquals(1, listener.count);
        assertTrue(listener.update.added().isEmpty());
        assertTrue(listener.update.removed().isEmpty());
        assertEquals(1, listener.update.modified().size());
        assertTrue(listener.update.modifiedColumnSet().containsAll(table.newModifiedColumnSet(modColumn)));
        assertFalse(listener.update.modifiedColumnSet().containsAny(table.newModifiedColumnSet(noModColumn)));
    }

    private static void doCheck(Table normal, QueryTable base, QueryTable reaggregated, QueryTable normalNoKey,
            QueryTable reaggregated2) {
        assertEquals(0, normal.update("CheckSum=sum(Sentinel)", "CheckSum2=sum(Sentinel2)")
                .where("Sum != CheckSum || Sum2 != CheckSum2").size());
        assertEquals(0, base.update("CheckSum=sum(Sentinel)", "CheckSum2=sum(Sentinel2)")
                .where("Sum != CheckSum || Sum2 != CheckSum2").size());
        assertEquals(0, reaggregated.update("CheckSum=sum(Sentinel)", "CheckSum2=sum(Sentinel2)")
                .where("Sum != CheckSum || Sum2 != CheckSum2").size());
        assertTableEquals(normal.view("Key1", "Sentinel", "Sum", "Sentinel2", "Sum2"),
                reaggregated.view("Key1", "Sentinel", "Sum", "Sentinel2", "Sum2"));

        assertTableEquals(normalNoKey.view("Sentinel", "Sum", "Sentinel2", "Sum2"),
                reaggregated2.view("Sentinel", "Sum", "Sentinel2", "Sum2"));

    }
}
