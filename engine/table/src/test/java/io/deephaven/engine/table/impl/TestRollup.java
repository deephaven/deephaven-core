//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.test.types.OutOfBandTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.*;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.testutil.TstUtils.*;

@Category(OutOfBandTest.class)
public class TestRollup extends RefreshingTableTestCase {
    // This is the list of supported aggregations for rollup. These are all using `intCol` as the column to aggregate
    // because the re-aggregation logic is effectively the same for all column types.
    private final Collection<Aggregation> aggs = List.of(
            AggAbsSum("absSum=intCol"),
            AggAvg("avg=intCol"),
            AggCount("count"),
            AggCountWhere("countWhere", "intCol > 50"),
            AggCountDistinct("countDistinct=intCol"),
            AggDistinct("distinct=intCol"),
            AggFirst("first=intCol"),
            AggLast("last=intCol"),
            AggMax("max=intCol"),
            AggMin("min=intCol"),
            AggSortedFirst("Sym", "firstSorted=intCol"),
            AggSortedLast("Sym", "lastSorted=intCol"),
            AggStd("std=intCol"),
            AggSum("sum=intCol"),
            AggUnique("unique=intCol"),
            AggVar("var=intCol"),
            AggWAvg("intCol", "wavg=intCol"),
            AggWSum("intCol", "wsum=intCol"));

    // Companion list of columns to compare between rollup root and the zero-key equivalent
    private final String[] columnsToCompare = new String[] {
            "intCol",
            "absSum",
            "avg",
            "count",
            "countWhere",
            "countDistinct",
            "distinct",
            "first",
            "last",
            "max",
            "min",
            "firstSorted",
            "lastSorted",
            "std",
            "sum",
            "unique",
            "var",
            "wavg",
            "wsum"
    };

    @SuppressWarnings("rawtypes")
    private final ColumnInfo[] columnInfo = initColumnInfos(
            new String[] {"Sym", "intCol"},
            new SetGenerator<>("a", "b", "c", "d"),
            new IntGenerator(10, 100));

    private QueryTable createTable(boolean refreshing, int size, Random random) {
        return getTable(refreshing, size, random, columnInfo);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    private String[] dropColumnNames(final Table table, final String[] columnsToKeep) {
        final List<String> columns = new ArrayList<>();
        final Set<String> columnsToKeepSet = new HashSet<>(Arrays.asList(columnsToKeep));
        for (final String column : table.getDefinition().getColumnNames()) {
            if (!columnsToKeepSet.contains(column)) {
                columns.add(column);
            }
        }
        return columns.toArray(String[]::new);
    }

    @Test
    public void testRollup() {
        final Random random = new Random(0);
        // Create the test table
        final Table testTable = createTable(false, 100_000, random);

        // Create the rollup table
        final RollupTable rollupTable = testTable.rollup(aggs, false, "Sym");
        // Extract the root table and drop columns we don't want to compare
        final Table rootTable = rollupTable.getRoot();
        final Table actual = rootTable.dropColumns(dropColumnNames(rootTable, columnsToCompare));

        // Create the expected table (zero-key equivalent of the rollup table)
        final Table expected = testTable.aggBy(aggs);

        // Compare the zero-key equivalent table to the rollup table root
        TstUtils.assertTableEquals(actual, expected);
    }

    @Test
    public void testRollupIncremental() {
        for (int size = 10; size <= 1000; size *= 10) {
            testRollupIncrementalInternal("size-" + size, size);
        }
    }

    private void testRollupIncrementalInternal(final String ctxt, final int size) {
        final Random random = new Random(0);

        // Create the test table
        final QueryTable testTable = createTable(true, size * 10, random);

        // Create the drop cplumns list
        final String[] dropColumns = dropColumnNames(
                testTable.rollup(aggs, false, "Sym").getRoot(), columnsToCompare);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(
                        testTable.rollup(aggs, false, "Sym")
                                .getRoot().dropColumns(dropColumns),
                        testTable.aggBy(aggs))
        };

        final int steps = 100; // 8;
        for (int step = 0; step < steps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep(ctxt + " step == " + step, size, random, testTable, columnInfo, en);
        }
    }
}
