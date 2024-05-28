//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.parquet.table.ParquetTools;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.select.IncrementalReleaseFilter;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.mutable.MutableInt;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.engine.testutil.TstUtils.*;

@Category(OutOfBandTest.class)
public class QueryTableSortTest extends QueryTableTestBase {

    public void testSort() {
        final Table result0 = newTable(col("Unsorted", 3.0, null, 2.0), col("DataToSort", "c", "a", "b"));
        show(result0.sort("Unsorted"));
        assertEquals(Arrays.asList(null, 2.0, 3.0),
                Arrays.asList(DataAccessHelpers.getColumn(result0.sort("Unsorted"), "Unsorted").get(0, 3)));
        show(result0.sortDescending("Unsorted"));
        assertEquals(Arrays.asList(3.0, 2.0, null),
                Arrays.asList(DataAccessHelpers.getColumn(result0.sortDescending("Unsorted"), "Unsorted").get(0, 3)));

        Table result1 = newTable(col("Unsorted", 4.0, 3.0, 1.1, Double.NaN, 2.0, 1.0, 5.0),
                col("DataToSort", "e", "d", "b", "g", "c", "a", "f"));
        final Table nanSorted = result1.sort("Unsorted");
        show(nanSorted);
        assertEquals(Arrays.asList(1.0, 1.1, 2.0, 3.0, 4.0, 5.0, Double.NaN),
                Arrays.asList(DataAccessHelpers.getColumn(nanSorted, "Unsorted").get(0, 7)));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f", "g"),
                Arrays.asList(DataAccessHelpers.getColumn(nanSorted, "DataToSort").get(0, 7)));

        result1 = newTable(col("Unsorted", 4.1f, 3.1f, 1.2f, Float.NaN, 2.1f, 1.1f, 5.1f),
                col("DataToSort", "e", "d", "b", "g", "c", "a", "f"));
        final Table nanFloatSorted = result1.sort("Unsorted");
        System.out.println("result1");
        show(result1);
        System.out.println("nanFloatedSorted");
        show(nanFloatSorted);
        assertEquals(Arrays.asList(1.1f, 1.2f, 2.1f, 3.1f, 4.1f, 5.1f, Float.NaN),
                Arrays.asList(DataAccessHelpers.getColumn(nanFloatSorted, "Unsorted").get(0, 7)));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f", "g"),
                Arrays.asList(DataAccessHelpers.getColumn(nanFloatSorted, "DataToSort").get(0, 7)));


        Table result = newTable(col("Unsorted", 3, 1, 2), col("DataToSort", "c", "a", "b")).sort("DataToSort");
        assertEquals(Arrays.asList(1, 2, 3), Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));
        result = newTable(col("Unsorted", 3, 1, 2), col("DataToSort", "c", "a", "b")).sortDescending("DataToSort");
        assertEquals(Arrays.asList(3, 2, 1), Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));

        result = newTable(col("Unsorted", '3', '1', '2'), col("DataToSort", "c", "a", "b")).sort("Unsorted");
        assertEquals(Arrays.asList('1', '2', '3'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));
        result = newTable(col("Unsorted", '3', '1', '2'), col("DataToSort", "c", "a", "b")).sortDescending("Unsorted");
        assertEquals(Arrays.asList('3', '2', '1'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));

        final ColumnHolder<?> c1 = TstUtils.colIndexed("Unsorted", 3, 1, 2);
        final Table table = newTable(c1, col("DataToSort", "c", "a", "b"));
        result = table.sort("DataToSort");
        assertEquals(Arrays.asList(1, 2, 3), Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));
        final ColumnHolder<?> c11 = TstUtils.colIndexed("Unsorted", 3, 1, 2);
        result = newTable(c11, col("DataToSort", "c", "a", "b")).sortDescending("DataToSort");
        assertEquals(Arrays.asList(3, 2, 1), Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));

        final ColumnHolder<?> c2 = TstUtils.colIndexed("Unsorted", '3', '1', '2');
        result = newTable(c2, col("DataToSort", "c", "a", "b")).sort("Unsorted");
        assertEquals(Arrays.asList('1', '2', '3'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));
        final ColumnHolder<?> c22 = TstUtils.colIndexed("Unsorted", '3', '1', '2');
        result = newTable(c22, col("DataToSort", "c", "a", "b")).sortDescending("Unsorted");
        assertEquals(Arrays.asList('3', '2', '1'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 3)));

        final Table input =
                newTable(col("C1", 2, 4, 2, 4), col("C2", '1', '1', '2', '2'), col("Witness", "a", "b", "c", "d"));
        System.out.println("Input:");
        showWithRowSet(input);
        result = input.sort("C1", "C2");
        System.out.println("Result:");
        showWithRowSet(result);
        assertEquals(Arrays.asList(2, 2, 4, 4), Arrays.asList(DataAccessHelpers.getColumn(result, "C1").get(0, 4)));
        assertEquals(Arrays.asList('1', '2', '1', '2'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "C2").get(0, 4)));
        assertEquals(Arrays.asList("a", "c", "b", "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Witness").get(0, 4)));

        result = newTable(col("C1", 2, 4, 2, 4), col("C2", '2', '2', '1', '1'), col("Witness", "a", "b", "c", "d"))
                .sort("C2",
                        "C1");
        assertEquals(Arrays.asList(2, 4, 2, 4), Arrays.asList(DataAccessHelpers.getColumn(result, "C1").get(0, 4)));
        assertEquals(Arrays.asList('1', '1', '2', '2'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "C2").get(0, 4)));
        assertEquals(Arrays.asList("c", "d", "a", "b"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Witness").get(0, 4)));

        result = newTable(col("C1", 2, 4, 2, 4), col("C2", '1', '1', '2', '2'), col("Witness", "a", "b", "c", "d"))
                .sortDescending("C1", "C2");
        assertEquals(Arrays.asList(4, 4, 2, 2), Arrays.asList(DataAccessHelpers.getColumn(result, "C1").get(0, 4)));
        assertEquals(Arrays.asList('2', '1', '2', '1'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "C2").get(0, 4)));
        assertEquals(Arrays.asList("d", "b", "c", "a"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Witness").get(0, 4)));

        result = newTable(col("C1", 2, 4, 2, 4), col("C2", '2', '2', '1', '1'), col("Witness", "a", "b", "c", "d"))
                .sortDescending("C2", "C1");
        assertEquals(Arrays.asList(4, 2, 4, 2), Arrays.asList(DataAccessHelpers.getColumn(result, "C1").get(0, 4)));
        assertEquals(Arrays.asList('2', '2', '1', '1'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "C2").get(0, 4)));
        assertEquals(Arrays.asList("b", "a", "d", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Witness").get(0, 4)));


        final ColumnHolder<?> c3 = TstUtils.colIndexed("Unsorted", '3', '1', '2', null);
        result = newTable(c3, col("DataToSort", "c", "a", "b", "d")).sort("Unsorted");
        show(result);
        assertEquals(Arrays.asList(null, '1', '2', '3'),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 4)));
        assertEquals(Arrays.asList("d", "a", "b", "c"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 4)));
        final ColumnHolder<?> c4 = TstUtils.colIndexed("Unsorted", '3', '1', null, '2');
        result = newTable(c4, col("DataToSort", "c", "a", "d", "b")).sortDescending("Unsorted");
        assertEquals(Arrays.asList('3', '2', '1', null),
                Arrays.asList(DataAccessHelpers.getColumn(result, "Unsorted").get(0, 4)));
        assertEquals(Arrays.asList("c", "b", "a", "d"),
                Arrays.asList(DataAccessHelpers.getColumn(result, "DataToSort").get(0, 4)));
    }

    public void testSort2() {
        final QueryTable table = testRefreshingTable(i(10, 20, 30).toTracking(),
                col("A", 3, 1, 2), col("B", "c", "a", "b"));

        final QueryTable sorted = (QueryTable) table.sort("A");
        show(sorted);
        assertTableEquals(testTable(col("A", 1, 2, 3), col("B", "a", "b", "c")), sorted);
        assertTrue(SortedColumnsAttribute.isSortedBy(sorted, "A", SortingOrder.Ascending));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(20), col("A", 1), col("B", "A"));
            table.notifyListeners(i(), i(), i(20));
        });
        show(sorted);

        assertTableEquals(testTable(col("A", 1, 2, 3), col("B", "A", "b", "c")), sorted);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(20), col("A", 1), col("B", "A2"));
            addToTable(table, i(25), col("A", 1), col("B", "A2'"));
            table.notifyListeners(i(25), i(), i(20));
        });
        show(sorted);

        assertTableEquals(testTable(col("A", 1, 1, 2, 3), col("B", "A2", "A2'", "b", "c")), sorted);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(20, 25), col("A", 1, 3), col("B", "A3", "C2"));
            table.notifyListeners(i(), i(), i(20, 25));
        });
        show(sorted);

        assertTableEquals(testRefreshingTable(col("A", 1, 2, 3, 3), col("B", "A3", "b", "c", "C2")), sorted);

        final Table sortedTwice = sorted.sort("A");
        assertSame(sorted, sortedTwice);

        final Table backwards = sorted.sortDescending("A");
        assertNotSame(sorted, backwards);

        final Table sortBA = sorted.sortDescending("B", "A");
        assertNotSame(sorted, sortBA);

        final Table sortB = sortBA.sortDescending("B");
        assertSame(sortB, sortBA);

        final Table sortA = sortBA.sort("A");
        assertNotSame(sortA, sortBA);

        assertTableEquals(sorted, sortA);
    }

    public void testGroupedSortRefreshing() {
        final Table table = testRefreshingTable(RowSetFactory.flat(9).toTracking(),
                colIndexed("A", "Apple", "Apple", "Apple", "Banana", "Banana", "Banana", "Canteloupe", "Canteloupe",
                        "Canteloupe"),
                col("Secondary", "C", "A", "B", "C", "A", "B", "C", "A", "B")).update("Sentinel=i");

        final QueryTable sorted = (QueryTable) table.dropColumns("Secondary").sortDescending("A");
        show(sorted);
        assertTableEquals(newTable(col("A", "Canteloupe", "Canteloupe", "Canteloupe", "Banana", "Banana", "Banana",
                "Apple", "Apple", "Apple"), col("Sentinel", 6, 7, 8, 3, 4, 5, 0, 1, 2)), sorted);

        final QueryTable sorted2 = (QueryTable) table.sort(List.of(
                SortColumn.desc(ColumnName.of("A")),
                SortColumn.asc(ColumnName.of("Secondary"))));
        show(sorted2);
        assertTableEquals(newTable(
                col("A", "Canteloupe", "Canteloupe", "Canteloupe", "Banana", "Banana", "Banana", "Apple", "Apple",
                        "Apple"),
                col("Secondary", "A", "B", "C", "A", "B", "C", "A", "B", "C"),
                col("Sentinel", 7, 8, 6, 4, 5, 3, 1, 2, 0)), sorted2);

        // Add a new index covering both columns.
        DataIndexer.getOrCreateDataIndex(table, "A", "Secondary");

        final QueryTable sorted3 = (QueryTable) table.sort(List.of(
                SortColumn.desc(ColumnName.of("A")),
                SortColumn.asc(ColumnName.of("Secondary"))));
        show(sorted3);
        assertTableEquals(newTable(
                col("A", "Canteloupe", "Canteloupe", "Canteloupe", "Banana", "Banana", "Banana", "Apple", "Apple",
                        "Apple"),
                col("Secondary", "A", "B", "C", "A", "B", "C", "A", "B", "C"),
                col("Sentinel", 7, 8, 6, 4, 5, 3, 1, 2, 0)), sorted3);
    }

    public void testIndexedSortHistorical() {
        testIndexedSortHistorical(10000);
        testIndexedSortHistorical(1000000);
    }

    private void testIndexedSortHistorical(int size) {
        final String[] choices = new String[] {"Hornigold", "Jennings", "Vane", "Bellamy"};
        final String[] letters = new String[] {"D", "C", "A", "B"};

        assertEquals(0, size % choices.length);
        assertEquals(0, size % letters.length);

        final String[] values = new String[size];
        final String[] letterValues = new String[size];
        for (int ii = 0; ii < values.length; ++ii) {
            values[ii] = choices[ii % choices.length];
            letterValues[ii] = letters[ii % letters.length];
        }

        // Single column index on "Captain"
        Table indexed = testTable(RowSetFactory.flat(values.length).toTracking(),
                colIndexed("Captain", values),
                col("Secondary", values)).update("Sentinel=i");
        Table nogroups = testTable(RowSetFactory.flat(values.length).toTracking(),
                col("Captain", values),
                col("Secondary", values)).update("Sentinel=i");

        Table sortedIndexed = indexed.sortDescending("Captain");
        Table sortedNoGroups = nogroups.sortDescending("Captain");
        show(sortedIndexed);
        assertTableEquals(sortedNoGroups, sortedIndexed);

        // Single column indexes on both "Captain" and "Secondary"
        indexed = testTable(RowSetFactory.flat(values.length).toTracking(),
                colIndexed("Captain", values),
                colIndexed("Secondary", values)).update("Sentinel=i");
        nogroups = testTable(RowSetFactory.flat(values.length).toTracking(),
                col("Captain", values),
                col("Secondary", values)).update("Sentinel=i");

        sortedIndexed = indexed.sortDescending("Captain", "Secondary");
        sortedNoGroups = nogroups.sortDescending("Captain", "Secondary");
        show(sortedIndexed);
        assertTableEquals(sortedNoGroups, sortedIndexed);

        sortedIndexed = indexed.sortDescending("Secondary", "Captain");
        sortedNoGroups = nogroups.sortDescending("Secondary", "Captain");
        show(sortedIndexed);
        assertTableEquals(sortedNoGroups, sortedIndexed);

        // Multi-column indexes on "Captain" and "Secondary"
        indexed = testTable(RowSetFactory.flat(values.length).toTracking(),
                col("Captain", values),
                col("Secondary", values)).update("Sentinel=i");
        DataIndexer.getOrCreateDataIndex(indexed, "Captain", "Secondary");
        nogroups = testTable(RowSetFactory.flat(values.length).toTracking(),
                col("Captain", values),
                col("Secondary", values)).update("Sentinel=i");

        sortedIndexed = indexed.sortDescending("Captain", "Secondary");
        sortedNoGroups = nogroups.sortDescending("Captain", "Secondary");
        show(sortedIndexed);
        assertTableEquals(sortedNoGroups, sortedIndexed);
    }

    public void testSortBool() {
        final QueryTable table = testRefreshingTable(i(10, 20, 30, 40, 50).toTracking(),
                col("boolCol", false, true, null, true, false));

        final QueryTable sorted = (QueryTable) table.sort("boolCol");
        show(sorted);
        assertTableEquals(sorted, testRefreshingTable(col("boolCol", null, false, false, true, true)));
        final QueryTable descending = (QueryTable) table.sort(List.of(SortColumn.desc(ColumnName.of("boolCol"))));
        show(descending);
        assertTableEquals(descending, testRefreshingTable(col("boolCol", true, true, false, false, null)));
    }

    public void testSortIncremental2() {
        final int[] sizes = {10, 100, 1000};
        for (int size : sizes) {
            testSortIncremental("size == " + size, size, 0, new MutableInt(50));
        }
    }

    public void testMultiColumnRuns() {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final int tableSize;
        if (SHORT_TESTS) {
            tableSize = 1_000;
        } else {
            tableSize = 10_000;
        }
        final QueryTable queryTable = getTable(tableSize, random,
                columnInfo = initColumnInfos(new String[] {"bool1", "bool2", "bool3", "bool4", "bool5", "Sentinel"},
                        new BooleanGenerator(0.25, 0.25),
                        new BooleanGenerator(0.50, 0.25),
                        new BooleanGenerator(),
                        new BooleanGenerator(),
                        new BooleanGenerator(),
                        new IntGenerator(0, 100000)));

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.sort("bool1")),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("bool1", "bool2", "bool3", "bool4", "bool5");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("bool5", "bool4", "bool3", "bool2", "bool1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("bool1", "bool2", "bool3", "bool4", "bool5");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("bool5", "bool4", "bool3", "bool2", "bool1");
                    }
                },
        };
        final int steps = 50;
        for (int step = 0; step < steps; step++) {
            simulateShiftAwareStep(" step == " + step, tableSize, random, queryTable, columnInfo, en);
        }
    }

    private ColumnInfo<?, ?>[] getIncrementalColumnInfo() {
        return initColumnInfos(
                new String[] {"Sym", "intCol", "doubleCol", "floatCol", "longCol", "shortCol", "byteCol", "charCol",
                        "boolCol", "bigI", "bigD", "Indices"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(10, 100),
                new SetGenerator<>(10.1, 20.1, 30.1),
                new FloatGenerator(100.0f, 2000.f),
                new LongGenerator(),
                new ShortGenerator(),
                new ByteGenerator(),
                new CharGenerator('A', 'Z'),
                new BooleanGenerator(),
                new BigIntegerGenerator(BigInteger.valueOf(100000), BigInteger.valueOf(100100)),
                new BigDecimalGenerator(BigInteger.valueOf(100000), BigInteger.valueOf(100100)),
                new SortedLongGenerator(0, Long.MAX_VALUE - 1));
    }

    private void testSortIncremental(final String ctxt, final int size, int seed, MutableInt numSteps) {
        final int maxSteps = numSteps.get();
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo = getIncrementalColumnInfo();
        final QueryTable queryTable = getTable(size, random, columnInfo);

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.sort("Sym")),
                EvalNugget.from(() -> queryTable.update("x = Indices").sortDescending("intCol")),
                EvalNugget.from(() -> queryTable.updateView("x = Indices").sort("Sym", "intCol"))
                        .hasUnstableColumns("x"),
                EvalNugget.from(() -> queryTable.groupBy("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.groupBy("Sym").sort("Sym").update("x=sum(intCol)")),
                EvalNugget.from(() -> queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sort(List.of(
                        SortColumn.asc(ColumnName.of("Sym")), SortColumn.desc(ColumnName.of("intCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(
                        SortColumn.asc(ColumnName.of("Sym")), SortColumn.desc(ColumnName.of("intCol")),
                        SortColumn.asc(ColumnName.of("doubleCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("floatCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("doubleCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("byteCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("shortCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("intCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("boolCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("longCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("charCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("bigI"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.asc(ColumnName.of("bigD"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("floatCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("doubleCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("byteCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("shortCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("intCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("boolCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("longCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("charCol"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("bigI"))))),
                EvalNugget.from(() -> queryTable.sort(List.of(SortColumn.desc(ColumnName.of("bigD"))))),
        };

        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
            simulateShiftAwareStep(ctxt + " step == " + numSteps.get(), size, random, queryTable, columnInfo, en);
        }
    }

    /**
     * Test sort performance on a variety of scenarios.
     */
    public void testSortPerformance() {
        final long large = 100000000;
        // sequence: 0, 10, 20, 30, ...
        // If increasing sort, always add at end
        // If decreasing sort, always add at beginning
        // Expected performance: very fast. Expect add ratio of 100%, others 0.
        performTests("simple increasing", 1, 0, 0,
                ii -> ii * 10);

        // sequence: 0, -10, -20, -30, -40
        // increasing sort: always add at beginning
        // decreasing sort: always add at end
        // Expected performance: very fast. Expect add ratio of 100%, others 0.
        performTests("simple decreasing", 1, 0, 0,
                ii -> -ii * 10);

        // sequence: 0, -10, 20, -30, 40, -50
        // (both increasing and decreasing sort) alternates between adding at end and adding at beginning
        // Expected performance: very fast. Expect add ratio of 100%, others 0.
        performTests("external alternating", 1, 0, 0,
                ii -> (ii % 2) == 0 ? ii * 10 : -ii * 10);

        // (Just for the sake of the readability of this comment, assume "large" is 100)
        // sequence: 100, -99, 98, -97, 96, -95, 94, -93, ...
        // (both increasing and decreasing sort): always insert at the middle
        // Expected performance: modest (sometimes getting lucky, sometimes being forced to move elements, sometimes
        // being forced to re-spread).
        // Expected performance: medium? Not really sure what numbers to use here. Just going to say 20, 20, 75.
        // The actual numbers will be a function of test size.
        performTests("internal always near median", 20, 20, 100,
                ii -> (ii % 2) == 0 ? large - ii : -large + ii);

        // sequence: large, large+1, large+2, ..., large+9, 0, 1, 2, 3, 4, ...
        // increasing sort: always insert at a position 10 before back
        // decreasing sort: insert at a position further and further away from the front
        // Expected performance: Should be very fast (just moving 10 elements each time).
        // Rationale:
        // In the sort-ascending case, (once things get going), the new elements are to the right of the median, so the
        // code will be operating in the forward direction and always simply push [large...large+9] to the right.
        //
        // In the sort-descending case, (once things get going), the new elements are to the left of the median, so
        // the code will be operating in the reverse direction and always simply push [large+9...large] to the left.
        // Ratios: 1 add, 0 remove, 10 modified.
        performTests("block of 10 at end", 1, 0, 10,
                ii -> ii < 10 ? large + ii : ii - 10);

        // sequence: 0, 1, 2, ..., 9, large, large-1, large-2, ...
        // increasing sort: always insert at a position 10 after front
        // decreasing sort: insert at a position further and further away from the back
        // Expected performance: Should be very fast (just moving 10 elements each time). Similar rationale to the above
        // Ratios: 1 add, 0 remove, 10 modified.
        performTests("block of 10 at beginning", 1, 0, 10,
                ii -> ii < 10 ? ii : large - ii + 10);
    }

    private void performTests(String what, long addedRatioLimit, long removedRatioLimit, long modifiedRatioLimit,
            LongUnaryOperator generator) {
        performTestsInDirection(what, addedRatioLimit, removedRatioLimit, modifiedRatioLimit, generator, true);
        performTestsInDirection(what, addedRatioLimit, removedRatioLimit, modifiedRatioLimit, generator, false);
    }

    private void performTestsInDirection(String what, long addedRatioLimit, long removedRatioLimit,
            long modifiedRatioLimit, LongUnaryOperator generator, boolean ascending) {
        final int numValues = 10000;
        final long[] values = new long[numValues];

        for (int ii = 0; ii < numValues; ++ii) {
            values[ii] = generator.applyAsLong(ii);
        }

        final QueryTable queryTable = TstUtils.testRefreshingTable(i(0).toTracking(),
                col("intCol", values[0]));

        final QueryTable sorted = (QueryTable) (ascending ? queryTable.sort("intCol")
                : queryTable.sortDescending("intCol"));

        final io.deephaven.engine.table.impl.SimpleListener simpleListener =
                new io.deephaven.engine.table.impl.SimpleListener(sorted);
        sorted.addUpdateListener(simpleListener);

        long adds = 0, removes = 0, modifies = 0, shifts = 0, modifiedColumns = 0;

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int ii = 1; ii < values.length; ++ii) {
            final int fii = ii;
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(queryTable, i(fii), col("intCol", values[fii]));
                queryTable.notifyListeners(i(fii), i(), i());
            });

            adds += simpleListener.update.added().size();
            removes += simpleListener.update.removed().size();
            modifies += simpleListener.update.modified().size();
            shifts += simpleListener.update.shifted().getEffectiveSize();
            modifiedColumns += simpleListener.update.modifiedColumnSet().size();
        }

        final double denominator = values.length - 1;

        final double addedRatio = adds / denominator;
        final double removedRatio = removes / denominator;
        final double modifiedRatio = modifies / denominator;

        final String description = String.format("\"%s\": sort %s", what, ascending ? "ascending" : "descending");
        System.out.println("Results for: " + description);
        System.out.println("Add Ratio: " + addedRatio);
        System.out.println("Removed Ratio: " + removedRatio);
        System.out.println("Modified Ratio: " + modifiedRatio);

        Assert.leq(addedRatio, "addedRatio", addedRatioLimit, "addedRatioLimit");
        Assert.leq(removedRatio, "removedRatio", removedRatioLimit, "removedRatioLimit");
        Assert.leq(modifiedRatio, "modifiedRatio", modifiedRatioLimit, "modifiedRatioLimit");

        simpleListener.close();
    }

    public void testSortIncremental() {
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6).toTracking(),
                col("Sym", "aa", "bc", "aa", "aa"),
                col("intCol", 10, 20, 30, 50),
                col("doubleCol", 0.1, 0.2, 0.3, 0.5));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                EvalNugget.from(() -> queryTable.sort("Sym").view("Sym")),
                EvalNugget.from(() -> queryTable.update("x = k").sortDescending("intCol")),
                EvalNugget.from(() -> queryTable.updateView("x = k").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.groupBy("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.groupBy("Sym").sort("Sym").update("x=sum(intCol)")),
                EvalNugget.from(() -> queryTable.groupBy("Sym", "intCol").sort("Sym", "intCol").update("x=intCol+1")),
                new TableComparator(
                        queryTable.updateView("ok=k").sort(List.of(
                                SortColumn.asc(ColumnName.of("Sym")), SortColumn.desc(ColumnName.of("intCol")))),
                        "Single Sort", queryTable.updateView("ok=k").sortDescending("intCol").sort("Sym"),
                        "Double Sort")
        };

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), col("Sym", "aa", "aa"), col("intCol", 20, 10), col("doubleCol", 2.1, 2.2));
            queryTable.notifyListeners(i(3, 9), i(), i());
        });
        TstUtils.validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(1, 9), col("Sym", "bc", "aa"), col("intCol", 30, 11), col("doubleCol", 2.1, 2.2));
            queryTable.notifyListeners(i(), i(), i(1, 9));
        });
        TstUtils.validate(en);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(queryTable, i(9));
            queryTable.notifyListeners(i(), i(9), i());
        });
        TstUtils.validate(en);

    }

    public void testSortFloatIncremental() {
        final Random random = new Random(0);
        final ColumnInfo<?, ?>[] columnInfo;
        final QueryTable queryTable = getTable(100, random,
                columnInfo = initColumnInfos(new String[] {"intCol", "doubleCol", "floatCol"},
                        new IntGenerator(10, 10000),
                        new DoubleGenerator(0, 1000, 0.1, 0.1, 0.05, 0.05),
                        new FloatGenerator(0, 1000, 0.1, 0.1, 0.05, 0.05)));
        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("doubleCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("doubleCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("floatCol");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("floatCol");
                    }
                }
        };
        final int steps = 50; // 8;
        for (int i = 0; i < steps; i++) {
            simulateShiftAwareStep("floatSort step == " + i, 100, random, queryTable, columnInfo, en);
        }
    }

    public void testGrowingMergeReinterpret() {
        final QueryTable table = testRefreshingTable(i(1).toTracking(), col("Sentinel", 1));
        final Table viewed = table.update("Timestamp='2019-04-11T09:30 NY' + (ii * 60L * 1000000000L)");
        final Table sorted = TableTools.merge(viewed, viewed).sortDescending("Timestamp");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int ii = 2; ii < 10000; ++ii) {
            // Use large enough indices that we blow beyond merge's initially reserved 64k key-space.
            final int fii = 8059 * ii;
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(table, i(fii), col("Sentinel", fii));
                table.notifyListeners(i(fii), i(), i());
            });
        }

        TableTools.show(sorted);
    }

    public void testMergedReintrepret() throws IOException {
        diskBackedTestHarness(this::doReinterpretTest);
    }

    public void testMergedReintrepretIncremental() throws IOException {
        diskBackedTestHarness(this::doReinterpretTestIncremental);
    }

    private void doReinterpretTest(Table table) {
        final Table merged = TableTools.merge(table.updateView("Sentinel=Sentinel + 100"), table);

        final Table mergeSorted = merged.sort("Timestamp");

        final TIntList sentinels = new TIntArrayList();
        mergeSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}),
                sentinels);
        sentinels.clear();

        final Table mergeSorted2 = merged.sort("Timestamp");

        mergeSorted2.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}),
                sentinels);
        sentinels.clear();

        final Table boolSorted = merged.sort("Truthiness");
        boolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {102, 105, 108, 2, 5, 8, 101, 104, 107, 1, 4, 7, 100, 103, 106, 109, 0, 3, 6, 9}),
                sentinels);
        sentinels.clear();

        // we are redirecting the union now, which should also be reinterpreted
        final Table boolInverseSorted = boolSorted.sortDescending("Timestamp");
        boolInverseSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {107, 7, 103, 3, 106, 6, 102, 2, 109, 9, 105, 5, 101, 1, 108, 8, 104, 4, 100, 0}),
                sentinels);
        sentinels.clear();
    }

    private void doReinterpretTestIncremental(Table table) {
        final Table merged = TableTools.merge(table.updateView("Sentinel=Sentinel + 100"), table);
        final IncrementalReleaseFilter filter = new IncrementalReleaseFilter(5, 5);
        final Table filtered = merged.where(filter);
        final Table mergeSorted = filtered.sort("Timestamp");
        final Table mergeSorted2 = filtered.sort("Timestamp");
        final Table boolSorted = filtered.sort("Truthiness");
        final Table boolInverseSorted = boolSorted.sortDescending("Timestamp");

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        while (filtered.size() < merged.size()) {
            updateGraph.runWithinUnitTestCycle(filter::run);
        }

        final TIntList sentinels = new TIntArrayList();
        mergeSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}),
                sentinels);
        sentinels.clear();


        mergeSorted2.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}),
                sentinels);
        sentinels.clear();

        boolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {102, 105, 108, 2, 5, 8, 101, 104, 107, 1, 4, 7, 100, 103, 106, 109, 0, 3, 6, 9}),
                sentinels);
        sentinels.clear();

        boolInverseSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels",
                new TIntArrayList(
                        new int[] {107, 7, 103, 3, 106, 6, 102, 2, 109, 9, 105, 5, 101, 1, 108, 8, 104, 4, 100, 0}),
                sentinels);
        sentinels.clear();
    }

    public void testDh11506() {
        final Table x = TableTools.newTable(
                col("Symbol", "B", "B", "B", "B"),
                col("X", "2", "4", "6", "8"),
                intCol("Y", 101, 102, 103, 104));
        final QueryTable y = TstUtils.testRefreshingTable(
                col("Symbol", "B", "B", "B", "B"),
                col("X", "3", "5", "7", "9"),
                intCol("Y", 105, 106, 107, 108));
        final Table xb = x.groupBy("Symbol");
        final Table yb = y.groupBy("Symbol");
        final Table m = TableTools.merge(xb, yb).ungroup();
        final Table ms = m.select();
        final Table s = m.sortDescending("Symbol", "X");
        final Table ss = ms.sortDescending("Symbol", "X");
        TableTools.showWithRowSet(s);
        assertTableEquals(ss, s);
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(y, i(10), col("Symbol", "B"), col("X", "5"), intCol("Y", 109));
            y.notifyListeners(i(10), i(), i());
        });
        assertTableEquals(ss, s);
    }

    public void testSymbolTableSort() throws IOException {
        diskBackedTestHarness(this::doSymbolTableTest);
    }

    private void doSymbolTableTest(Table table) {
        assertEquals(10, table.size());

        final Table symbolSorted = table.sort("Symbol");
        showWithRowSet(symbolSorted);

        final TIntList sentinels = new TIntArrayList();
        symbolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[] {0, 3, 6, 9, 1, 4, 7, 2, 5, 8}), sentinels);
        sentinels.clear();

        final Table tsSorted = table.sort("Timestamp");
        showWithRowSet(tsSorted);
        tsSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[] {0, 4, 8, 1, 5, 9, 2, 6, 3, 7}), sentinels);
        sentinels.clear();

        final Table boolSorted = table.sort("Truthiness");
        showWithRowSet(boolSorted);
        boolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[] {2, 5, 8, 1, 4, 7, 0, 3, 6, 9}), sentinels);
        sentinels.clear();
    }

    public void testSymbolTableSortIncremental() throws IOException {
        diskBackedTestHarness(this::doSymbolTableIncrementalTest);
    }

    private void doSymbolTableIncrementalTest(Table table) {
        setExpectError(false);
        assertEquals(10, table.size());

        final TrackingWritableRowSet rowSet = table.getRowSet().subSetByPositionRange(0, 4).toTracking();
        final QueryTable refreshing = new QueryTable(rowSet, table.getColumnSourceMap());
        refreshing.setRefreshing(true);

        final Table symbolSorted = refreshing.sort("Symbol");
        showWithRowSet(symbolSorted);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added1 = table.getRowSet().subSetByPositionRange(4, 10);
            rowSet.insert(added1);
            refreshing.notifyListeners(added1, i(), i());
        });

        showWithRowSet(symbolSorted);

        final TIntList sentinels = new TIntArrayList();
        symbolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int) sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[] {0, 3, 6, 9, 1, 4, 7, 2, 5, 8}), sentinels);
        sentinels.clear();
    }

    private void diskBackedTestHarness(Consumer<Table> testFunction) throws IOException {
        final File testDirectory = Files.createTempDirectory("SymbolTableTest").toFile();

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("Sentinel"),
                ColumnDefinition.ofString("Symbol"),
                ColumnDefinition.ofTime("Timestamp"),
                ColumnDefinition.ofBoolean("Truthiness"));

        final String[] syms = new String[] {"Apple", "Banana", "Cantaloupe"};
        final Instant baseTime = DateTimeUtils.parseInstant("2019-04-11T09:30 NY");
        final long[] dateOffset = new long[] {0, 5, 10, 15, 1, 6, 11, 16, 2, 7};
        final Boolean[] booleans = new Boolean[] {true, false, null, true, false, null, true, false, null, true, false};
        QueryScope.addParam("syms", syms);
        QueryScope.addParam("baseTime", baseTime);
        QueryScope.addParam("dateOffset", dateOffset);
        QueryScope.addParam("booleans", booleans);

        final Table source = emptyTable(10).updateView("Sentinel=i", "Symbol=syms[i % syms.length]",
                "Timestamp=baseTime+dateOffset[i]*3600L*1000000000L", "Truthiness=booleans[i]");
        testDirectory.mkdirs();
        final File dest = new File(testDirectory, "Table.parquet");
        try {
            ParquetTools.writeTable(source, dest, definition);
            final Table table = ParquetTools.readTable(dest);
            testFunction.accept(table);
            table.close();
        } finally {
            FileUtils.deleteRecursively(testDirectory);
        }
    }

    public void testAlreadySorted() {
        final Table t = emptyTable(10000).update("Key=i");
        final Table s = t.sort("Key");
        assertSame(t.getRowSet(), s.getRowSet());
        final Table sd = t.sortDescending("Key");
        assertNotSame(t.getRowSet(), sd.getRowSet());
    }
}
