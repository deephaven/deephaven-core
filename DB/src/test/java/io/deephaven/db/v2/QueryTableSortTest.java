package io.deephaven.db.v2;

import io.deephaven.base.FileUtils;
import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.*;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.select.QueryScope;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.ParquetTools;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.select.IncrementalReleaseFilter;
import io.deephaven.db.v2.utils.ColumnHolder;
import io.deephaven.db.v2.utils.Index;
import gnu.trove.list.TIntList;
import gnu.trove.list.array.TIntArrayList;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.LongUnaryOperator;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.*;
import static io.deephaven.db.v2.TstUtils.*;

@Category(OutOfBandTest.class)
public class QueryTableSortTest extends QueryTableTestBase {

    public void testSort() {
        final Table result0 = newTable(c("Unsorted", 3.0, null, 2.0), c("DataToSort", "c", "a", "b"));
        show(result0.sort("Unsorted"));
        assertEquals(Arrays.asList(null, 2.0, 3.0), Arrays.asList(result0.sort("Unsorted").getColumn("Unsorted").get(0, 3)));
        show(result0.sortDescending("Unsorted"));
        assertEquals(Arrays.asList(3.0, 2.0, null), Arrays.asList(result0.sortDescending("Unsorted").getColumn("Unsorted").get(0, 3)));

        Table result1 = newTable(c("Unsorted", 4.0, 3.0, 1.1, Double.NaN, 2.0, 1.0, 5.0), c("DataToSort", "e", "d", "b", "g", "c", "a", "f"));
        final Table nanSorted = result1.sort("Unsorted");
        show(nanSorted);
        assertEquals(Arrays.asList(1.0, 1.1, 2.0, 3.0, 4.0, 5.0, Double.NaN), Arrays.asList(nanSorted.getColumn("Unsorted").get(0, 7)));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), Arrays.asList(nanSorted.getColumn("DataToSort").get(0, 7)));

        result1 = newTable(c("Unsorted", 4.1f, 3.1f, 1.2f, Float.NaN, 2.1f, 1.1f, 5.1f), c("DataToSort", "e", "d", "b", "g", "c", "a", "f"));
        final Table nanFloatSorted = result1.sort("Unsorted");
        System.out.println("result1");
        show(result1);
        System.out.println("nanFloatedSorted");
        show(nanFloatSorted);
        assertEquals(Arrays.asList(1.1f, 1.2f, 2.1f, 3.1f, 4.1f, 5.1f, Float.NaN), Arrays.asList(nanFloatSorted.getColumn("Unsorted").get(0, 7)));
        assertEquals(Arrays.asList("a", "b", "c", "d", "e", "f", "g"), Arrays.asList(nanFloatSorted.getColumn("DataToSort").get(0, 7)));


        Table result = newTable(c("Unsorted", 3, 1, 2), c("DataToSort", "c", "a", "b")).sort("DataToSort");
        assertEquals(Arrays.asList(1, 2, 3), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));
        result = newTable(c("Unsorted", 3, 1, 2), c("DataToSort", "c", "a", "b")).sortDescending("DataToSort");
        assertEquals(Arrays.asList(3, 2, 1), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));

        result = newTable(c("Unsorted", '3', '1', '2'), c("DataToSort", "c", "a", "b")).sort("Unsorted");
        assertEquals(Arrays.asList('1', '2', '3'), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));
        result = newTable(c("Unsorted", '3', '1', '2'), c("DataToSort", "c", "a", "b")).sortDescending("Unsorted");
        assertEquals(Arrays.asList('3', '2', '1'), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));

        final ColumnHolder c1 = TstUtils.cG("Unsorted", 3, 1, 2);
        final Table table = newTable(c1, c("DataToSort", "c", "a", "b"));
        result = table.sort("DataToSort");
        assertEquals(Arrays.asList(1, 2, 3), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));
        final ColumnHolder c11 = TstUtils.cG("Unsorted", 3, 1, 2);
        result = newTable(c11, c("DataToSort", "c", "a", "b")).sortDescending("DataToSort");
        assertEquals(Arrays.asList(3, 2, 1), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));

        final ColumnHolder c2 = TstUtils.cG("Unsorted", '3', '1', '2');
        result = newTable(c2, c("DataToSort", "c", "a", "b")).sort("Unsorted");
        assertEquals(Arrays.asList('1', '2', '3'), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("a", "b", "c"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));
        final ColumnHolder c22 = TstUtils.cG("Unsorted", '3', '1', '2');
        result = newTable(c22, c("DataToSort", "c", "a", "b")).sortDescending("Unsorted");
        assertEquals(Arrays.asList('3', '2', '1'), Arrays.asList(result.getColumn("Unsorted").get(0, 3)));
        assertEquals(Arrays.asList("c", "b", "a"), Arrays.asList(result.getColumn("DataToSort").get(0, 3)));

        final Table input = newTable(c("C1", 2, 4, 2, 4), c("C2", '1', '1', '2', '2'), c("Witness", "a", "b", "c", "d"));
        System.out.println("Input:");
        TableTools.showWithIndex(input);
        result = input.sort("C1", "C2");
        System.out.println("Result:");
        TableTools.showWithIndex(result);
        assertEquals(Arrays.asList(2, 2, 4, 4), Arrays.asList(result.getColumn("C1").get(0, 4)));
        assertEquals(Arrays.asList('1', '2', '1', '2'), Arrays.asList(result.getColumn("C2").get(0, 4)));
        assertEquals(Arrays.asList("a", "c", "b", "d"), Arrays.asList(result.getColumn("Witness").get(0, 4)));

        result = newTable(c("C1", 2, 4, 2, 4), c("C2", '2', '2', '1', '1'), c("Witness", "a", "b", "c", "d")).sort("C2", "C1");
        assertEquals(Arrays.asList(2, 4, 2, 4), Arrays.asList(result.getColumn("C1").get(0, 4)));
        assertEquals(Arrays.asList('1', '1', '2', '2'), Arrays.asList(result.getColumn("C2").get(0, 4)));
        assertEquals(Arrays.asList("c", "d", "a", "b"), Arrays.asList(result.getColumn("Witness").get(0, 4)));

        result = newTable(c("C1", 2, 4, 2, 4), c("C2", '1', '1', '2', '2'), c("Witness", "a", "b", "c", "d")).sortDescending("C1", "C2");
        assertEquals(Arrays.asList(4, 4, 2, 2), Arrays.asList(result.getColumn("C1").get(0, 4)));
        assertEquals(Arrays.asList('2', '1', '2', '1'), Arrays.asList(result.getColumn("C2").get(0, 4)));
        assertEquals(Arrays.asList("d", "b", "c", "a"), Arrays.asList(result.getColumn("Witness").get(0, 4)));

        result = newTable(c("C1", 2, 4, 2, 4), c("C2", '2', '2', '1', '1'), c("Witness", "a", "b", "c", "d")).sortDescending("C2", "C1");
        assertEquals(Arrays.asList(4, 2, 4, 2), Arrays.asList(result.getColumn("C1").get(0, 4)));
        assertEquals(Arrays.asList('2', '2', '1', '1'), Arrays.asList(result.getColumn("C2").get(0, 4)));
        assertEquals(Arrays.asList("b", "a", "d", "c"), Arrays.asList(result.getColumn("Witness").get(0, 4)));


        final ColumnHolder c3 = TstUtils.cG("Unsorted", '3', '1', '2', null);
        result = newTable(c3, c("DataToSort", "c", "a", "b", "d")).sort("Unsorted");
        show(result);
        assertEquals(Arrays.asList(null, '1', '2', '3'), Arrays.asList(result.getColumn("Unsorted").get(0, 4)));
        assertEquals(Arrays.asList("d", "a", "b", "c"), Arrays.asList(result.getColumn("DataToSort").get(0, 4)));
        final ColumnHolder c4 = TstUtils.cG("Unsorted", '3', '1', null, '2');
        result = newTable(c4, c("DataToSort", "c", "a", "d", "b")).sortDescending("Unsorted");
        assertEquals(Arrays.asList('3', '2', '1', null), Arrays.asList(result.getColumn("Unsorted").get(0, 4)));
        assertEquals(Arrays.asList("c", "b", "a", "d"), Arrays.asList(result.getColumn("DataToSort").get(0, 4)));
    }

    public void testSort2() {
        final QueryTable table = testRefreshingTable(i(10,20,30), c("A", 3, 1, 2), c("B", "c", "a", "b"));

        final QueryTable sorted = (QueryTable)table.sort("A");
        show(sorted);
        assertTableEquals(testTable(c("A", 1, 2, 3), c("B", "a", "b", "c")), sorted);
        assertTrue(SortedColumnsAttribute.isSortedBy(sorted, "A", SortingOrder.Ascending));

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(20), c("A", 1), c("B", "A"));
            table.notifyListeners(i(), i(), i(20));
        });
        show(sorted);

        assertTableEquals(testTable(c("A", 1, 2, 3), c("B", "A", "b", "c")), sorted);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(20), c("A", 1), c("B", "A2"));
            addToTable(table, i(25), c("A", 1), c("B", "A2'"));
            table.notifyListeners(i(25), i(), i(20));
        });
        show(sorted);

        assertTableEquals(testTable(c("A", 1, 1, 2, 3), c("B", "A2", "A2'", "b", "c")),sorted);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(table, i(20, 25), c("A", 1, 3), c("B", "A3", "C2"));
            table.notifyListeners(i(), i(), i(20, 25));
        });
        show(sorted);

        assertTableEquals(testRefreshingTable(c("A", 1, 2, 3, 3), c("B", "A3", "b", "c", "C2")), sorted);

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
        final Table table = testRefreshingTable(Index.FACTORY.getFlatIndex(9), cG("A", "Apple", "Apple", "Apple", "Banana", "Banana", "Banana", "Canteloupe", "Canteloupe", "Canteloupe"), c("Secondary", "C", "A", "B", "C", "A", "B", "C", "A", "B")).update("Sentinel=i");

        final QueryTable sorted = (QueryTable)table.dropColumns("Secondary").sortDescending("A");
        show(sorted);
        assertTableEquals(newTable(col("A", "Canteloupe", "Canteloupe", "Canteloupe", "Banana", "Banana", "Banana", "Apple", "Apple", "Apple"), col("Sentinel", 6, 7, 8, 3, 4, 5, 0, 1, 2)), sorted);

        final QueryTable sorted2 = (QueryTable)table.sort(SortPair.descending("A"), SortPair.ascending("Secondary"));
        show(sorted2);
        assertTableEquals(newTable(col("A", "Canteloupe", "Canteloupe", "Canteloupe", "Banana", "Banana", "Banana", "Apple", "Apple", "Apple"),
                col("Secondary", "A", "B", "C", "A", "B", "C", "A", "B", "C"),
                col("Sentinel", 7, 8, 6, 4, 5, 3, 1, 2, 0)), sorted2);
    }

    public void testGroupedSortHistorical() {
        testGroupedSortHistorical(10000);
        testGroupedSortHistorical(1000000);
    }

    private void testGroupedSortHistorical(int size) {
        final String [] choices = new String[]{"Hornigold", "Jennings", "Vane", "Bellamy"};

        assertEquals(0, size % choices.length);

        final String [] values = new String[size];
        for (int ii = 0; ii < values.length; ++ii) {
            values[ii] = choices[ii % choices.length];
        }

        final Table grouped = testTable(Index.FACTORY.getFlatIndex(values.length), cG("Captain", values)).update("Sentinel=i");
        final Table nogroups = testTable(Index.FACTORY.getFlatIndex(values.length), c("Captain", values)).update("Sentinel=i");

        final Table sortedGrouped = grouped.sortDescending("Captain");
        final Table sortedNoGroups = nogroups.sortDescending("Captain");
        show(sortedGrouped);

        assertTableEquals(sortedNoGroups, sortedGrouped);
    }

    public void testSortBool() {
        final QueryTable table = testRefreshingTable(i(10,20,30,40,50), c("boolCol", false, true, null, true, false));

        final QueryTable sorted = (QueryTable)table.sort("boolCol");
        show(sorted);
        assertEquals("",diff(sorted, testRefreshingTable(c("boolCol", null, false, false, true, true)),10));
        final QueryTable descending = (QueryTable)table.sort(SortPair.descending("boolCol"));
        show(descending);
        assertEquals("",diff(descending, testRefreshingTable(c("boolCol", true, true, false, false, null)),10));
    }

    public void testSortIncremental2() {
        final int[] sizes = {10, 100, 1000};
        for (int size : sizes) {
            testSortIncremental("size == " + size, size, 0, new MutableInt(50));
        }
    }

    public void testMultiColumnRuns() {
        final Random random = new Random(0);
        final ColumnInfo columnInfo[];
        final QueryTable queryTable = getTable(10000, random, columnInfo = initColumnInfos(new String[]{"bool1", "bool2", "bool3", "bool4", "bool5", "Sentinel"},
                new BooleanGenerator(0.25, 0.25),
                new BooleanGenerator(0.50, 0.25),
                new BooleanGenerator(),
                new BooleanGenerator(),
                new BooleanGenerator(),
                new IntGenerator(0, 100000)
        ));

        final EvalNugget en[] = new EvalNugget[]{
                EvalNugget.from(() -> queryTable.sort("bool1")),
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("bool1","bool2", "bool3", "bool4", "bool5");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sort("bool5","bool4", "bool3", "bool2", "bool1");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("bool1","bool2", "bool3", "bool4", "bool5");
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return queryTable.sortDescending("bool5","bool4", "bool3", "bool2", "bool1");
                    }
                },
        };
        final int steps = 50;
        for (int step = 0; step < steps; step++) {
            simulateShiftAwareStep(" step == " + step, 10000, random, queryTable, columnInfo, en);
        }
    }

    private ColumnInfo[] getIncrementalColumnInfo() {
        return initColumnInfos(new String[]{"Sym", "intCol", "doubleCol", "floatCol", "longCol", "shortCol", "byteCol", "charCol", "boolCol", "bigI", "bigD", "Keys"},
                new SetGenerator<>("a", "b","c","d"),
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
                new SortedLongGenerator(0, Long.MAX_VALUE - 1)
        );
    }

    private void testSortIncremental(final String ctxt, final int size, int seed, MutableInt numSteps) {
        final int maxSteps = numSteps.intValue();
        final Random random = new Random(seed);
        final ColumnInfo[] columnInfo = getIncrementalColumnInfo();
        final QueryTable queryTable = getTable(size, random, columnInfo);

        final EvalNugget en[] = new EvalNugget[]{
                EvalNugget.from(() -> queryTable.sort("Sym")),
                EvalNugget.from(() -> queryTable.update("x = Keys").sortDescending("intCol")),
                EvalNugget.from(() -> queryTable.updateView("x = Keys").sort("Sym", "intCol"))
                        .hasUnstableColumns("x"),
                EvalNugget.from(() -> queryTable.by("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.by("Sym", "intCol").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.by("Sym").sort("Sym").update("x=sum(intCol)")),
                EvalNugget.from(() -> queryTable.by("Sym", "intCol").sort("Sym", "intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("Sym"), SortPair.descending("intCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("Sym"), SortPair.descending("intCol"), SortPair.ascending("doubleCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("floatCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("doubleCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("byteCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("shortCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("intCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("boolCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("longCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("charCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("bigI"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.ascending("bigD"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("floatCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("doubleCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("byteCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("shortCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("intCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("boolCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("longCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("charCol"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("bigI"))),
                EvalNugget.from(() -> queryTable.sort(SortPair.descending("bigD"))),
        };

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            simulateShiftAwareStep(ctxt + " step == " + numSteps.getValue(), size, random, queryTable, columnInfo, en);
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
        //   In the sort-ascending case, (once things get going), the new elements are to the right of the median, so the
        //   code will be operating in the forward direction and always simply push [large...large+9] to the right.
        //
        //   In the sort-descending case, (once things get going), the new elements are to the left of the median, so
        //   the code will be operating in the reverse direction and always simply push [large+9...large] to the left.
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

        final QueryTable queryTable = TstUtils.testRefreshingTable(i(0),
                c("intCol", values[0]));

        final QueryTable sorted = (QueryTable) (ascending ? queryTable.sort("intCol")
                : queryTable.sortDescending("intCol"));

        final SimpleShiftAwareListener simpleListener = new SimpleShiftAwareListener(sorted);
        sorted.listenForUpdates(simpleListener);

        long adds = 0, removes = 0, modifies = 0, shifts = 0, modifiedColumns = 0;

        for (int ii = 1; ii < values.length; ++ii) {
            final int fii = ii;
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(queryTable, i(fii), c("intCol", values[fii]));
                queryTable.notifyListeners(i(fii), i(), i());
            });

            adds += simpleListener.update.added.size();
            removes += simpleListener.update.removed.size();
            modifies += simpleListener.update.modified.size();
            shifts += simpleListener.update.shifted.getEffectiveSize();
            modifiedColumns += simpleListener.update.modifiedColumnSet.size();
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
        final QueryTable queryTable = TstUtils.testRefreshingTable(i(1, 2, 4, 6),
                c("Sym", "aa", "bc", "aa", "aa"),
                c("intCol", 10, 20, 30, 50),
                c("doubleCol", 0.1, 0.2, 0.3, 0.5));

        final EvalNuggetInterface[] en = new EvalNuggetInterface[]{
                EvalNugget.from(() -> queryTable.sort("Sym").view("Sym")),
                EvalNugget.from(() -> queryTable.update("x = k").sortDescending("intCol")),
                EvalNugget.from(() -> queryTable.updateView("x = k").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.by("Sym").sort("Sym")),
                EvalNugget.from(() -> queryTable.by("Sym", "intCol").sort("Sym", "intCol")),
                EvalNugget.from(() -> queryTable.sort("Sym").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1")),
                EvalNugget.from(() -> queryTable.by("Sym").sort("Sym").update("x=sum(intCol)")),
                EvalNugget.from(() -> queryTable.by("Sym", "intCol").sort("Sym", "intCol").update("x=intCol+1")),
                new TableComparator(queryTable.updateView("ok=k").sort(SortPair.ascending("Sym"), SortPair.descending("intCol")), "Single Sort", queryTable.updateView("ok=k").sortDescending("intCol").sort("Sym"), "Double Sort")
        };

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(3, 9), c("Sym", "aa", "aa"), c("intCol", 20, 10), c("doubleCol", 2.1, 2.2));
            queryTable.notifyListeners(i(3, 9), i(), i());
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            addToTable(queryTable, i(1, 9), c("Sym", "bc", "aa"), c("intCol", 30, 11), c("doubleCol", 2.1, 2.2));
            queryTable.notifyListeners(i(), i(), i(1, 9));
        });
        TstUtils.validate(en);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            TstUtils.removeRows(queryTable, i(9));
            queryTable.notifyListeners(i(), i(9), i());
        });
        TstUtils.validate(en);

    }

    public void testSortFloatIncremental() {
        final Random random = new Random(0);
        final ColumnInfo columnInfo[];
        final QueryTable queryTable = getTable(100, random, columnInfo = initColumnInfos(new String[]{"intCol", "doubleCol", "floatCol"},
                new IntGenerator(10, 10000),
                new DoubleGenerator(0, 1000, 0.1, 0.1, 0.05, 0.05),
                new FloatGenerator(0, 1000, 0.1, 0.1, 0.05, 0.05))
                );
        final EvalNugget en[] = new EvalNugget[]{
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
        final QueryTable table = testRefreshingTable(i(1), c("Sentinel", 1));
        final Table viewed = table.update("Timestamp='2019-04-11T09:30 NY' + (ii * 60L * 1000000000L)");
        final Table sorted = TableTools.merge(viewed, viewed).sortDescending("Timestamp");

        for (int ii = 2; ii < 10000; ++ii) {
            // Use large enough indices that we blow beyond merge's initially reserved 64k key-space.
            final int fii = 8059 * ii;
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
                addToTable(table, i(fii), c("Sentinel", fii));
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
        mergeSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}), sentinels);
        sentinels.clear();

        final Table mergeSorted2 = merged.sort("Timestamp");

        mergeSorted2.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}), sentinels);
        sentinels.clear();

        final Table boolSorted = merged.sort("Truthiness");
        boolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{102, 105, 108, 2,  5, 8, 101, 104, 107, 1, 4, 7, 100, 103, 106, 109, 0, 3, 6, 9}), sentinels);
        sentinels.clear();

        // we are redirecting the union now, which should also be reinterpreted
        final Table boolInverseSorted = boolSorted.sortDescending("Timestamp");
        boolInverseSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{107, 7, 103, 3, 106, 6, 102, 2, 109, 9, 105, 5, 101, 1, 108, 8, 104, 4, 100, 0}), sentinels);
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

        while (filtered.size() < merged.size()) {
            LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(filter::refresh);
        }

        final TIntList sentinels = new TIntArrayList();
        mergeSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}), sentinels);
        sentinels.clear();


        mergeSorted2.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{100, 0, 104, 4, 108, 8, 101, 1, 105, 5, 109, 9, 102, 2, 106, 6, 103, 3, 107, 7}), sentinels);
        sentinels.clear();

        boolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{102, 105, 108, 2,  5, 8, 101, 104, 107, 1, 4, 7, 100, 103, 106, 109, 0, 3, 6, 9}), sentinels);
        sentinels.clear();

        boolInverseSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{107, 7, 103, 3, 106, 6, 102, 2, 109, 9, 105, 5, 101, 1, 108, 8, 104, 4, 100, 0}), sentinels);
        sentinels.clear();
    }

    public void testSymbolTableSort() throws IOException {
        diskBackedTestHarness(this::doSymbolTableTest);
    }

    private void doSymbolTableTest(Table table) {
        assertEquals(10, table.size());

        final Table symbolSorted = table.sort("Symbol");
        TableTools.showWithIndex(symbolSorted);

        final TIntList sentinels = new TIntArrayList();
        symbolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{0, 3, 6, 9, 1, 4, 7, 2, 5, 8}), sentinels);
        sentinels.clear();

        final Table tsSorted = table.sort("Timestamp");
        TableTools.showWithIndex(tsSorted);
        tsSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{0, 4, 8, 1, 5, 9, 2, 6, 3, 7}), sentinels);
        sentinels.clear();

        final Table boolSorted = table.sort("Truthiness");
        TableTools.showWithIndex(boolSorted);
        boolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{2, 5, 8, 1, 4, 7, 0, 3, 6, 9}), sentinels);
        sentinels.clear();
    }

    public void testSymbolTableSortIncremental() throws IOException {
        diskBackedTestHarness(this::doSymbolTableIncrementalTest);
    }

    private void doSymbolTableIncrementalTest(Table table) {
        setExpectError(false);
        assertEquals(10, table.size());

        final Index index = table.getIndex().subindexByPos(0, 4);
        final QueryTable refreshing = new QueryTable(index, table.getColumnSourceMap());
        refreshing.setRefreshing(true);

        final Table symbolSorted = refreshing.sort("Symbol");
        TableTools.showWithIndex(symbolSorted);

        LiveTableMonitor.DEFAULT.runWithinUnitTestCycle(() -> {
            final Index added = table.getIndex().subindexByPos(4, 10);
            index.insert(added);
            refreshing.notifyListeners(added, i(), i());
        });

        TableTools.showWithIndex(symbolSorted);

        final TIntList sentinels = new TIntArrayList();
        symbolSorted.columnIterator("Sentinel").forEachRemaining(sentinel -> sentinels.add((int)sentinel));
        assertEquals("sentinels", new TIntArrayList(new int[]{0, 3, 6, 9, 1, 4, 7, 2, 5, 8}), sentinels);
        sentinels.clear();
    }

    private void diskBackedTestHarness(Consumer<Table> testFunction) throws IOException {
        final File testDirectory = Files.createTempDirectory("SymbolTableTest").toFile();

        final TableDefinition definition = TableDefinition.of(
                ColumnDefinition.ofInt("Sentinel"),
                ColumnDefinition.ofString("Symbol"),
                ColumnDefinition.ofTime("Timestamp"),
                ColumnDefinition.ofBoolean("Truthiness"));

        final String [] syms = new String[]{"Apple", "Banana", "Cantaloupe"};
        final DBDateTime baseTime = DBTimeUtils.convertDateTime("2019-04-11T09:30 NY");
        final long dateOffset [] = new long[]{0, 5, 10, 15, 1, 6, 11, 16, 2, 7};
        final Boolean booleans [] = new Boolean[]{true, false, null, true, false, null, true, false, null, true, false};
        QueryScope.addParam("syms", syms);
        QueryScope.addParam("baseTime", baseTime);
        QueryScope.addParam("dateOffset", dateOffset);
        QueryScope.addParam("booleans", booleans);

        final Table source = emptyTable(10).updateView("Sentinel=i", "Symbol=syms[i % syms.length]", "Timestamp=baseTime+dateOffset[i]*3600L*1000000000L", "Truthiness=booleans[i]");
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
        assertSame(t, s);
        final Table sd = t.sortDescending("Key");
        assertNotSame(t, sd);
    }
}
