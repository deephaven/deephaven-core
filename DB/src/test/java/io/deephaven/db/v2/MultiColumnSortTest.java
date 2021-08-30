package io.deephaven.db.v2;

import io.deephaven.db.tables.SortPair;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.test.types.SerialTest;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import io.deephaven.benchmarking.generator.EnumStringColumnGenerator;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigInteger;
import java.util.*;

import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;

@Category(SerialTest.class)
public class MultiColumnSortTest {
    @Before
    public void setUp() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void teardown() {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void testMultiColumnSort() {
        for (int size = 10; size <= 100_000; size *= 10) {
            for (int seed = 0; seed < 1; ++seed) {
                System.out.println("Seed: " + seed);
                testMultiColumnSort(seed, size);
            }
        }
    }

    private void testMultiColumnSort(int seed, int size) {
        final Random random = new Random(seed);

        final Table table = getTable(size, random,
            initColumnInfos(
                new String[] {"Sym", "intCol", "doubleCol", "floatCol", "longCol", "shortCol",
                        "byteCol", "charCol", "boolCol", "bigI", "bigD"},
                new TstUtils.SetGenerator<>("a", "b", "c", "d", "e", "f", "g"),
                new TstUtils.IntGenerator(10, 100),
                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                new TstUtils.FloatGenerator(100.0f, 2000.f),
                new TstUtils.LongGenerator(),
                new TstUtils.ShortGenerator(),
                new TstUtils.ByteGenerator(),
                new TstUtils.CharGenerator('A', 'Z'),
                new TstUtils.BooleanGenerator(),
                new TstUtils.BigIntegerGenerator(BigInteger.valueOf(100000),
                    BigInteger.valueOf(100100)),
                new TstUtils.BigDecimalGenerator(BigInteger.valueOf(100000),
                    BigInteger.valueOf(100100))));

        final List<String> columnNames = new ArrayList<>(table.getColumnSourceMap().keySet());

        doMultiColumnTest(table, SortPair.ascending("boolCol"), SortPair.descending("Sym"));

        for (String outerColumn : columnNames) {
            final SortPair outerPair = random.nextBoolean() ? SortPair.ascending(outerColumn)
                : SortPair.descending(outerColumn);
            for (String innerColumn : columnNames) {
                if (innerColumn.equals(outerColumn)) {
                    continue;
                }
                final SortPair innerPair = random.nextBoolean() ? SortPair.ascending(innerColumn)
                    : SortPair.descending(innerColumn);
                doMultiColumnTest(table, outerPair, innerPair);
            }
        }

        // now let each type have a chance at being in the middle, but pick something else as the
        // outer type
        for (String middleColumn : columnNames) {
            final String outerColumn = oneOf(columnNames, middleColumn);
            final String innerColumn = oneOf(columnNames, middleColumn, outerColumn);
            final SortPair outerPair = random.nextBoolean() ? SortPair.ascending(outerColumn)
                : SortPair.descending(outerColumn);
            final SortPair innerPair = random.nextBoolean() ? SortPair.ascending(innerColumn)
                : SortPair.descending(innerColumn);

            doMultiColumnTest(table, outerPair, SortPair.ascending(middleColumn), innerPair);
            doMultiColumnTest(table, outerPair, SortPair.descending(middleColumn), innerPair);
        }
    }

    private <T> T oneOf(List<T> names, T... exclusions) {
        final List<T> copy = new ArrayList<>(names);
        copy.removeAll(Arrays.asList(exclusions));
        Collections.shuffle(copy);
        return copy.get(0);
    }

    private void doMultiColumnTest(Table table, SortPair... sortPairs) {
        final Table sorted = table.sort(sortPairs);

        System.out.println("SortPairs: " + Arrays.toString(sortPairs) + ", size=" + table.size());
        // TableTools.show(table);
        // TableTools.show(sorted);

        checkSort(sorted, sortPairs);
    }

    private void checkSort(Table sorted, SortPair[] sortPairs) {
        final String[] columns =
            Arrays.stream(sortPairs).map(SortPair::getColumn).toArray(String[]::new);

        Object[] lastRow = sorted.getRecord(0, columns);

        for (int ii = 1; ii < sorted.intSize(); ++ii) {
            final Object[] rowData = sorted.getRecord(ii, columns);

            for (int jj = 0; jj < rowData.length; ++jj) {
                // make sure lastRow <= rowData
                final Comparable last = (Comparable) lastRow[jj];
                final Comparable current = (Comparable) rowData[jj];
                if (sortPairs[jj].getOrder() == SortingOrder.Ascending) {
                    if (!leq(last, current)) {
                        TestCase.fail("Out of order[" + (ii - 1) + "]: !" + Arrays.toString(lastRow)
                            + " <= [" + ii + "] " + Arrays.toString(rowData));
                    }
                } else {
                    if (!geq(last, current)) {
                        TestCase.fail("Out of order[" + (ii - 1) + "]: !" + Arrays.toString(lastRow)
                            + " >= [" + ii + "] " + Arrays.toString(rowData));
                    }
                }
                if (!Objects.equals(last, current)) {
                    break;
                }
            }

            lastRow = rowData;
        }
    }

    private boolean leq(Comparable last, Comparable current) {
        if (last == null && current == null) {
            return true;
        }
        if (last == null) {
            return true;
        }
        if (current == null) {
            return false;
        }
        // noinspection unchecked
        return last.compareTo(current) <= 0;
    }

    private boolean geq(Comparable last, Comparable current) {
        if (last == null && current == null) {
            return true;
        }
        if (current == null) {
            return true;
        }
        if (last == null) {
            return false;
        }
        // noinspection unchecked
        return last.compareTo(current) >= 0;
    }

    // the benchmarks found an infinite loop when doing multi column sorts
    @Test
    public void benchmarkTest() {
        {
            final EnumStringColumnGenerator enumStringCol1 =
                (EnumStringColumnGenerator) BenchmarkTools.stringCol("Enum1", 10000, 6, 6,
                    0xB00FB00F);
            final EnumStringColumnGenerator enumStringCol2 =
                (EnumStringColumnGenerator) BenchmarkTools.stringCol("Enum2", 1000, 6, 6,
                    0xF00DF00D);

            final BenchmarkTableBuilder builder;
            final int actualSize = BenchmarkTools.sizeWithSparsity(25000000, 90);

            System.out.println("Actual Size: " + actualSize);

            builder = BenchmarkTools.persistentTableBuilder("Carlos", actualSize);

            final BenchmarkTable bmTable = builder
                .setSeed(0xDEADBEEF)
                .addColumn(BenchmarkTools.stringCol("PartCol", 4, 5, 7, 0xFEEDBEEF))
                .addColumn(BenchmarkTools.numberCol("I1", int.class))
                .addColumn(BenchmarkTools.numberCol("D1", double.class, -10e6, 10e6))
                .addColumn(BenchmarkTools.numberCol("L1", long.class))
                .addColumn(enumStringCol1)
                .addColumn(enumStringCol2)
                .build();


            final long startGen = System.currentTimeMillis();
            System.out.println(new Date(startGen) + " Generating Table.");
            final Table table = bmTable.getTable();
            final long endGen = System.currentTimeMillis();
            System.out
                .println(new Date(endGen) + " Completed generate in " + (endGen - startGen) + "ms");

            final long startSort = System.currentTimeMillis();
            System.out.println(new Date(startSort) + " Starting sort.");

            final Table sorted = table.sort("Enum1", "L1");

            final long end = System.currentTimeMillis();
            System.out.println(new Date(end) + " Completed sort in " + (end - startSort) + "ms");

            checkSort(sorted, SortPair.ascendingPairs("Enum1", "L1"));
        }
    }
}
