package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
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

import static io.deephaven.engine.table.impl.TstUtils.getTable;
import static io.deephaven.engine.table.impl.TstUtils.initColumnInfos;

@Category(SerialTest.class)
public class MultiColumnSortTest {
    @Before
    public void setUp() {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void teardown() {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
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
                        new String[] {"Sym", "intCol", "doubleCol", "floatCol", "longCol", "shortCol", "byteCol",
                                "charCol", "boolCol", "bigI", "bigD"},
                        new TstUtils.SetGenerator<>("a", "b", "c", "d", "e", "f", "g"),
                        new TstUtils.IntGenerator(10, 100),
                        new TstUtils.SetGenerator<>(10.1, 20.1, 30.1),
                        new TstUtils.FloatGenerator(100.0f, 2000.f),
                        new TstUtils.LongGenerator(),
                        new TstUtils.ShortGenerator(),
                        new TstUtils.ByteGenerator(),
                        new TstUtils.CharGenerator('A', 'Z'),
                        new TstUtils.BooleanGenerator(),
                        new TstUtils.BigIntegerGenerator(BigInteger.valueOf(100000), BigInteger.valueOf(100100)),
                        new TstUtils.BigDecimalGenerator(BigInteger.valueOf(100000), BigInteger.valueOf(100100))));

        final List<String> columnNames = new ArrayList<>(table.getColumnSourceMap().keySet());

        doMultiColumnTest(table, SortColumn.asc(ColumnName.of("boolCol")), SortColumn.desc(ColumnName.of("Sym")));

        for (String outerColumn : columnNames) {
            final SortColumn outerPair = random.nextBoolean()
                    ? SortColumn.asc(ColumnName.of(outerColumn))
                    : SortColumn.desc(ColumnName.of(outerColumn));
            for (String innerColumn : columnNames) {
                if (innerColumn.equals(outerColumn)) {
                    continue;
                }
                final SortColumn innerPair = random.nextBoolean()
                        ? SortColumn.asc(ColumnName.of(innerColumn))
                        : SortColumn.desc(ColumnName.of(innerColumn));
                doMultiColumnTest(table, outerPair, innerPair);
            }
        }

        // now let each type have a chance at being in the middle, but pick something else as the outer type
        for (String middleColumn : columnNames) {
            final String outerColumn = oneOf(columnNames, middleColumn);
            final String innerColumn = oneOf(columnNames, middleColumn, outerColumn);
            final SortColumn outerPair =
                    random.nextBoolean() ? SortColumn.asc(ColumnName.of(outerColumn))
                            : SortColumn.desc(ColumnName.of(outerColumn));
            final SortColumn innerPair =
                    random.nextBoolean() ? SortColumn.asc(ColumnName.of(innerColumn))
                            : SortColumn.desc(ColumnName.of(innerColumn));

            doMultiColumnTest(table, outerPair, SortColumn.asc(ColumnName.of(middleColumn)), innerPair);
            doMultiColumnTest(table, outerPair, SortColumn.desc(ColumnName.of(middleColumn)), innerPair);
        }
    }

    private <T> T oneOf(List<T> names, T... exclusions) {
        final List<T> copy = new ArrayList<>(names);
        copy.removeAll(Arrays.asList(exclusions));
        Collections.shuffle(copy);
        return copy.get(0);
    }

    private void doMultiColumnTest(Table table, SortColumn... sortColumns) {
        final Table sorted = table.sort(Arrays.asList(sortColumns));

        System.out.println("SortColumns: " + Arrays.toString(sortColumns) + ", size=" + table.size());
        // TableTools.show(table);
        // TableTools.show(sorted);

        checkSort(sorted, sortColumns);
    }

    private void checkSort(Table sorted, SortColumn... sortColumns) {
        final String[] columns = Arrays.stream(sortColumns).map(SortColumn::column).map(ColumnName::name)
                .toArray(String[]::new);

        Object[] lastRow = sorted.getRecord(0, columns);

        for (int ii = 1; ii < sorted.intSize(); ++ii) {
            final Object[] rowData = sorted.getRecord(ii, columns);

            for (int jj = 0; jj < rowData.length; ++jj) {
                // make sure lastRow <= rowData
                final Comparable last = (Comparable) lastRow[jj];
                final Comparable current = (Comparable) rowData[jj];
                if (sortColumns[jj].order() == SortColumn.Order.ASCENDING) {
                    if (!leq(last, current)) {
                        TestCase.fail("Out of order[" + (ii - 1) + "]: !" + Arrays.toString(lastRow) + " <= [" + ii
                                + "] " + Arrays.toString(rowData));
                    }
                } else {
                    if (!geq(last, current)) {
                        TestCase.fail("Out of order[" + (ii - 1) + "]: !" + Arrays.toString(lastRow) + " >= [" + ii
                                + "] " + Arrays.toString(rowData));
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
                    (EnumStringColumnGenerator) BenchmarkTools.stringCol("Enum1", 10000, 6, 6, 0xB00FB00F);
            final EnumStringColumnGenerator enumStringCol2 =
                    (EnumStringColumnGenerator) BenchmarkTools.stringCol("Enum2", 1000, 6, 6, 0xF00DF00D);

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
            System.out.println(new Date(endGen) + " Completed generate in " + (endGen - startGen) + "ms");

            final long startSort = System.currentTimeMillis();
            System.out.println(new Date(startSort) + " Starting sort.");

            final Table sorted = table.sort("Enum1", "L1");

            final long end = System.currentTimeMillis();
            System.out.println(new Date(end) + " Completed sort in " + (end - startSort) + "ms");

            checkSort(sorted, SortColumn.asc(ColumnName.of("Enum1")), SortColumn.asc(ColumnName.of("L1")));
        }
    }
}
