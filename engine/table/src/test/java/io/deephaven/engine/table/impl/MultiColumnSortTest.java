//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.collect.Sets;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortSpec;
import io.deephaven.api.SortColumn;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.SerialTest;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigInteger;
import java.time.Instant;
import java.util.*;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

@Category(SerialTest.class)
public class MultiColumnSortTest {
    private enum IndexType {
        NONE, FULL, PARTIAL
    }

    @Rule
    public EngineCleanup framework = new EngineCleanup();

    @Test
    public void testMultiColumnSort() {
        for (int size = 10; size <= 100_000; size *= 10) {
            for (int seed = 0; seed < 1; ++seed) {
                System.out.println("Seed: " + seed);
                testMultiColumnSort(seed, size, IndexType.NONE);
            }
        }
    }

    @Test
    public void testMultiColumnSortFullIndex() {
        for (int size = 10; size <= 100_000; size *= 10) {
            for (int seed = 0; seed < 1; ++seed) {
                System.out.println("Seed: " + seed);
                testMultiColumnSort(seed, size, IndexType.FULL);
            }
        }
    }

    @Test
    public void testMultiColumnSortPartialIndex() {
        for (int size = 10; size <= 100_000; size *= 10) {
            for (int seed = 0; seed < 1; ++seed) {
                System.out.println("Seed: " + seed);
                testMultiColumnSort(seed, size, IndexType.PARTIAL);
            }
        }
    }

    private void testMultiColumnSort(int seed, int size, IndexType indexType) {
        final Random random = new Random(seed);

        final Table table = getTable(size, random,
                initColumnInfos(
                        new String[] {"Sym", "intCol", "doubleCol", "floatCol", "longCol", "shortCol", "byteCol",
                                "charCol", "boolCol", "bigI", "bigD"},
                        new SetGenerator<>("a", "b", "c", "d", "e", "f", "g"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1),
                        new FloatGenerator(100.0f, 2000.f),
                        new LongGenerator(),
                        new ShortGenerator(),
                        new ByteGenerator(),
                        new CharGenerator('A', 'Z'),
                        new BooleanGenerator(),
                        new BigIntegerGenerator(BigInteger.valueOf(100000), BigInteger.valueOf(100100)),
                        new BigDecimalGenerator(BigInteger.valueOf(100000), BigInteger.valueOf(100100))));

        final List<String> columnNames = table.getDefinition().getColumnNames();

        Set<Set<String>> keyColumnPowerSet = Sets.powerSet(new HashSet<>(columnNames));

        if (indexType == IndexType.FULL) {
            // Create full indexes for every possible column subset.
            for (Set<String> keyColumnSubset : keyColumnPowerSet) {
                if (keyColumnSubset.isEmpty() || keyColumnSubset.size() == columnNames.size()) {
                    // Won't consider the empty or full set.
                    continue;
                }
                DataIndexer.getOrCreateDataIndex(table, keyColumnSubset.toArray(String[]::new));
            }
        } else if (indexType == IndexType.PARTIAL) {
            // Only create single-column indexes
            for (String keyColumn : columnNames) {
                DataIndexer.getOrCreateDataIndex(table, keyColumn);
            }
        }

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

        Object[] lastRow = TstUtils.getRowData(sorted, 0, columns);

        for (int ii = 1; ii < sorted.intSize(); ++ii) {
            final Object[] rowData = TstUtils.getRowData(sorted, ii, columns);

            for (int jj = 0; jj < rowData.length; ++jj) {
                // make sure lastRow <= rowData
                final Comparable last = (Comparable) lastRow[jj];
                final Comparable current = (Comparable) rowData[jj];
                if (sortColumns[jj].order() == SortSpec.Order.ASCENDING) {
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
            final ColumnGenerator<String> enumStringCol1 = BenchmarkTools.stringCol(
                    "Enum1", 10000, 6, 6, 0xB00FB00FL);
            final ColumnGenerator<String> enumStringCol2 = BenchmarkTools.stringCol(
                    "Enum2", 1000, 6, 6, 0xF00DF00DL);

            final BenchmarkTableBuilder builder;
            final int actualSize = BenchmarkTools.sizeWithSparsity(10_000_000, 90);

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

    @Test
    public void benchmarkFullIndexTest() {
        {
            final ColumnGenerator<String> enumStringCol1 = BenchmarkTools.stringCol(
                    "Enum1", 10000, 6, 6, 0xB00FB00FL);
            final ColumnGenerator<String> enumStringCol2 = BenchmarkTools.stringCol(
                    "Enum2", 1000, 6, 6, 0xF00DF00DL);

            final BenchmarkTableBuilder builder;
            final int actualSize = BenchmarkTools.sizeWithSparsity(10_000_000, 90);

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
            DataIndexer.getOrCreateDataIndex(table, "Enum1", "L1");

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

    @Test
    public void testSkipConstantSingleValueSortColumn() {
        final int size = 200;
        final long[] varying = new long[size];
        for (int ii = 0; ii < size; ++ii) {
            varying[ii] = (ii * 31L) % 17;
        }

        final SingleValueColumnSource<Integer> constSrc =
                SingleValueColumnSource.getSingleValueColumnSource(int.class);
        constSrc.set(42);

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put("Const", constSrc);
        columnSourceMap.put("Var", InMemoryColumnSource.getImmutableMemoryColumnSource(varying, long.class, null));
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);

        // (Const, Var) sort must produce same row content as (Var) sort, because Const is constant.
        final Table sortedBoth = table.sort("Const", "Var");
        final Table sortedVarOnly = table.sort("Var");
        TstUtils.assertTableEquals(sortedVarOnly, sortedBoth);
        checkSort(sortedBoth, SortColumn.asc(ColumnName.of("Const")), SortColumn.asc(ColumnName.of("Var")));

        // (Var, Const) sort must also produce same row content as (Var) sort.
        final Table sortedVarThenConst = table.sort("Var", "Const");
        TstUtils.assertTableEquals(sortedVarOnly, sortedVarThenConst);
        checkSort(sortedVarThenConst, SortColumn.asc(ColumnName.of("Var")), SortColumn.asc(ColumnName.of("Const")));
    }

    @Test
    public void testSkipConstantInteriorSortColumn() {
        final int size = 200;
        final int[] outer = new int[size];
        final long[] inner = new long[size];
        for (int ii = 0; ii < size; ++ii) {
            outer[ii] = ii % 5;
            inner[ii] = (ii * 31L) % 17;
        }

        final SingleValueColumnSource<Integer> constSrc =
                SingleValueColumnSource.getSingleValueColumnSource(int.class);
        constSrc.set(7);

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put("Outer", InMemoryColumnSource.getImmutableMemoryColumnSource(outer, int.class, null));
        columnSourceMap.put("Const", constSrc);
        columnSourceMap.put("Inner", InMemoryColumnSource.getImmutableMemoryColumnSource(inner, long.class, null));
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);

        final Table sortedAll = table.sort("Outer", "Const", "Inner");
        final Table sortedSkipConst = table.sort("Outer", "Inner");
        TstUtils.assertTableEquals(sortedSkipConst, sortedAll);
        checkSort(sortedAll, SortColumn.asc(ColumnName.of("Outer")), SortColumn.asc(ColumnName.of("Const")),
                SortColumn.asc(ColumnName.of("Inner")));
    }

    @Test
    public void testSkipNullValueSortColumn() {
        final int size = 100;
        final int[] varying = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            varying[ii] = (ii * 13) % 7;
        }

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put("NullCol", NullValueColumnSource.getInstance(int.class, null));
        columnSourceMap.put("Var", InMemoryColumnSource.getImmutableMemoryColumnSource(varying, int.class, null));
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);

        final Table sortedBoth = table.sort("NullCol", "Var");
        final Table sortedVarOnly = table.sort("Var");
        TstUtils.assertTableEquals(sortedVarOnly, sortedBoth);
    }

    @Test
    public void testSkipImmutableConstantInstantSortColumn() {
        final int size = 100;
        final int[] varying = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            varying[ii] = (ii * 13) % 7;
        }

        final ColumnSource<Instant> constInstant = InMemoryColumnSource.makeImmutableConstantSource(
                Instant.class, null, Instant.parse("2026-01-01T00:00:00Z"));

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put("ConstInstant", constInstant);
        columnSourceMap.put("Var", InMemoryColumnSource.getImmutableMemoryColumnSource(varying, int.class, null));
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);

        final Table sortedBoth = table.sort("ConstInstant", "Var");
        final Table sortedVarOnly = table.sort("Var");
        TstUtils.assertTableEquals(sortedVarOnly, sortedBoth);
    }

    @Test
    public void testAllConstantSortIsIdentity() {
        final int size = 50;
        final SingleValueColumnSource<Integer> constSrc =
                SingleValueColumnSource.getSingleValueColumnSource(int.class);
        constSrc.set(99);

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put("Const", constSrc);
        columnSourceMap.put("AlsoConst",
                InMemoryColumnSource.makeImmutableConstantSource(String.class, null, "x"));
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);

        final Table sorted = table.sort("Const", "AlsoConst");
        // Sorting an all-constant key set is a no-op: result should match the original table row-for-row.
        TstUtils.assertTableEquals(table, sorted);
    }

    @Test
    public void testNonEqualityRespectingComparatorOnConstantNotSkipped() {
        final int size = 100;
        final int[] varying = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            varying[ii] = (ii * 13) % 7;
        }

        final SingleValueColumnSource<String> constSrc =
                SingleValueColumnSource.getSingleValueColumnSource(String.class);
        constSrc.set("apple");

        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();
        columnSourceMap.put("ConstStr", constSrc);
        columnSourceMap.put("Var", InMemoryColumnSource.getImmutableMemoryColumnSource(varying, int.class, null));
        final QueryTable table = new QueryTable(RowSetFactory.flat(size).toTracking(), columnSourceMap);

        // respectsEquality=false: optimization MUST NOT drop the column. Result must still be a valid sort.
        final Table sortedWithComparator = table.sort(
                ComparatorSortColumn.asc("ConstStr", String.CASE_INSENSITIVE_ORDER, false),
                SortColumn.asc(ColumnName.of("Var")));
        checkSort(sortedWithComparator,
                SortColumn.asc(ColumnName.of("ConstStr")), SortColumn.asc(ColumnName.of("Var")));
        // Row content (independent of order on the constant column) must equal sorting by Var alone.
        TstUtils.assertTableEquals(table.sort("Var"), sortedWithComparator);
    }

    @Test
    public void benchmarkFirstColumnIndexTest() {
        {
            final ColumnGenerator<String> enumStringCol1 = BenchmarkTools.stringCol(
                    "Enum1", 10000, 6, 6, 0xB00FB00FL);
            final ColumnGenerator<String> enumStringCol2 = BenchmarkTools.stringCol(
                    "Enum2", 1000, 6, 6, 0xF00DF00DL);

            final BenchmarkTableBuilder builder;
            final int actualSize = BenchmarkTools.sizeWithSparsity(10_000_000, 90);

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
            DataIndexer.getOrCreateDataIndex(table, "Enum1");

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
