//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.collect.Sets;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortSpec;
import io.deephaven.api.SortColumn;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.DataIndexOptions;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.dataindex.AbstractDataIndex;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.sources.InMemoryColumnSource;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.table.impl.sources.SingleValueColumnSource;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.test.types.SerialTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.datastructures.SizeException;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import org.jetbrains.annotations.NotNull;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;
import static org.junit.Assert.*;

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
                        fail("Out of order[" + (ii - 1) + "]: !" + Arrays.toString(lastRow) + " <= [" + ii
                                + "] " + Arrays.toString(rowData));
                    }
                } else {
                    if (!geq(last, current)) {
                        fail("Out of order[" + (ii - 1) + "]: !" + Arrays.toString(lastRow) + " >= [" + ii
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

    /**
     * A data index on a constant sort column, which the sort drops, does not apply to the remaining sort columns: a
     * sort by (C, A) with a data index on C sorts by A.
     */
    @Test
    public void testIndexOnSkippedConstantSortColumn() {
        final Table table = TableTools.emptyTable(6).update("A = 6 - i").updateView("C = 7");
        DataIndexer.getOrCreateDataIndex(table, "C");
        final Table sorted = table.sort("C", "A");
        assertArrayEquals(new int[] {1, 2, 3, 4, 5, 6}, ColumnVectors.ofInt(sorted, "A").toArray());
    }

    /**
     * A sort drops a constant sort column and uses a data index on the remaining column: a sort by (C, A) with a data
     * index on A reads none of A's values.
     */
    @Test
    public void testIndexOnColumnAfterSkippedConstantSortColumn() {
        final int size = 3000;
        final AtomicInteger aReads = new AtomicInteger();
        QueryScope.addParam("aReads", aReads);
        final Table table = TableTools.emptyTable(size)
                .update("RawA = (int) (ii % 10)")
                .updateView("A = RawA + 0 * aReads.getAndIncrement()", "C = 7");
        // computing the index reads A, so it is computed before the reads are counted
        DataIndexer.getOrCreateDataIndex(table, "A").table();
        aReads.set(0);
        final Table sorted = table.sort("C", "A");
        assertEquals("reads of A", 0, aReads.get());
        TstUtils.assertTableEquals(table.select().sort("A"), sorted);
    }

    /**
     * BigDecimal 1.0 and 1.00 compare equal, so a sort by (BD, N) orders those rows by N. The data index on (BD, N) is
     * too selective for the full-index route, so the sort runs the one-column-at-a-time path, whose runs of the first
     * column are its ties under compareTo.
     */
    @Test
    public void testBigDecimalTiesOfFirstColumnSortedBySecondColumn() {
        final Table table = TableTools.newTable(
                TableTools.col("BD", new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1.0"),
                        new BigDecimal("1.00")),
                TableTools.intCol("N", 4, 3, 2, 1));
        assertArrayEquals(new int[] {1, 2, 3, 4}, ColumnVectors.ofInt(table.sort("BD", "N"), "N").toArray());
        DataIndexer.getOrCreateDataIndex(table, "BD", "N");
        assertArrayEquals(new int[] {1, 2, 3, 4}, ColumnVectors.ofInt(table.sort("BD", "N"), "N").toArray());
    }

    /**
     * BigDecimal 1.0 and 1.00 compare equal, so a sort by (Group, Amount, Tie) orders those rows by Tie. With a data
     * index on Group, the sort runs the one-column-at-a-time path, whose runs of the second column are its ties under
     * compareTo.
     */
    @Test
    public void testBigDecimalTiesOfSecondColumnSortedByThirdColumn() {
        final Table table = TableTools.newTable(
                TableTools.intCol("Group", 0, 0, 0, 0),
                TableTools.col("Amount", new BigDecimal("1.0"), new BigDecimal("1.00"), new BigDecimal("1.0"),
                        new BigDecimal("1.00")),
                TableTools.intCol("Tie", 4, 3, 2, 1));
        assertArrayEquals(new int[] {1, 2, 3, 4},
                ColumnVectors.ofInt(table.sort("Group", "Amount", "Tie"), "Tie").toArray());
        DataIndexer.getOrCreateDataIndex(table, "Group");
        assertArrayEquals(new int[] {1, 2, 3, 4},
                ColumnVectors.ofInt(table.sort("Group", "Amount", "Tie"), "Tie").toArray());
    }

    /**
     * BigDecimal 1.0 and 1.00 are ties in distinct data index groups, and two int arrays with the same contents are
     * ties whose data index has no table, since an aggregation cannot key on an array. A stable sort of either column
     * keeps their rows in row key order.
     */
    @Test
    public void testIndexGroupsOfTiedValuesKeepRowKeyOrder() {
        final int[] first = {1, 2};
        final int[] second = {1, 2};
        final BigDecimal[] amounts = new BigDecimal[12];
        final int[][] paths = new int[12][];
        for (int ii = 0; ii < 12; ++ii) {
            amounts[ii] = new BigDecimal(ii % 2 == 0 ? "1.0" : "1.00");
            paths[ii] = ii % 2 == 0 ? first : second;
        }
        final Table table = TableTools.newTable(TableTools.col("BD", amounts), TableTools.col("Path", paths),
                TableTools.intCol("N", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11));
        DataIndexer.getOrCreateDataIndex(table, "BD");
        DataIndexer.getOrCreateDataIndex(table, "Path");
        final int[] rowKeyOrder = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11};
        assertArrayEquals(rowKeyOrder, ColumnVectors.ofInt(table.sort("BD"), "N").toArray());
        assertArrayEquals(rowKeyOrder, ColumnVectors.ofInt(table.sort("Path"), "N").toArray());
    }

    /**
     * A sort by an int column and then a BigDecimal column uses the data index on the int column. An index on both
     * columns does not apply, since BigDecimal ordering does not respect equality.
     */
    @Test
    public void testFirstColumnIndexUsedWhenFullIndexHasColumnWithoutConsistentOrder() {
        final int[] keys = new int[12];
        final BigDecimal[] amounts = new BigDecimal[12];
        final int[] rowNumbers = new int[12];
        for (int ii = 0; ii < 12; ++ii) {
            keys[ii] = ii % 3;
            amounts[ii] = new BigDecimal(ii % 2 == 0 ? "1.0" : "1.00");
            rowNumbers[ii] = ii;
        }
        final Table table = TableTools.newTable(TableTools.intCol("A", keys), TableTools.col("BD", amounts),
                TableTools.intCol("N", rowNumbers));
        // an index on an identical table supplies the groups, whose row keys are the same in both tables
        final Table twin = TableTools.newTable(TableTools.intCol("A", keys));
        final CountingDataIndex firstColumnIndex = new CountingDataIndex(table.getColumnSource("A"), "A",
                DataIndexer.getOrCreateDataIndex(twin, "A"));
        DataIndexer.of(table.getRowSet()).addDataIndex(firstColumnIndex);
        DataIndexer.getOrCreateDataIndex(table, "A", "BD");

        assertArrayEquals(new int[] {0, 3, 6, 9, 1, 4, 7, 10, 2, 5, 8, 11},
                ColumnVectors.ofInt(table.sort("A", "BD"), "N").toArray());
        assertTrue(firstColumnIndex.tableRequests.get() > 0);
    }

    /** A data index on one column that delegates to another index and counts the requests for its table. */
    private static final class CountingDataIndex extends AbstractDataIndex {
        private final ColumnSource<?> indexedColumn;
        private final String keyColumnName;
        private final DataIndex delegate;
        final AtomicInteger tableRequests = new AtomicInteger();

        CountingDataIndex(@NotNull final ColumnSource<?> indexedColumn, @NotNull final String keyColumnName,
                @NotNull final DataIndex delegate) {
            this.indexedColumn = indexedColumn;
            this.keyColumnName = keyColumnName;
            this.delegate = delegate;
            manage(delegate);
        }

        @Override
        public boolean isValid() {
            return true;
        }

        @Override
        public @NotNull List<String> keyColumnNames() {
            return List.of(keyColumnName);
        }

        @Override
        public @NotNull Map<ColumnSource<?>, String> keyColumnNamesByIndexedColumn() {
            return Map.of(indexedColumn, keyColumnName);
        }

        @Override
        public boolean tableIsCached() {
            return delegate.tableIsCached();
        }

        @Override
        public @NotNull Table table(final DataIndexOptions options) {
            tableRequests.incrementAndGet();
            return delegate.table(options);
        }

        @Override
        public @NotNull RowKeyLookup rowKeyLookup(final DataIndexOptions options) {
            return delegate.rowKeyLookup(options);
        }

        @Override
        public boolean isRefreshing() {
            return delegate.isRefreshing();
        }
    }

    /**
     * A case-insensitive comparator makes "a" and "A" ties, so a sort by (S case-insensitively, N) orders those rows by
     * N. A data index on S does not apply to a comparator that does not respect equality, so the sort runs the
     * one-column-at-a-time path, whose runs of the first column are its ties under the comparator.
     */
    @Test
    public void testComparatorTiesOfFirstColumnSortedBySecondColumn() {
        final QueryTable table = (QueryTable) TableTools.newTable(
                TableTools.stringCol("S", "a", "A", "b", "B"),
                TableTools.intCol("N", 2, 1, 4, 3));
        final SortSpec caseInsensitive = ComparatorSortColumn.asc("S", String.CASE_INSENSITIVE_ORDER);
        final SortSpec byN = SortColumn.asc(ColumnName.of("N"));
        assertArrayEquals(new int[] {1, 2, 3, 4},
                ColumnVectors.ofInt(table.sort(caseInsensitive, byN), "N").toArray());
        DataIndexer.getOrCreateDataIndex(table, "S");
        assertArrayEquals(new int[] {1, 2, 3, 4},
                ColumnVectors.ofInt(table.sort(caseInsensitive, byN), "N").toArray());
    }

    /**
     * A case-insensitive comparator makes "a" and "A" ties, so a sort by (Group, Name case-insensitively, Tie) orders
     * those rows by Tie. With a data index on Group, the sort runs the one-column-at-a-time path, whose runs of the
     * second column are its ties under the comparator.
     */
    @Test
    public void testComparatorTiesOfSecondColumnSortedByThirdColumn() {
        final QueryTable table = (QueryTable) TableTools.newTable(
                TableTools.intCol("Group", 0, 0, 0, 0),
                TableTools.stringCol("Name", "a", "A", "B", "b"),
                TableTools.intCol("Tie", 2, 1, 4, 3));
        DataIndexer.getOrCreateDataIndex(table, "Group");
        final Table sorted = table.sort(
                SortColumn.asc(ColumnName.of("Group")),
                ComparatorSortColumn.asc("Name", String.CASE_INSENSITIVE_ORDER, false),
                SortColumn.asc(ColumnName.of("Tie")));
        assertArrayEquals(new int[] {1, 2, 3, 4}, ColumnVectors.ofInt(sorted, "Tie").toArray());
    }

    /**
     * Array columns sort with the comparator from the ComparatorRegistry, under which distinct arrays with the same
     * contents are ties, so a sort by (Group, Path, Tie) orders those rows by Tie. With a data index on Group, the sort
     * runs the one-column-at-a-time path, whose runs of the second column are its ties under that comparator.
     */
    @Test
    public void testArrayTiesOfSecondColumnSortedByThirdColumn() {
        final Table table = TableTools.newTable(
                TableTools.intCol("Group", 0, 0, 0, 0),
                TableTools.col("Path", new int[] {1, 2}, new int[] {1, 2}, new int[] {1, 2}, new int[] {1, 2}),
                TableTools.intCol("Tie", 4, 3, 2, 1));
        DataIndexer.getOrCreateDataIndex(table, "Group");
        assertArrayEquals(new int[] {1, 2, 3, 4},
                ColumnVectors.ofInt(table.sort("Group", "Path", "Tie"), "Tie").toArray());
    }

    private static SortSpec asc(final String column) {
        return SortColumn.asc(ColumnName.of(column));
    }

    private static SortSpec desc(final String column) {
        return SortColumn.desc(ColumnName.of(column));
    }

    private static final int TEST_MEGA_SORT_SIZE = 1000;
    private static final int TEST_SORT_CHUNK_SIZE = 100;

    /**
     * The columns of the mega sort test tables, for megaSortSize 1000 and sortChunkSize 100 and a few thousand rows:
     * Low (four int values with null) and Pair and Coin (two values each, a String and a byte) whose runs exceed
     * megaSortSize; Group (25 int values with null) whose runs span chunks; MostlyUnique, whose runs are mostly single
     * rows; later columns with nulls, NaN, signed zeros and case-insensitive String ties; and Id, the row position.
     */
    private static QueryTable makeMegaSortTestTable(final long seed, final int size, final boolean refreshing) {
        final Random random = new Random(seed);
        final int[] low = new int[size];
        final String[] pair = new String[size];
        final byte[] coin = new byte[size];
        final int[] group = new int[size];
        final int[] mostlyUnique = new int[size];
        final double[] doubles = new double[size];
        final String[] strings = new String[size];
        final long[] longs = new long[size];
        final long[] id = new long[size];
        final double[] doubleChoices = {Double.NaN, QueryConstants.NULL_DOUBLE, -0.0, 0.0, 1.5, -2.5, 3};
        final String[] stringChoices = {"a", "A", "b", "B", null, "c"};
        for (int ii = 0; ii < size; ++ii) {
            final int lowValue = random.nextInt(4);
            low[ii] = lowValue == 0 ? QueryConstants.NULL_INT : lowValue;
            pair[ii] = random.nextBoolean() ? "x" : "y";
            coin[ii] = (byte) random.nextInt(2);
            final int groupValue = random.nextInt(25);
            group[ii] = groupValue == 0 ? QueryConstants.NULL_INT : groupValue;
            mostlyUnique[ii] = random.nextInt(size * 4);
            doubles[ii] = doubleChoices[random.nextInt(doubleChoices.length)];
            strings[ii] = stringChoices[random.nextInt(stringChoices.length)];
            longs[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_LONG : random.nextInt(8);
            id[ii] = ii;
        }
        final ColumnHolder<?>[] columns = {
                TableTools.intCol("Low", low),
                TableTools.stringCol("Pair", pair),
                TableTools.byteCol("Coin", coin),
                TableTools.intCol("Group", group),
                TableTools.intCol("MostlyUnique", mostlyUnique),
                TableTools.doubleCol("D", doubles),
                TableTools.stringCol("S", strings),
                TableTools.longCol("L", longs),
                TableTools.longCol("Id", id)};
        return refreshing
                ? TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking(), columns)
                : (QueryTable) TableTools.newTable(columns);
    }

    /**
     * Sort the table with the default megaSortSize, then check that a mega sort (with megaSortSize and sortChunkSize
     * lowered) gives the same result. The sort of the refreshing copy of the table fails with a SizeException, as a
     * ticking sort cannot use the mapping of a mega sort.
     */
    private static void checkMegaSort(final QueryTable table, final QueryTable refreshing, final SortSpec... specs) {
        final Table expected = table.sort(specs);
        final int oldMegaSortSize = SortHelpers.megaSortSize;
        final int oldSortChunkSize = SortHelpers.sortChunkSize;
        try {
            SortHelpers.megaSortSize = TEST_MEGA_SORT_SIZE;
            SortHelpers.sortChunkSize = TEST_SORT_CHUNK_SIZE;
            TstUtils.assertTableEquals(expected, table.sort(specs));
            assertThrows(SizeException.class, () -> refreshing.sort(specs));
        } finally {
            SortHelpers.megaSortSize = oldMegaSortSize;
            SortHelpers.sortChunkSize = oldSortChunkSize;
        }
    }

    /**
     * A mega sort by two columns matches the sort of fewer than megaSortSize rows, for ascending and descending ints,
     * longs, doubles and Strings with nulls, NaN and signed zeros, first columns whose runs span sort chunks, and a
     * first column whose runs are mostly single rows.
     */
    @Test
    public void testMegaSortTwoColumns() {
        final int size = 5000;
        final QueryTable table = makeMegaSortTestTable(1001, size, false);
        final QueryTable refreshing = makeMegaSortTestTable(1001, size, true);
        checkMegaSort(table, refreshing, asc("Group"), desc("D"));
        checkMegaSort(table, refreshing, desc("Group"), asc("S"));
        checkMegaSort(table, refreshing, asc("D"), asc("L"));
        checkMegaSort(table, refreshing, desc("D"), desc("Id"));
        checkMegaSort(table, refreshing, desc("S"), asc("Group"));
        checkMegaSort(table, refreshing, asc("L"), desc("S"));
        checkMegaSort(table, refreshing, asc("MostlyUnique"), asc("L"));
    }

    /**
     * A mega sort by three columns matches the sort of fewer than megaSortSize rows, with mixed ascending and
     * descending columns.
     */
    @Test
    public void testMegaSortThreeColumns() {
        final int size = 4000;
        final QueryTable table = makeMegaSortTestTable(1002, size, false);
        final QueryTable refreshing = makeMegaSortTestTable(1002, size, true);
        checkMegaSort(table, refreshing, asc("Group"), desc("S"), asc("D"));
        checkMegaSort(table, refreshing, desc("D"), asc("L"), desc("S"));
        checkMegaSort(table, refreshing, asc("S"), desc("D"), desc("L"));
        checkMegaSort(table, refreshing, desc("L"), asc("Group"), asc("MostlyUnique"));
    }

    /**
     * A first column with runs of at least megaSortSize rows sorts each such run by the remaining columns with another
     * mega sort, which in turn handles its own runs of at least megaSortSize rows the same way.
     */
    @Test
    public void testMegaSortRunsOfMegaSortSize() {
        final int size = 6000;
        final QueryTable table = makeMegaSortTestTable(1003, size, false);
        final QueryTable refreshing = makeMegaSortTestTable(1003, size, true);
        checkMegaSort(table, refreshing, asc("Low"), desc("D"));
        checkMegaSort(table, refreshing, desc("Low"), asc("S"), asc("L"));
        checkMegaSort(table, refreshing, asc("Pair"), desc("Coin"), asc("D"));
        checkMegaSort(table, refreshing, desc("Pair"), asc("Coin"), desc("Low"), asc("S"));
        checkMegaSort(table, refreshing, asc("Coin"), asc("Group"), desc("MostlyUnique"));
    }

    /**
     * Rows that tie on every column of a mega sort remain in row key order, both within runs of fewer than megaSortSize
     * rows and within the runs of larger ones.
     */
    @Test
    public void testMegaSortTiesKeepRowKeyOrder() {
        final int size = 5000;
        final QueryTable table = makeMegaSortTestTable(1004, size, false);
        final QueryTable refreshing = makeMegaSortTestTable(1004, size, true);
        checkMegaSort(table, refreshing, asc("Low"), desc("Pair"));
        checkMegaSort(table, refreshing, asc("Pair"), asc("Coin"));
        checkMegaSort(table, refreshing, desc("Coin"), asc("Low"), asc("Pair"));
        checkMegaSort(table, refreshing, asc("Group"), desc("Coin"));
    }

    /**
     * A mega sort ignores a data index on its first column, and a full data index on its sort columns is still used.
     */
    @Test
    public void testMegaSortWithDataIndex() {
        final int size = 5000;
        final QueryTable firstColumnIndexed = makeMegaSortTestTable(1005, size, false);
        final QueryTable refreshing = makeMegaSortTestTable(1005, size, true);
        DataIndexer.getOrCreateDataIndex(firstColumnIndexed, "Group");
        DataIndexer.getOrCreateDataIndex(refreshing, "Group");
        checkMegaSort(firstColumnIndexed, refreshing, asc("Group"), desc("D"), asc("S"));

        final QueryTable fullyIndexed = makeMegaSortTestTable(1005, size, false);
        DataIndexer.getOrCreateDataIndex(fullyIndexed, "Low", "Coin");
        final Table expected = fullyIndexed.sort(asc("Low"), desc("Coin"));
        final int oldMegaSortSize = SortHelpers.megaSortSize;
        final int oldSortChunkSize = SortHelpers.sortChunkSize;
        try {
            SortHelpers.megaSortSize = TEST_MEGA_SORT_SIZE;
            SortHelpers.sortChunkSize = TEST_SORT_CHUNK_SIZE;
            TstUtils.assertTableEquals(expected, fullyIndexed.sort(asc("Low"), desc("Coin")));
        } finally {
            SortHelpers.megaSortSize = oldMegaSortSize;
            SortHelpers.sortChunkSize = oldSortChunkSize;
        }
    }

    /**
     * A mega sort with comparator columns matches the sort of fewer than megaSortSize rows: a comparator on the first
     * column, ascending and descending, whose runs exceed megaSortSize and span sort chunks; on the column sorted
     * within first-column runs of megaSortSize rows or more; and on a later column of the run batches. Values that tie
     * under the comparator, such as "a" and "A", remain in row key order.
     */
    @Test
    public void testMegaSortWithComparators() {
        final int size = 5000;
        final QueryTable table = makeMegaSortTestTable(1006, size, false);
        final QueryTable refreshing = makeMegaSortTestTable(1006, size, true);
        final Comparator<String> caseInsensitive = Comparator.nullsLast(String.CASE_INSENSITIVE_ORDER);
        checkMegaSort(table, refreshing, ComparatorSortColumn.asc("S", caseInsensitive), asc("Group"));
        checkMegaSort(table, refreshing, ComparatorSortColumn.desc("S", caseInsensitive), desc("L"));
        checkMegaSort(table, refreshing, asc("Pair"), ComparatorSortColumn.desc("S", caseInsensitive), asc("L"));
        checkMegaSort(table, refreshing, asc("Group"), desc("D"), ComparatorSortColumn.asc("S", caseInsensitive));
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
