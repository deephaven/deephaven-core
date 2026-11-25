//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import com.google.common.collect.Sets;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortSpec;
import io.deephaven.api.SortColumn;
import io.deephaven.benchmarking.generator.ColumnGenerator;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.rowset.RowSet;
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
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.test.types.SerialTest;
import io.deephaven.util.QueryConstants;
import io.deephaven.benchmarking.BenchmarkTable;
import io.deephaven.benchmarking.BenchmarkTableBuilder;
import io.deephaven.benchmarking.BenchmarkTools;
import org.apache.commons.lang3.mutable.MutableObject;
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

    /**
     * The columns of the index test tables: an int, String and double first column for an index (with nulls, and NaN
     * for the double), a first column whose index has mostly single-row groups, and later columns with nulls, NaN,
     * signed zeros and case-insensitive String ties.
     */
    private static Table makeIndexTestTable(final long seed, final int size, final boolean refreshing) {
        final Random random = new Random(seed);
        final int[] groupInt = new int[size];
        final String[] groupString = new String[size];
        final double[] groupDouble = new double[size];
        final int[] mostlyUnique = new int[size];
        final double[] doubles = new double[size];
        final String[] strings = new String[size];
        final long[] longs = new long[size];
        final double[] doubleChoices = {Double.NaN, QueryConstants.NULL_DOUBLE, -0.0, 0.0, 1.5, -2.5, 3};
        final String[] stringChoices = {"a", "A", "b", "B", null, "c"};
        for (int ii = 0; ii < size; ++ii) {
            final int group = random.nextInt(25);
            groupInt[ii] = group == 0 ? QueryConstants.NULL_INT : group;
            groupString[ii] = group == 0 ? null : "g" + group;
            groupDouble[ii] = group == 0 ? QueryConstants.NULL_DOUBLE : group == 1 ? Double.NaN : group * 0.5;
            mostlyUnique[ii] = random.nextInt(size * 4);
            doubles[ii] = doubleChoices[random.nextInt(doubleChoices.length)];
            strings[ii] = stringChoices[random.nextInt(stringChoices.length)];
            longs[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_LONG : random.nextInt(8);
        }
        final ColumnHolder<?>[] columns = {
                TableTools.intCol("GroupInt", groupInt),
                TableTools.stringCol("GroupString", groupString),
                TableTools.doubleCol("GroupDouble", groupDouble),
                TableTools.intCol("MostlyUnique", mostlyUnique),
                TableTools.doubleCol("D", doubles),
                TableTools.stringCol("S", strings),
                TableTools.longCol("L", longs)};
        return refreshing
                ? TstUtils.testRefreshingTable(RowSetFactory.flat(size).toTracking(), columns)
                : TableTools.newTable(columns);
    }

    /**
     * Sort with the generated kernel and no index, then check that the other paths give the same result: the
     * one-column-at-a-time path with and without the index on the first column, the generated kernel with that index
     * present but its groups below the minimum size, and the index groups sorted by the remaining columns.
     */
    private static void checkFirstColumnIndexSort(final Table unindexed, final Table indexed, final SortSpec... specs) {
        final boolean oldKernels = QueryTable.USE_INDIRECT_SORT_KERNELS;
        final int oldMinimumGroupSize = SortHelpers.minimumIndexGroupSize;
        try {
            QueryTable.USE_INDIRECT_SORT_KERNELS = true;
            final Table expected = ((QueryTable) unindexed).sort(specs);
            SortHelpers.minimumIndexGroupSize = 1;
            TstUtils.assertTableEquals(expected, ((QueryTable) indexed).sort(specs));
            SortHelpers.minimumIndexGroupSize = Integer.MAX_VALUE;
            TstUtils.assertTableEquals(expected, ((QueryTable) indexed).sort(specs));
            QueryTable.USE_INDIRECT_SORT_KERNELS = false;
            TstUtils.assertTableEquals(expected, ((QueryTable) unindexed).sort(specs));
            TstUtils.assertTableEquals(expected, ((QueryTable) indexed).sort(specs));
        } finally {
            QueryTable.USE_INDIRECT_SORT_KERNELS = oldKernels;
            SortHelpers.minimumIndexGroupSize = oldMinimumGroupSize;
        }
    }

    private static SortSpec asc(final String column) {
        return SortColumn.asc(ColumnName.of(column));
    }

    private static SortSpec desc(final String column) {
        return SortColumn.desc(ColumnName.of(column));
    }

    /**
     * A sort whose first column has a data index sorts each index group by the remaining columns, giving the same
     * result as the generated kernel without the index and the one-column-at-a-time path: for two and more columns,
     * ascending and descending, nulls, NaN and signed zeros, comparators on later columns, and indexes whose groups are
     * mostly single rows. The groups are sorted serially and in parallel tasks.
     */
    @Test
    public void testFirstColumnIndexSortMatchesOtherPaths() {
        final int size = 5000;
        final Table unindexed = makeIndexTestTable(31337, size, false);
        final Table indexed = makeIndexTestTable(31337, size, false);
        for (final String first : new String[] {"GroupInt", "GroupString", "GroupDouble", "MostlyUnique"}) {
            DataIndexer.getOrCreateDataIndex(indexed, first);
        }
        final Comparator<String> caseInsensitive = Comparator.nullsFirst(String.CASE_INSENSITIVE_ORDER);

        final long oldMinimumParallel = QueryTable.MINIMUM_PARALLEL_SORT_ROWS;
        final long oldSegmentSize = SortHelpers.parallelSortSegmentSize;
        try {
            for (final boolean parallel : new boolean[] {false, true}) {
                QueryTable.MINIMUM_PARALLEL_SORT_ROWS = parallel ? 1 : Long.MAX_VALUE;
                SortHelpers.parallelSortSegmentSize = 256;
                for (final String first : new String[] {"GroupInt", "GroupString", "GroupDouble", "MostlyUnique"}) {
                    checkFirstColumnIndexSort(unindexed, indexed, asc(first), asc("D"));
                    checkFirstColumnIndexSort(unindexed, indexed, desc(first), desc("S"));
                    checkFirstColumnIndexSort(unindexed, indexed, asc(first), desc("S"), asc("D"), desc("L"));
                    checkFirstColumnIndexSort(unindexed, indexed, desc(first), asc("L"), desc("D"));
                    checkFirstColumnIndexSort(unindexed, indexed, asc(first),
                            ComparatorSortColumn.asc("S", caseInsensitive), asc("L"));
                    checkFirstColumnIndexSort(unindexed, indexed, desc(first),
                            ComparatorSortColumn.desc("S", caseInsensitive));
                }
            }
        } finally {
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = oldMinimumParallel;
            SortHelpers.parallelSortSegmentSize = oldSegmentSize;
        }
    }

    /**
     * The initial sort of a refreshing table with a data index on its first column sorts the index groups, and the sort
     * then stays correct as rows are added.
     */
    @Test
    public void testRefreshingFirstColumnIndexSort() {
        final int size = 2000;
        final QueryTable source = (QueryTable) makeIndexTestTable(4242, size, true);
        DataIndexer.getOrCreateDataIndex(source, "GroupInt");
        final int oldMinimumGroupSize = SortHelpers.minimumIndexGroupSize;
        final Table sorted;
        try {
            SortHelpers.minimumIndexGroupSize = 1;
            sorted = source.sort("GroupInt", "S", "D", "L");
        } finally {
            SortHelpers.minimumIndexGroupSize = oldMinimumGroupSize;
        }
        TstUtils.assertTableEquals(makeIndexTestTable(4242, size, false).sort("GroupInt", "S", "D", "L"), sorted);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(size, size + 2);
            TstUtils.addToTable(source, added, TableTools.intCol("GroupInt", 3, 4, 3),
                    TableTools.stringCol("GroupString", "g3", "g4", "g3"),
                    TableTools.doubleCol("GroupDouble", 1.5, 2.0, 1.5),
                    TableTools.intCol("MostlyUnique", 1, 2, 3),
                    TableTools.doubleCol("D", 0.0, Double.NaN, -0.0),
                    TableTools.stringCol("S", "b", null, "B"),
                    TableTools.longCol("L", 1, 2, 3));
            source.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        });
        TstUtils.assertTableEquals(source.select().sort("GroupInt", "S", "D", "L"), sorted);
    }

    /**
     * A refreshing sort with a data index on its first column, created during an update cycle after its source has
     * changed but before the source notifies, sorts the index groups of the source's previous values, and matches a
     * fresh sort once the cycle completes.
     */
    @Test
    public void testRefreshingFirstColumnIndexSortCreatedDuringUpdate() {
        final int size = 2000;
        final QueryTable source = (QueryTable) makeIndexTestTable(4243, size, true);
        DataIndexer.getOrCreateDataIndex(source, "GroupInt");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final MutableObject<Table> sorted = new MutableObject<>();
        final int oldMinimumGroupSize = SortHelpers.minimumIndexGroupSize;
        try {
            SortHelpers.minimumIndexGroupSize = 1;
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet added = RowSetFactory.fromRange(size, size + 2);
                TstUtils.addToTable(source, added, TableTools.intCol("GroupInt", 3, 4, 3),
                        TableTools.stringCol("GroupString", "g3", "g4", "g3"),
                        TableTools.doubleCol("GroupDouble", 1.5, 2.0, 1.5),
                        TableTools.intCol("MostlyUnique", 1, 2, 3),
                        TableTools.doubleCol("D", 0.0, Double.NaN, -0.0),
                        TableTools.stringCol("S", "b", null, "B"),
                        TableTools.longCol("L", 1, 2, 3));
                final RowSet modified = RowSetFactory.fromKeys(0, 1);
                TstUtils.addToTable(source, modified, TableTools.intCol("GroupInt", 4, 3),
                        TableTools.stringCol("GroupString", "g4", "g3"),
                        TableTools.doubleCol("GroupDouble", 2.0, 1.5),
                        TableTools.intCol("MostlyUnique", 4, 5),
                        TableTools.doubleCol("D", 1.0, 2.0),
                        TableTools.stringCol("S", "a", "c"),
                        TableTools.longCol("L", 4, 5));
                // the source has not notified yet, so the sort initializes from its previous values
                sorted.setValue(source.sort("GroupInt", "S", "D", "L"));
                source.notifyListeners(added, RowSetFactory.empty(), modified);
                updateGraph.markSourcesRefreshedForUnitTests();
            }, false);
        } finally {
            SortHelpers.minimumIndexGroupSize = oldMinimumGroupSize;
        }
        TstUtils.assertTableEquals(source.select().sort("GroupInt", "S", "D", "L"), sorted.getValue());
    }

    /**
     * Sorting the groups of a data index on the first column reads none of the first column's values and each value of
     * the remaining columns once; the generated kernel without the index reads every column.
     */
    @Test
    public void testFirstColumnIndexSortReadsOnlyRemainingColumns() {
        final int size = 3000;
        final Random random = new Random(1618);
        final int[] groups = new int[size];
        final int[] second = new int[size];
        final int[] third = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            groups[ii] = random.nextInt(10);
            second[ii] = random.nextInt(5);
            third[ii] = random.nextInt(1000);
        }
        final AtomicInteger firstReads = new AtomicInteger();
        final AtomicInteger remainingReads = new AtomicInteger();
        QueryScope.addParam("firstReads", firstReads);
        QueryScope.addParam("remainingReads", remainingReads);
        final Table table = TableTools.newTable(TableTools.intCol("RawA", groups), TableTools.intCol("RawB", second),
                TableTools.intCol("RawC", third)).updateView(
                        "A = RawA + 0 * firstReads.getAndIncrement()",
                        "B = RawB + 0 * remainingReads.getAndIncrement()",
                        "C = RawC + 0 * remainingReads.getAndIncrement()");
        // computing the index reads A, so it is computed before the reads are counted
        DataIndexer.getOrCreateDataIndex(table, "A").table();

        final boolean oldKernels = QueryTable.USE_INDIRECT_SORT_KERNELS;
        final int oldMinimumGroupSize = SortHelpers.minimumIndexGroupSize;
        try {
            QueryTable.USE_INDIRECT_SORT_KERNELS = true;
            SortHelpers.minimumIndexGroupSize = 1;
            firstReads.set(0);
            remainingReads.set(0);
            final Table indexGroupsSorted = table.sort("A", "B", "C");
            assertEquals("first column reads when sorting the index groups", 0, firstReads.get());
            assertEquals("remaining column reads when sorting the index groups", 2 * size, remainingReads.get());

            SortHelpers.minimumIndexGroupSize = Integer.MAX_VALUE;
            firstReads.set(0);
            remainingReads.set(0);
            final Table kernelSorted = table.sort("A", "B", "C");
            assertEquals("first column reads by the generated kernel", size, firstReads.get());
            assertEquals("remaining column reads by the generated kernel", 2 * size, remainingReads.get());
            TstUtils.assertTableEquals(kernelSorted, indexGroupsSorted);
        } finally {
            QueryTable.USE_INDIRECT_SORT_KERNELS = oldKernels;
            SortHelpers.minimumIndexGroupSize = oldMinimumGroupSize;
        }
    }

    /**
     * A data index on all the sort columns that is too large for the sort to use leaves the sort to the generated
     * kernel, which reads every sort column. Without the generated kernels, the one-column-at-a-time path reads the
     * second column only for ties of the first, and here the first column has none.
     */
    @Test
    public void testTooLargeFullIndexUsesGeneratedKernel() {
        final int size = 1000;
        final Random random = new Random(2718);
        final int[] distinct = new int[size];
        final int[] second = new int[size];
        for (int ii = 0; ii < size; ++ii) {
            distinct[ii] = ii;
            second[ii] = random.nextInt(10);
        }
        for (int ii = size - 1; ii > 0; --ii) {
            final int jj = random.nextInt(ii + 1);
            final int swap = distinct[ii];
            distinct[ii] = distinct[jj];
            distinct[jj] = swap;
        }
        final AtomicInteger secondReads = new AtomicInteger();
        QueryScope.addParam("secondReads", secondReads);
        final Table table = TableTools.newTable(TableTools.intCol("A", distinct), TableTools.intCol("RawB", second))
                .updateView("B = RawB + 0 * secondReads.getAndIncrement()");
        // computing the index reads B, so it is computed before the reads are counted
        DataIndexer.getOrCreateDataIndex(table, "A", "B").table();

        final boolean oldKernels = QueryTable.USE_INDIRECT_SORT_KERNELS;
        try {
            QueryTable.USE_INDIRECT_SORT_KERNELS = false;
            secondReads.set(0);
            final Table columnAtATime = table.sort("A", "B");
            assertEquals("second column reads with the generated kernels disabled", 0, secondReads.get());

            QueryTable.USE_INDIRECT_SORT_KERNELS = true;
            secondReads.set(0);
            final Table generated = table.sort("A", "B");
            assertEquals("second column reads by the generated kernel", size, secondReads.get());
            TstUtils.assertTableEquals(columnAtATime, generated);
        } finally {
            QueryTable.USE_INDIRECT_SORT_KERNELS = oldKernels;
        }
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
