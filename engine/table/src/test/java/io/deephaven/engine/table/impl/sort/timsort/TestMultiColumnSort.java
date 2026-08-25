//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.ComparatorSortColumn;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortHelpers;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.impl.sort.timsort.indirect.IndirectTimsortKernelFactory;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import junit.framework.TestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.util.TableTools.byteCol;
import static io.deephaven.engine.util.TableTools.charCol;
import static io.deephaven.engine.util.TableTools.col;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.shortCol;
import static io.deephaven.engine.util.TableTools.stringCol;

/**
 * Verifies that {@link QueryTable#sort} produces identical results whether the multi-column timsort kernel or the
 * one-column-at-a-time pipeline is used. The Sentinel column makes the comparison sensitive to any difference in the
 * resulting row permutation, so this also verifies that both paths produce the same stable sort.
 */
public class TestMultiColumnSort {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private boolean oldMemoize;

    @Before
    public void disableMemoization() {
        // every check here sorts the same table with the same columns twice, once per arm of an A/B comparison;
        // with memoization on, the second sort would return the first result and the comparison would be vacuous
        oldMemoize = QueryTable.setMemoizeResults(false);
    }

    @After
    public void restoreMemoization() {
        QueryTable.setMemoizeResults(oldMemoize);
    }

    private static final String[] FIRST_COLUMNS =
            {"CharA", "ByteA", "ShortA", "IntA", "LongA", "FloatA", "DoubleA", "ObjA"};
    private static final String[] SECOND_COLUMNS =
            {"CharB", "ByteB", "ShortB", "IntB", "LongB", "FloatB", "DoubleB", "ObjB"};

    private static Table makeTable(final Random random, final int size) {
        final char[] charA = new char[size];
        final char[] charB = new char[size];
        final byte[] byteA = new byte[size];
        final byte[] byteB = new byte[size];
        final short[] shortA = new short[size];
        final short[] shortB = new short[size];
        final int[] intA = new int[size];
        final int[] intB = new int[size];
        final long[] longA = new long[size];
        final long[] longB = new long[size];
        final float[] floatA = new float[size];
        final float[] floatB = new float[size];
        final double[] doubleA = new double[size];
        final double[] doubleB = new double[size];
        final String[] objA = new String[size];
        final String[] objB = new String[size];
        final long[] sentinel = new long[size];

        final float[] floatSpecials =
                {QueryConstants.NULL_FLOAT, Float.NaN, Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY, -0.0f, 0.0f};
        final double[] doubleSpecials =
                {QueryConstants.NULL_DOUBLE, Double.NaN, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, -0.0,
                        0.0};

        for (int ii = 0; ii < size; ++ii) {
            // small domains so both columns have plenty of duplicates (and thus plenty of ties to break)
            charA[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_CHAR : (char) ('A' + random.nextInt(5));
            charB[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_CHAR : (char) ('a' + random.nextInt(7));
            byteA[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_BYTE : (byte) (random.nextInt(7) - 3);
            byteB[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_BYTE : (byte) (random.nextInt(5) - 2);
            shortA[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_SHORT : (short) (random.nextInt(9) - 4);
            shortB[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_SHORT : (short) (random.nextInt(5) - 2);
            intA[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_INT : random.nextInt(7) - 3;
            intB[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_INT : random.nextInt(11) - 5;
            longA[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_LONG : random.nextInt(7) - 3;
            longB[ii] = random.nextInt(10) == 0 ? QueryConstants.NULL_LONG : random.nextInt(9) - 4;
            floatA[ii] = random.nextInt(4) == 0 ? floatSpecials[random.nextInt(floatSpecials.length)]
                    : (float) (random.nextInt(7) - 3);
            floatB[ii] = random.nextInt(4) == 0 ? floatSpecials[random.nextInt(floatSpecials.length)]
                    : (float) (random.nextInt(5) - 2);
            doubleA[ii] = random.nextInt(4) == 0 ? doubleSpecials[random.nextInt(doubleSpecials.length)]
                    : (double) (random.nextInt(7) - 3);
            doubleB[ii] = random.nextInt(4) == 0 ? doubleSpecials[random.nextInt(doubleSpecials.length)]
                    : (double) (random.nextInt(5) - 2);
            objA[ii] = random.nextInt(10) == 0 ? null : "S" + random.nextInt(5);
            objB[ii] = random.nextInt(10) == 0 ? null : "T" + random.nextInt(7);
            sentinel[ii] = ii;
        }

        return TableTools.newTable(
                charCol("CharA", charA), charCol("CharB", charB),
                byteCol("ByteA", byteA), byteCol("ByteB", byteB),
                shortCol("ShortA", shortA), shortCol("ShortB", shortB),
                intCol("IntA", intA), intCol("IntB", intB),
                longCol("LongA", longA), longCol("LongB", longB),
                floatCol("FloatA", floatA), floatCol("FloatB", floatB),
                doubleCol("DoubleA", doubleA), doubleCol("DoubleB", doubleB),
                col("ObjA", objA), col("ObjB", objB),
                longCol("Sentinel", sentinel));
    }

    private interface SortInvoker {
        Table sort(Table table);
    }

    private static void checkSame(final Table table, final SortInvoker invoker) {
        final boolean oldFlag = QueryTable.USE_INDIRECT_SORT_KERNELS;
        final Table expected;
        final Table actual;
        try {
            QueryTable.USE_INDIRECT_SORT_KERNELS = false;
            expected = invoker.sort(table);
            QueryTable.USE_INDIRECT_SORT_KERNELS = true;
            actual = invoker.sort(table);
        } finally {
            QueryTable.USE_INDIRECT_SORT_KERNELS = oldFlag;
        }
        assertTableEquals(expected, actual);
    }

    @Test
    public void testAllTypePairs() {
        for (final int size : new int[] {23, 1000, 10000}) {
            for (int seed = 0; seed < 2; ++seed) {
                final Table table = makeTable(new Random(seed), size);
                for (final String first : FIRST_COLUMNS) {
                    for (final String second : SECOND_COLUMNS) {
                        checkSame(table, t -> t.sort(first, second));
                    }
                }
            }
        }
    }

    @Test
    public void testEdgeSizes() {
        for (final int size : new int[] {0, 1, 2}) {
            final Table table = makeTable(new Random(0), size);
            checkSame(table, t -> t.sort("IntA", "ObjB"));
            checkSame(table, t -> t.sort("ObjA", "LongB"));
        }
    }

    @Test
    public void testDispatch() {
        // every multi-column shape is compiled on demand, so the type-pair test exercises real kernels
        for (final ChunkType first : new ChunkType[] {ChunkType.Char, ChunkType.Byte, ChunkType.Short, ChunkType.Int,
                ChunkType.Long, ChunkType.Float, ChunkType.Double, ChunkType.Object}) {
            for (final ChunkType second : new ChunkType[] {ChunkType.Int, ChunkType.Object}) {
                try (final MultiColumnSortKernel<Any> kernel = IndirectTimsortKernelFactory.makeContext(
                        new ChunkType[] {first, second},
                        new SortingOrder[] {SortingOrder.Ascending, SortingOrder.Ascending}, new Comparator[2], 16)) {
                    TestCase.assertNotNull(kernel);
                }
            }
        }
        // single-column Object sorts use indirect kernels in either direction, with or without a comparator;
        // primitives use the direct kernels
        for (final SortingOrder order : SortingOrder.values()) {
            try (final MultiColumnSortKernel<Any> kernel = IndirectTimsortKernelFactory.makeContext(
                    new ChunkType[] {ChunkType.Object}, new SortingOrder[] {order}, new Comparator[1], 16)) {
                TestCase.assertNotNull(kernel);
            }
            TestCase.assertNull(IndirectTimsortKernelFactory.makeContext(
                    new ChunkType[] {ChunkType.Int}, new SortingOrder[] {order}, new Comparator[1], 16));
            try (final MultiColumnSortKernel<Any> kernel = IndirectTimsortKernelFactory.makeContext(
                    new ChunkType[] {ChunkType.Object}, new SortingOrder[] {order},
                    new Comparator[] {Comparator.naturalOrder()}, 16)) {
                TestCase.assertNotNull(kernel);
            }
        }
        // descending, three-column, and comparator shapes compile on demand
        try (final MultiColumnSortKernel<Any> kernel = IndirectTimsortKernelFactory.makeContext(
                new ChunkType[] {ChunkType.Int, ChunkType.Long},
                new SortingOrder[] {SortingOrder.Ascending, SortingOrder.Descending}, new Comparator[2], 16)) {
            TestCase.assertNotNull(kernel);
        }
        try (final MultiColumnSortKernel<Any> kernel = IndirectTimsortKernelFactory.makeContext(
                new ChunkType[] {ChunkType.Int, ChunkType.Long, ChunkType.Object},
                new SortingOrder[] {SortingOrder.Ascending, SortingOrder.Ascending, SortingOrder.Ascending},
                new Comparator[3], 16)) {
            TestCase.assertNotNull(kernel);
        }
        try (final MultiColumnSortKernel<Any> kernel = IndirectTimsortKernelFactory.makeContext(
                new ChunkType[] {ChunkType.Object, ChunkType.Int},
                new SortingOrder[] {SortingOrder.Descending, SortingOrder.Ascending},
                new Comparator[] {Comparator.nullsFirst(Comparator.naturalOrder()), null}, 16)) {
            TestCase.assertNotNull(kernel);
        }
        // boolean chunks have no kernel; the caller falls back
        TestCase.assertNull(IndirectTimsortKernelFactory.makeContext(
                new ChunkType[] {ChunkType.Boolean, ChunkType.Int},
                new SortingOrder[] {SortingOrder.Ascending, SortingOrder.Ascending}, new Comparator[2], 16));
    }

    @Test
    public void testComparators() {
        final Table table = makeTable(new Random(271828), 10000);
        // an equality-respecting comparator produces the same result on the kernel and pipeline paths
        final Comparator<String> nullsFirstNatural = Comparator.nullsFirst(Comparator.naturalOrder());
        checkSame(table, t -> ((QueryTable) t.coalesce()).sort(
                ComparatorSortColumn.asc("ObjA", nullsFirstNatural, true),
                SortColumn.asc(ColumnName.of("IntB"))));
        checkSame(table, t -> ((QueryTable) t.coalesce()).sort(
                SortColumn.asc(ColumnName.of("IntA")),
                ComparatorSortColumn.desc("ObjB", nullsFirstNatural, true)));
        checkSame(table, t -> ((QueryTable) t.coalesce()).sort(
                ComparatorSortColumn.desc("ObjA", nullsFirstNatural, true),
                SortColumn.desc(ColumnName.of("LongB")),
                ComparatorSortColumn.asc("ObjB", nullsFirstNatural, true)));
    }

    @Test
    public void testSingleColumn() {
        for (final int size : new int[] {23, 10000}) {
            final Table table = makeTable(new Random(42), size);
            for (final String column : FIRST_COLUMNS) {
                checkSame(table, t -> t.sort(column));
                checkSame(table, t -> t.sortDescending(column));
            }
        }
    }

    private static void checkParallelSame(final Table table, final SortInvoker invoker) {
        final long oldMinimum = QueryTable.MINIMUM_PARALLEL_SORT_ROWS;
        final long oldSegment = SortHelpers.parallelSortSegmentSize;
        final Table serial;
        final Table parallel;
        try {
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = 0;
            serial = invoker.sort(table);
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = 1;
            SortHelpers.parallelSortSegmentSize = 1;
            parallel = invoker.sort(table);
        } finally {
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = oldMinimum;
            SortHelpers.parallelSortSegmentSize = oldSegment;
        }
        assertTableEquals(serial, parallel);
    }

    @Test
    public void testParallelSort() {
        // the comparisons below are only meaningful if this environment can actually parallelize
        TestCase.assertTrue(ExecutionContext.getContext().getOperationInitializer().canParallelize());

        // a minimum size of one splits into parallelismFactor segments, exercising the full merge tree; the small
        // sizes stress the single-element-segment and odd-segment-count edges of the tree
        for (final int size : new int[] {2, 3, 23, 1000, 10000, 100_000}) {
            final Table table = makeTable(new Random(8675309 + size), size);
            checkParallelSame(table, t -> t.sort("ObjA"));
            // single-column sorts of every stripe run the direct kernels' segment sorts and merges
            checkParallelSame(table, t -> t.sort("IntA"));
            checkParallelSame(table, t -> t.sortDescending("IntA"));
            checkParallelSame(table, t -> t.sort("DoubleA"));
            checkParallelSame(table, t -> t.sort("CharA"));
            checkParallelSame(table, t -> ((QueryTable) t.coalesce()).sort(
                    ComparatorSortColumn.asc("ObjA", Comparator.nullsFirst(Comparator.naturalOrder()), true)));
            checkParallelSame(table, t -> t.sort("ObjA", "IntB"));
            checkParallelSame(table, t -> t.sort("IntA", "LongB", "ObjB"));
            checkParallelSame(table, t -> t.sortDescending("ObjA", "DoubleB"));
            checkParallelSame(table, t -> ((QueryTable) t.coalesce()).sort(
                    ComparatorSortColumn.asc("ObjA", Comparator.nullsFirst(Comparator.naturalOrder()), true),
                    SortColumn.asc(ColumnName.of("IntB"))));
        }

        // exactly two segments: a 10,000 row table with 5,000 row segments
        final long oldMinimum = QueryTable.MINIMUM_PARALLEL_SORT_ROWS;
        final long oldSegment = SortHelpers.parallelSortSegmentSize;
        try {
            final Table table = makeTable(new Random(31415), 10000);
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = 0;
            final Table serial = table.sort("ObjA", "IntB");
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = 1;
            SortHelpers.parallelSortSegmentSize = 5000;
            final Table twoSegments = table.sort("ObjA", "IntB");
            assertTableEquals(serial, twoSegments);
        } finally {
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = oldMinimum;
            SortHelpers.parallelSortSegmentSize = oldSegment;
        }

        // the one-column-at-a-time pipeline fills values through the same helper
        final boolean oldFlag = QueryTable.USE_INDIRECT_SORT_KERNELS;
        try {
            QueryTable.USE_INDIRECT_SORT_KERNELS = false;
            final Table table = makeTable(new Random(8675309), 10000);
            checkParallelSame(table, t -> t.sort("ObjA"));
            checkParallelSame(table, t -> t.sort("ObjA", "IntB"));
            checkParallelSame(table, t -> t.sort("IntA", "LongB", "ObjB"));
        } finally {
            QueryTable.USE_INDIRECT_SORT_KERNELS = oldFlag;
        }

        // the parallelSort switch forces the serial path even when the size thresholds would parallelize
        final boolean oldParallelSort = QueryTable.PARALLEL_SORT;
        final long oldMinimumSize = QueryTable.MINIMUM_PARALLEL_SORT_ROWS;
        final long oldSegmentSize = SortHelpers.parallelSortSegmentSize;
        try {
            final Table table = makeTable(new Random(8675309), 10000);
            final Table expected = table.sort("ObjA", "IntB");
            QueryTable.PARALLEL_SORT = false;
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = 1;
            SortHelpers.parallelSortSegmentSize = 1;
            assertTableEquals(expected, table.sort("ObjA", "IntB"));
        } finally {
            QueryTable.PARALLEL_SORT = oldParallelSort;
            QueryTable.MINIMUM_PARALLEL_SORT_ROWS = oldMinimumSize;
            SortHelpers.parallelSortSegmentSize = oldSegmentSize;
        }
    }

    @Test
    public void testCompiledKernelPaths() {
        final Table table = makeTable(new Random(31337), 10000);
        // these shapes have no pregenerated kernel and exercise the on-demand compiled kernels
        checkSame(table, t -> t.sortDescending("IntA", "LongB"));
        checkSame(table, t -> t.sort("IntA", "LongB", "ObjB"));
        checkSame(table, t -> t.sort("ObjA", "IntB", "DoubleB"));
        checkSame(table, t -> t.sortDescending("ObjA", "DoubleB"));
        checkSame(table, t -> t.sort(List.of(
                SortColumn.asc(ColumnName.of("CharA")),
                SortColumn.desc(ColumnName.of("IntB")),
                SortColumn.asc(ColumnName.of("ObjB")),
                SortColumn.desc(ColumnName.of("FloatB")))));
    }

    /**
     * A sort column whose source is row-key agnostic (every row holds the same value) is dropped before a kernel is
     * selected. The kernel for the shape that remains must already be compiled when the sort listener first needs it:
     * the listener may run on an update graph thread whose ExecutionContext cannot compile, and for an initially empty
     * table the listener is the first to sort anything.
     */
    @Test
    public void testRowKeyAgnosticSortColumnOnUpdateThread() {
        final QueryTable base = TstUtils.testRefreshingTable(intCol("IntA"), longCol("LongB"), stringCol("ObjB"));
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>(base.getColumnSourceMap());
        sources.put("Const", NullValueColumnSource.getInstance(int.class, null));
        final QueryTable source = new QueryTable(base.getRowSet(), sources);
        source.setRefreshing(true);

        // a shape (with and without the constant column) that no other test compiles, so the kernel cache cannot
        // already hold it
        final List<SortColumn> sortColumns = List.of(
                SortColumn.asc(ColumnName.of("IntA")),
                SortColumn.desc(ColumnName.of("Const")),
                SortColumn.desc(ColumnName.of("LongB")),
                SortColumn.asc(ColumnName.of("ObjB")));
        // constructed while the table is empty, on a thread whose context has a QueryCompiler
        final Table sorted = source.sort(sortColumns);

        final int[] intA = {3, 1, 2, 1, 3, 2};
        final long[] longB = {1, 2, 3, 4, 5, 6};
        final String[] objB = {"b", "a", "c", "a", "b", "c"};

        // the first rows arrive under a context with no QueryCompiler, as they would on an update graph thread
        final ExecutionContext context = ExecutionContext.getContext();
        final ExecutionContext noCompiler = ExecutionContext.newBuilder()
                .captureQueryScope()
                .captureQueryLibrary()
                .setUpdateGraph(context.getUpdateGraph())
                .setOperationInitializer(context.getOperationInitializer())
                .build();
        final ControlledUpdateGraph updateGraph = context.getUpdateGraph().cast();
        try (final SafeCloseable ignored = noCompiler.open()) {
            updateGraph.runWithinUnitTestCycle(() -> {
                final RowSet added = RowSetFactory.fromRange(0, intA.length - 1);
                TstUtils.addToTable(source, added, intCol("IntA", intA), longCol("LongB", longB),
                        col("ObjB", objB), intCol("Const"));
                source.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
            });
        }

        TestCase.assertFalse("sorted.isFailed()", ((BaseTable<?>) sorted).isFailed());
        final int[] constValues = new int[intA.length];
        java.util.Arrays.fill(constValues, QueryConstants.NULL_INT);
        final Table expected = TableTools.newTable(
                intCol("IntA", intA), longCol("LongB", longB), col("ObjB", objB), intCol("Const", constValues))
                .sort(sortColumns);
        assertTableEquals(expected, sorted);
    }

    /**
     * When every sort column is row-key agnostic nothing remains to sort by; constructing such a sort on an empty
     * refreshing table (a partitioned table proxy sorting by its partition key, which is constant within each
     * constituent) must not try to prepare a kernel with no columns, and the result is the source order.
     */
    @Test
    public void testAllRowKeyAgnosticSortColumns() {
        TestCase.assertFalse(IndirectTimsortKernelFactory.hasKernel(new ChunkType[0], new Comparator[0]));

        final QueryTable base = TstUtils.testRefreshingTable(intCol("IntA"));
        final Map<String, ColumnSource<?>> sources = new LinkedHashMap<>(base.getColumnSourceMap());
        sources.put("ConstI", NullValueColumnSource.getInstance(int.class, null));
        sources.put("ConstS", NullValueColumnSource.getInstance(String.class, null));
        final QueryTable source = new QueryTable(base.getRowSet(), sources);
        source.setRefreshing(true);

        final Table sortedOne = source.sortDescending("ConstI");
        final Table sortedBoth = source.sort("ConstS", "ConstI");
        // one column remaining after the constants are dropped uses the pregenerated kernels
        final Table sortedWithData = source.sort("ConstS", "IntA", "ConstI");

        final int[] intA = {3, 1, 2};
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(0, intA.length - 1);
            TstUtils.addToTable(source, added, intCol("IntA", intA), intCol("ConstI"), stringCol("ConstS"));
            source.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        });

        for (final Table sorted : new Table[] {sortedOne, sortedBoth, sortedWithData}) {
            TestCase.assertFalse("sorted.isFailed()", ((BaseTable<?>) sorted).isFailed());
        }
        assertTableEquals(source, sortedOne);
        assertTableEquals(source, sortedBoth);
        assertTableEquals(source.sort("IntA"), sortedWithData);
    }
}
