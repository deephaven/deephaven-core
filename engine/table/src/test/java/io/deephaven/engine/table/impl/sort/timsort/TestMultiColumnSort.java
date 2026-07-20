//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sort.timsort;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.impl.ComparatorSortColumn;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.SortHelpers;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.impl.sort.MultiColumnSortKernel;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.engine.table.impl.sort.timsort.indirect.IndirectTimsortKernelFactory;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.QueryConstants;
import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;

import java.util.Comparator;
import java.util.List;
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

/**
 * Verifies that {@link QueryTable#sort} produces identical results whether the multi-column timsort kernel or the
 * one-column-at-a-time pipeline is used. The Sentinel column makes the comparison sensitive to any difference in the
 * resulting row permutation, so this also verifies that both paths produce the same stable sort.
 */
public class TestMultiColumnSort {
    @Rule
    public final EngineCleanup framework = new EngineCleanup();

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
        final long oldMinimum = SortHelpers.parallelSortMinimumSize;
        final long oldSegment = SortHelpers.parallelSortSegmentSize;
        final Table serial;
        final Table parallel;
        try {
            SortHelpers.parallelSortMinimumSize = 0;
            serial = invoker.sort(table);
            SortHelpers.parallelSortMinimumSize = 1;
            SortHelpers.parallelSortSegmentSize = 1;
            parallel = invoker.sort(table);
        } finally {
            SortHelpers.parallelSortMinimumSize = oldMinimum;
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
        final long oldMinimum = SortHelpers.parallelSortMinimumSize;
        final long oldSegment = SortHelpers.parallelSortSegmentSize;
        try {
            final Table table = makeTable(new Random(31415), 10000);
            SortHelpers.parallelSortMinimumSize = 0;
            final Table serial = table.sort("ObjA", "IntB");
            SortHelpers.parallelSortMinimumSize = 1;
            SortHelpers.parallelSortSegmentSize = 5000;
            final Table twoSegments = table.sort("ObjA", "IntB");
            assertTableEquals(serial, twoSegments);
        } finally {
            SortHelpers.parallelSortMinimumSize = oldMinimum;
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
}
