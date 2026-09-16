//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.benchmarking.BenchUtil;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.concurrent.TimeUnit;

/**
 * {@link OrderedLongSet#ixOverlaps} across the shapes, sizes and representations that decide whether the two cursors
 * seek or one of them walks range by range.
 *
 * <p>
 * The cost of an overlap test is set by where the answer is and how the two sets interleave on the way there. The
 * patterns cover both: {@link Pattern#TOUCH_AT_START} answers immediately and measures only the fixed overhead, while
 * the patterns that answer at the end or not at all are where a walk pays per range and a seek pays per alternation.
 * {@link #size} is the number of ranges per side; {@link SortedRanges} holds one array position per single key and two
 * per longer range, so the sizes here are the ones that fit before a row set converts to {@link RspBitmap}.
 *
 * <p>
 * {@link #swapped} tests the same pair with the arguments the other way around. For a symmetric implementation the two
 * orders cost the same; where they do not, the caller's argument order is a performance variable.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(value = 1)
public class RowSetOverlapsBench {

    private static final int BLOCK_SIZE = RspBitmap.BLOCK_SIZE;

    /** Keys per block on the sparse side of {@link Pattern#DENSE_VS_SPARSE}. */
    private static final int KEYS_PER_SPARSE_BLOCK = 8;

    /** Clusters, and the keys each one has to itself, for {@link Pattern#CLUSTERED}. */
    private static final int CLUSTERS = 8;
    private static final long CLUSTER_SPAN = 64L * BLOCK_SIZE;

    /** Distance between the starts of consecutive runs in {@link Pattern#INTERLEAVED_RUNS}. */
    private static final long RUN_STRIDE = 16384;

    /** Keys per run in {@link Pattern#INTERLEAVED_RUNS}. */
    private static final long RUN_LEN = 64;

    /** Which representation each side is built as. */
    public enum Layout {
        SORTED_SORTED, SORTED_RSP, RSP_RSP
    }

    public enum Pattern {
        /** Alternating single keys one apart, never meeting. The tightest interleaving there is. */
        INTERLEAVED_KEYS,
        /** Alternating single keys a block apart, never meeting, so every step crosses an RSP span. */
        INTERLEAVED_BLOCKS,
        /** Alternating runs of {@value #RUN_LEN} keys, never meeting. */
        INTERLEAVED_RUNS,
        /** Key spaces that do not even abut: the whole of one side lies below the whole of the other. */
        SEPARATED,
        /**
         * Both sides hold all their ranges in a handful of clusters, and the clusters alternate between the sides, so
         * the two never meet but interleave only a few times. A walk pays per range here; a seek pays per cluster.
         */
        CLUSTERED,
        /** Interleaved keys a block apart, with the last key of each side shared. */
        TOUCH_AT_END,
        /** Interleaved keys a block apart, with the middle key of each side shared. */
        TOUCH_IN_MIDDLE,
        /** Interleaved keys a block apart, with the first key of each side shared. */
        TOUCH_AT_START,
        /**
         * Whole blocks against a comb of single keys over the same key span, so one side holds eight times the ranges
         * of the other. Which side gets walked is what the size normalization decides.
         */
        DENSE_VS_SPARSE
    }

    @Param
    private Layout layout;

    @Param
    private Pattern pattern;

    /** Ranges per side. */
    @Param({"64", "512", "2048"})
    private int size;

    /** Whether to pass the two sides in the other order. */
    @Param({"false", "true"})
    private boolean swapped;

    private OrderedLongSet left;
    private OrderedLongSet right;
    private boolean expected;

    @Setup
    public void setup() {
        final long[] a;
        final long[] b;
        switch (pattern) {
            case INTERLEAVED_KEYS:
                a = singletons(0, 2, size);
                b = singletons(1, 2, size);
                break;
            case INTERLEAVED_BLOCKS:
                a = singletons(0, 2L * BLOCK_SIZE, size);
                b = singletons(BLOCK_SIZE, 2L * BLOCK_SIZE, size);
                break;
            case INTERLEAVED_RUNS:
                a = runs(0, RUN_STRIDE, RUN_LEN, size);
                b = runs(RUN_LEN, RUN_STRIDE, RUN_LEN, size);
                break;
            case CLUSTERED: {
                // Cluster c belongs to one side or the other by parity, and each cluster starts far enough into its
                // own key region that no cluster reaches the next.
                final int per = Math.max(1, size / CLUSTERS);
                a = new long[2 * per * (CLUSTERS / 2)];
                b = new long[2 * per * (CLUSTERS / 2)];
                for (int c = 0; c < CLUSTERS; ++c) {
                    final long[] side = (c & 1) == 0 ? a : b;
                    final int base = per * (c / 2);
                    for (int j = 0; j < per; ++j) {
                        final int r = base + j;
                        side[2 * r] = side[2 * r + 1] = c * CLUSTER_SPAN + 2L * j;
                    }
                }
                break;
            }
            case SEPARATED:
                a = singletons(0, 2L * BLOCK_SIZE, size);
                b = singletons(2L * BLOCK_SIZE * size, 2L * BLOCK_SIZE, size);
                break;
            case TOUCH_AT_END:
            case TOUCH_IN_MIDDLE:
            case TOUCH_AT_START:
                a = singletons(0, 2L * BLOCK_SIZE, size);
                b = singletons(BLOCK_SIZE, 2L * BLOCK_SIZE, size);
                final int at = pattern == Pattern.TOUCH_AT_START ? 0
                        : pattern == Pattern.TOUCH_IN_MIDDLE ? size / 2 : size - 1;
                b[2 * at] = b[2 * at + 1] = a[2 * at];
                break;
            case DENSE_VS_SPARSE: {
                // The same key span on both sides, one holding whole even blocks and the other eight single keys in
                // each odd block, so one side has eight times the ranges of the other.
                final int blocks = Math.max(1, size / KEYS_PER_SPARSE_BLOCK);
                a = runs(0, 2L * BLOCK_SIZE, BLOCK_SIZE, blocks);
                b = new long[2 * KEYS_PER_SPARSE_BLOCK * blocks];
                for (int k = 0; k < blocks; ++k) {
                    for (int j = 0; j < KEYS_PER_SPARSE_BLOCK; ++j) {
                        final int r = KEYS_PER_SPARSE_BLOCK * k + j;
                        b[2 * r] = b[2 * r + 1] = (2L * k + 1) * BLOCK_SIZE + 2L * j;
                    }
                }
                break;
            }
            default:
                throw new IllegalStateException("unhandled pattern " + pattern);
        }
        expected = merge(a, b);

        final OrderedLongSet first;
        final OrderedLongSet second;
        switch (layout) {
            case SORTED_SORTED:
                first = sortedRanges(a);
                second = sortedRanges(b);
                break;
            case SORTED_RSP:
                first = sortedRanges(a);
                second = rsp(b);
                break;
            case RSP_RSP:
                first = rsp(a);
                second = rsp(b);
                break;
            default:
                throw new IllegalStateException("unhandled layout " + layout);
        }
        left = swapped ? second : first;
        right = swapped ? first : second;

        if (left.ixOverlaps(right) != expected) {
            throw new IllegalStateException("overlaps disagrees with the expected answer for " + pattern);
        }
        System.out.println(layout + " " + pattern + " size=" + size + " swapped=" + swapped
                + " expected=" + expected + " left=" + left.getClass().getSimpleName()
                + " right=" + right.getClass().getSimpleName());
    }

    /** {@code count} single keys {@code stride} apart, as a flat array of inclusive {@code [start, end]} pairs. */
    private static long[] singletons(final long start, final long stride, final int count) {
        final long[] ranges = new long[2 * count];
        for (int i = 0; i < count; ++i) {
            ranges[2 * i] = ranges[2 * i + 1] = start + i * stride;
        }
        return ranges;
    }

    /** {@code count} runs of {@code len} keys, {@code stride} apart. */
    private static long[] runs(final long start, final long stride, final long len, final int count) {
        final long[] ranges = new long[2 * count];
        for (int i = 0; i < count; ++i) {
            ranges[2 * i] = start + i * stride;
            ranges[2 * i + 1] = ranges[2 * i] + len - 1;
        }
        return ranges;
    }

    /** The answer, by linear merge, so the benchmark can check the implementation is measuring the right thing. */
    private static boolean merge(final long[] a, final long[] b) {
        int i = 0;
        int j = 0;
        while (i < a.length && j < b.length) {
            if (a[i] <= b[j + 1] && b[j] <= a[i + 1]) {
                return true;
            }
            if (a[i + 1] < b[j + 1]) {
                i += 2;
            } else {
                j += 2;
            }
        }
        return false;
    }

    private static SortedRanges sortedRanges(final long[] ranges) {
        SortedRanges sr = SortedRanges.makeEmpty();
        for (int i = 0; i < ranges.length; i += 2) {
            sr = sr.appendRange(ranges[i], ranges[i + 1]);
            if (sr == null) {
                throw new IllegalStateException("set of " + (ranges.length / 2) + " ranges does not fit in a "
                        + SortedRanges.class.getSimpleName());
            }
        }
        return sr;
    }

    private static RspBitmap rsp(final long[] ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < ranges.length; i += 2) {
            rb = rb.addRangeUnsafe(ranges[i], ranges[i + 1]);
        }
        rb.finishMutations();
        return rb;
    }

    @Benchmark
    public boolean overlaps() {
        return left.ixOverlaps(right);
    }

    public static void main(String[] args) throws RunnerException {
        BenchUtil.run(RowSetOverlapsBench.class);
    }
}
