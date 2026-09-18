//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.engine.util;

import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;

/**
 * The shapes the row set benchmarks in this package are built from, and the representations they are built as.
 *
 * <p>
 * A {@link Pattern} says how two sides interleave; {@link #build} turns one into a pair of key sets, each a flat array
 * of inclusive {@code [start, end]} pairs. Most patterns give the two sides the same number of ranges, but
 * {@link Pattern#DENSE_VS_SPARSE} deliberately does not -- that asymmetry is what it is for. What a benchmark then asks
 * of the pair is its own business: {@link RowSetOverlapsBench} tests the two against each other, while
 * {@link RowSetSubsetOfBench} derives a superset from them. Sharing the generators keeps the two measuring the same
 * shapes.
 *
 * <p>
 * {@link SortedRanges} holds one array position per single key and two per longer range, so a benchmark's sizes have to
 * stay under what fits before a row set converts to {@link RspBitmap}.
 */
public final class RowSetShapes {

    private RowSetShapes() {}

    private static final int BLOCK_SIZE = RspBitmap.BLOCK_SIZE;

    /** Keys per block on the sparse side of {@link Pattern#DENSE_VS_SPARSE}. */
    private static final int KEYS_PER_SPARSE_BLOCK = 8;

    /** Keys each side puts in each shared block in {@link Pattern#SAME_BLOCK_CONTAINERS}. */
    private static final int KEYS_PER_SHARED_BLOCK = 8;

    /** Clusters, and the keys each one has to itself, for {@link Pattern#CLUSTERED}. */
    private static final int CLUSTERS = 8;
    private static final long CLUSTER_SPAN = 64L * BLOCK_SIZE;

    /** Distance between the starts of consecutive runs in {@link Pattern#INTERLEAVED_RUNS}. */
    private static final long RUN_STRIDE = 16384;

    /** Keys per run in {@link Pattern#INTERLEAVED_RUNS}. */
    private static final long RUN_LEN = 64;

    /** How one side is represented. */
    public enum Representation {
        SORTED_RANGES, RSP
    }

    /**
     * Which representation each of the two sides is built as, for benchmarks of a symmetric operation, where the pair
     * is what matters and the order is covered by passing the two sides the other way around. A benchmark of an
     * asymmetric operation wants a {@link Representation} per side instead, so that all four directed combinations are
     * named rather than three plus a flag.
     */
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
        /**
         * {@link #CLUSTERED} with each cluster spread over one block per key instead of packed into a single block, so
         * an {@link RspBitmap} side holds a span per key rather than a span per cluster and a walk over its spans pays
         * per key too.
         */
        CLUSTERED_BLOCKS,
        /**
         * A key in every block on both sides, one apart inside the block, with the last key shared. The block search
         * hits every time and the answer comes down to the low bits, so neither side ever has a gap to seek over. This
         * is the case a seek cannot help and must not hurt.
         */
        SAME_BLOCK_KEYS,
        /**
         * {@link #SAME_BLOCK_KEYS} with several keys in each shared block instead of one, so the spans are containers
         * rather than singletons and the low-bit comparison is a container overlap rather than a value equality.
         */
        SAME_BLOCK_CONTAINERS,
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

    /**
     * The two sides of {@code pattern}.
     *
     * @param size ranges per side, except on {@link Pattern#DENSE_VS_SPARSE}, where it is the count on the sparse
     *        second side and the dense first side holds an eighth as many
     * @return {@code {a, b}}, each a flat array of inclusive {@code [start, end]} pairs
     */
    public static long[][] build(final Pattern pattern, final int size) {
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
            case CLUSTERED:
            case CLUSTERED_BLOCKS: {
                // Cluster c belongs to one side or the other by parity, and each cluster starts far enough into its
                // own key region that no cluster reaches the next. Each side owns half the clusters, so the keys are
                // spread over that half to leave it with `size` of them.
                final int clustersPerSide = CLUSTERS / 2;
                final int per = Math.max(1, size / clustersPerSide);
                // CLUSTERED packs a cluster's keys two apart inside one block; CLUSTERED_BLOCKS gives each key a block
                // of its own, and the cluster then has to be as wide as the keys it holds.
                final boolean perBlock = pattern == Pattern.CLUSTERED_BLOCKS;
                final long keyStride = perBlock ? BLOCK_SIZE : 2L;
                final long clusterSpan = perBlock ? per * (long) BLOCK_SIZE : CLUSTER_SPAN;
                a = new long[2 * per * clustersPerSide];
                b = new long[2 * per * clustersPerSide];
                for (int c = 0; c < CLUSTERS; ++c) {
                    final long[] side = (c & 1) == 0 ? a : b;
                    final int base = per * (c / 2);
                    for (int j = 0; j < per; ++j) {
                        final int r = base + j;
                        side[2 * r] = side[2 * r + 1] = c * clusterSpan + j * keyStride;
                    }
                }
                break;
            }
            case SAME_BLOCK_CONTAINERS: {
                // Both sides fill the same blocks with KEYS_PER_SHARED_BLOCK keys apiece, offset inside the block so
                // they interleave without meeting, and the last key shared.
                final int blocks = Math.max(1, size / KEYS_PER_SHARED_BLOCK);
                final int keys = blocks * KEYS_PER_SHARED_BLOCK;
                a = new long[2 * keys];
                b = new long[2 * keys];
                for (int k = 0; k < blocks; ++k) {
                    for (int j = 0; j < KEYS_PER_SHARED_BLOCK; ++j) {
                        final int r = KEYS_PER_SHARED_BLOCK * k + j;
                        final long low = 4L * j;
                        a[2 * r] = a[2 * r + 1] = k * (long) BLOCK_SIZE + low;
                        b[2 * r] = b[2 * r + 1] = k * (long) BLOCK_SIZE + low + 2;
                    }
                }
                b[2 * (keys - 1)] = b[2 * keys - 1] = a[2 * (keys - 1)];
                break;
            }
            case SAME_BLOCK_KEYS: {
                // Both sides put one key in block i, a pair of low-bit positions apart so they never coincide, and the
                // low bits move with the block so the comparison is not always against the same value.
                a = new long[2 * size];
                b = new long[2 * size];
                for (int i = 0; i < size; ++i) {
                    final long low = 2L * (i % (BLOCK_SIZE / 4));
                    a[2 * i] = a[2 * i + 1] = i * (long) BLOCK_SIZE + low;
                    b[2 * i] = b[2 * i + 1] = i * (long) BLOCK_SIZE + low + 1;
                }
                b[2 * (size - 1)] = b[2 * size - 1] = a[2 * (size - 1)];
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
        return new long[][] {a, b};
    }

    /** A key set as {@code representation} says to hold it. */
    public static OrderedLongSet impl(final Representation representation, final long[] ranges) {
        switch (representation) {
            case SORTED_RANGES:
                return sortedRanges(ranges);
            case RSP:
                return rsp(ranges);
            default:
                throw new IllegalStateException("unhandled representation " + representation);
        }
    }

    /** The two sides as {@code layout} says to represent them. */
    public static OrderedLongSet[] impls(final Layout layout, final long[] a, final long[] b) {
        switch (layout) {
            case SORTED_SORTED:
                return new OrderedLongSet[] {sortedRanges(a), sortedRanges(b)};
            case SORTED_RSP:
                return new OrderedLongSet[] {sortedRanges(a), rsp(b)};
            case RSP_RSP:
                return new OrderedLongSet[] {rsp(a), rsp(b)};
            default:
                throw new IllegalStateException("unhandled layout " + layout);
        }
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

    /** Whether the two sides share a key, by linear merge, so a benchmark can check what it measures. */
    public static boolean overlaps(final long[] a, final long[] b) {
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

    public static SortedRanges sortedRanges(final long[] ranges) {
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

    public static RspBitmap rsp(final long[] ranges) {
        RspBitmap rb = RspBitmap.makeEmpty();
        for (int i = 0; i < ranges.length; i += 2) {
            rb = rb.addRangeUnsafe(ranges[i], ranges[i + 1]);
        }
        rb.finishMutations();
        return rb;
    }

    /** The union of two key sets, as a flat array of inclusive {@code [start, end]} pairs. */
    public static long[] union(final long[] a, final long[] b) {
        final long[] out = new long[a.length + b.length];
        int n = 0;
        int i = 0;
        int j = 0;
        while (i < a.length || j < b.length) {
            final boolean takeA = j >= b.length || (i < a.length && a[i] <= b[j]);
            final long start = takeA ? a[i] : b[j];
            final long end = takeA ? a[i + 1] : b[j + 1];
            if (takeA) {
                i += 2;
            } else {
                j += 2;
            }
            if (n > 0 && start <= out[n - 1] + 1) {
                // Overlapping or abutting the range we last wrote; extend it rather than starting another.
                out[n - 1] = Math.max(out[n - 1], end);
            } else {
                out[n++] = start;
                out[n++] = end;
            }
        }
        final long[] trimmed = new long[n];
        System.arraycopy(out, 0, trimmed, 0, n);
        return trimmed;
    }

    /**
     * {@code ranges} without {@code key}, which must be one of its keys. Splits the range holding it when it falls in
     * the middle.
     */
    public static long[] without(final long[] ranges, final long key) {
        final long[] out = new long[ranges.length + 2];
        int n = 0;
        for (int i = 0; i < ranges.length; i += 2) {
            final long start = ranges[i];
            final long end = ranges[i + 1];
            if (key < start || key > end) {
                out[n++] = start;
                out[n++] = end;
                continue;
            }
            if (start < key) {
                out[n++] = start;
                out[n++] = key - 1;
            }
            if (key < end) {
                out[n++] = key + 1;
                out[n++] = end;
            }
        }
        final long[] trimmed = new long[n];
        System.arraycopy(out, 0, trimmed, 0, n);
        return trimmed;
    }

    /** Whether every key of {@code sub} is in {@code sup}, by linear merge. */
    public static boolean subsetOf(final long[] sub, final long[] sup) {
        int j = 0;
        for (int i = 0; i < sub.length; i += 2) {
            while (j < sup.length && sup[j + 1] < sub[i]) {
                j += 2;
            }
            if (j >= sup.length || sup[j] > sub[i] || sup[j + 1] < sub[i + 1]) {
                return false;
            }
        }
        return true;
    }

    /** The number of ranges in a key set. */
    public static int rangeCount(final long[] ranges) {
        return ranges.length / 2;
    }
}
