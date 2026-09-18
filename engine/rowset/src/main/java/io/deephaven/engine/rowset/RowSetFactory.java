//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.engine.rowset.impl.AdaptiveRowSetBuilderRandom;
import io.deephaven.engine.rowset.impl.BasicRowSetBuilderSequential;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.datastructures.LongRangeConsumer;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrays;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

/**
 * Repository of factory methods for constructing {@link WritableRowSet row sets}.
 */
public abstract class RowSetFactory {

    /**
     * How {@link #union(Collection)} builds its result. {@link #RADIX} is the default; {@link #SHIPPED} is the merge it
     * replaced, kept selectable so the two can be compared in one build.
     */
    public enum UnionStrategy {
        /**
         * Merge in passes: an accumulator keeps absorbing the next row set while it appends or while the previous one
         * duplicated rows already held, otherwise a new group starts. See {@link #mergeInPasses}.
         */
        SHIPPED,
        /**
         * When the inputs together hold more entries than a {@link SortedRanges} can, build an {@link RspBitmap} by a
         * radix pass on the block bits: every range of the small inputs is split into block-local pieces bucketed by
         * block, and each block's container is built once from its own pieces; bitmap-sized inputs are merged in passes
         * and combined at the end. See {@link #unionWithRadix}.
         */
        RADIX
    }

    @VisibleForTesting
    public static UnionStrategy unionStrategy = UnionStrategy.RADIX;

    private RowSetFactory() {}

    /**
     * Get an empty {@link WritableRowSet}.
     *
     * @return A new {@link WritableRowSet} containing no rows
     */
    public static WritableRowSet empty() {
        return new WritableRowSetImpl();
    }

    /**
     * Get a {@link WritableRowSet} containing the specified row keys. Row keys must be nonnegative numbers.
     *
     * @param rowKeys The row keys to include
     * @return A new {@link WritableRowSet} containing the specified row keys
     */
    public static WritableRowSet fromKeys(final long... rowKeys) {
        if (rowKeys.length == 0) {
            return empty();
        }
        if (rowKeys.length == 1) {
            return fromKeys(rowKeys[0]);
        }
        final RowSetBuilderRandom indexBuilder = builderRandom();
        for (final long rowKey : rowKeys) {
            indexBuilder.addKey(rowKey);
        }
        return indexBuilder.build();
    }

    /**
     * Produce a {@link WritableRowSet} containing a single row key. Row keys must be nonnegative numbers.
     *
     * @param rowKey The row key to include
     * @return A new {@link WritableRowSet} containing the specified row key
     */
    public static WritableRowSet fromKeys(final long rowKey) {
        return fromRange(rowKey, rowKey);
    }

    /**
     * Create a {@link WritableRowSet} containing the continuous range [firstRowKey, lastRowKey], or an {@link #empty()
     * empty row set} if {@code lastRowKey < firstRowKey}.
     *
     * @param firstRowKey The first row key in the continuous range
     * @param lastRowKey The last row key in the continuous range
     * @return A new {@link WritableRowSet} containing the specified row key range
     */
    public static WritableRowSet fromRange(final long firstRowKey, final long lastRowKey) {
        if (lastRowKey < firstRowKey) {
            return empty();
        }
        return new WritableRowSetImpl(SingleRange.make(firstRowKey, lastRowKey));
    }

    /**
     * Get a flat {@link WritableRowSet} containing the row key range {@code [0, size)}, or an {@link #empty() empty row
     * set} if {@code size <= 0}.
     *
     * @param size The size of the {@link WritableRowSet} to create
     * @return A flat {@link WritableRowSet} containing the row key range {@code [0, size)} or an {@link #empty() empty
     *         row set} if the {@code size <= 0}
     */
    public static WritableRowSet flat(final long size) {
        return size <= 0 ? empty() : fromRange(0, size - 1);
    }

    /**
     * @return A {@link RowSetBuilderRandom} suitable for inserting row keys and row key ranges in no particular order
     */
    public static RowSetBuilderRandom builderRandom() {
        return new AdaptiveRowSetBuilderRandom();
    }

    /**
     * @return A {@link RowSetBuilderRandom} optimized for inserting row keys and row key ranges sequentially in order
     */
    public static RowSetBuilderSequential builderSequential() {
        return new BasicRowSetBuilderSequential();
    }

    /**
     * Construct a new {@link WritableRowSet} from the union of {@code rowSets}, leaving the inputs untouched.
     *
     * <p>
     * The caller keeps ownership of {@code rowSets} and remains responsible for closing them. Considering only the
     * {@link RowSet#isNonempty() nonempty} inputs: if none exist {@link RowSetFactory#empty()} is returned; if exactly
     * one exists a {@link RowSet#copy() copy} of it is returned.
     *
     * @param rowSets The row sets to union
     * @return A new {@link WritableRowSet} containing every row key in {@code rowSets}
     */
    public static WritableRowSet union(final Collection<? extends RowSet> rowSets) {
        final RowSet[] input = rowSets.toArray(RowSet[]::new);
        return union(input, input.length);
    }

    /**
     * Construct a new {@link WritableRowSet} from the union of {@code rowSets}, leaving the inputs untouched.
     *
     * <p>
     * The array itself is not reordered. See {@link #union(Collection)} for the ownership and result contract.
     *
     * @param rowSets The row sets to union
     * @return A new {@link WritableRowSet} containing every row key in {@code rowSets}
     */
    public static WritableRowSet union(final RowSet... rowSets) {
        return union(rowSets.clone(), rowSets.length);
    }

    /**
     * Constructs a new combined {@link WritableRowSet} from the union of {@code rowSets}.
     *
     * @param rowSets the input row sets
     * @return the new row set
     * @deprecated Use {@link #union(Collection)}.
     */
    @Deprecated(forRemoval = true)
    public static WritableRowSet unionInsert(final Collection<RowSet> rowSets) {
        return union(rowSets);
    }

    /**
     * Union {@code rowSets[0, size)}, which this method owns and may reorder and clear.
     *
     * <p>
     * Row sets are merged in passes. Within a pass an accumulator keeps absorbing the next row set while that row set
     * only appends to it, and while the row set before it duplicated rows the accumulator already held, which means the
     * inputs are covering each other and further insertion stays cheap. A new accumulator is started as soon as the
     * next row set overlaps and the one before it brought nothing the accumulator already had, which is where inserting
     * everything into a single accumulator would become quadratic. Only the most recent insertion counts: a cumulative
     * count would let one early overlapping pair license absorbing an unbounded run of disjoint row sets afterwards.
     * Every accumulator takes at least one partner, so a pass at least halves the count and the merge terminates; where
     * nothing duplicates anything this is a balanced pairwise merge, and where the inputs are disjoint and ordered the
     * first pass consumes all of them by appending.
     *
     * <p>
     * Sorting by first row key is what makes the append case reachable regardless of the order the caller supplies.
     * Cardinality and endpoints are O(1) to query; range counts, which drive the real cost, are linear in the span
     * count and too expensive to consult per decision.
     */
    private static WritableRowSet union(final RowSet[] rowSets, final int size) {
        // Compact away the empty inputs so that first and last row key are meaningful for every remaining row set.
        int count = 0;
        for (int ii = 0; ii < size; ++ii) {
            final RowSet rowSet = rowSets[ii];
            rowSets[ii] = null;
            if (rowSet == null) {
                continue;
            }
            if (rowSet.isEmpty()) {
                continue;
            }
            rowSets[count++] = rowSet;
        }
        if (count == 0) {
            return empty();
        }
        Arrays.sort(rowSets, 0, count, Comparator.comparingLong(RowSet::firstRowKey));
        if (unionStrategy == UnionStrategy.RADIX) {
            return unionWithRadix(rowSets, count);
        }
        return mergeInPasses(rowSets, count);
    }

    /**
     * Merge {@code rowSets[0, count)}, nonempty and sorted by first row key, in passes. The array is cleared as its
     * entries are consumed.
     */
    private static WritableRowSet mergeInPasses(final RowSet[] rowSets, final int count) {

        // Each group but the last takes at least two row sets.
        final WritableRowSet[] groups = new WritableRowSet[(count + 1) / 2];
        int groupCount = 0;
        try {
            int read = 0;
            while (read < count) {
                final WritableRowSet accumulator = rowSets[read].copy();
                rowSets[read++] = null;
                groups[groupCount++] = accumulator;
                long duplicates = 0;
                if (read < count) {
                    duplicates = absorb(accumulator, rowSets[read], appends(accumulator, rowSets[read]));
                    rowSets[read++] = null;
                }
                while (read < count) {
                    final RowSet next = rowSets[read];
                    final boolean appends = appends(accumulator, next);
                    if (!appends && duplicates == 0) {
                        break;
                    }
                    duplicates = absorb(accumulator, next, appends);
                    rowSets[read++] = null;
                }
            }
            while (groupCount > 1) {
                int write = 0;
                int read2 = 0;
                while (read2 < groupCount) {
                    final WritableRowSet accumulator = groups[read2];
                    if (write != read2) {
                        groups[write] = accumulator;
                        groups[read2] = null;
                    }
                    ++write;
                    ++read2;
                    long duplicates = 0;
                    if (read2 < groupCount) {
                        // Every row set in this pass is an accumulator the first pass created, so absorbing one hands
                        // this method the last reference to it. Insertion borrows its argument; closing is ours to do.
                        try (final WritableRowSet next = groups[read2]) {
                            groups[read2++] = null;
                            duplicates = absorb(accumulator, next, appends(accumulator, next));
                        }
                    }
                    while (read2 < groupCount) {
                        final WritableRowSet next = groups[read2];
                        final boolean appends = appends(accumulator, next);
                        if (!appends && duplicates == 0) {
                            break;
                        }
                        try (next) {
                            groups[read2++] = null;
                            duplicates = absorb(accumulator, next, appends);
                        }
                    }
                }
                groupCount = write;
            }
            return groups[0];
        } catch (final RuntimeException | Error e) {
            // Accumulators are parked in groups before anything is inserted into them, so everything this method
            // created is reachable from that array.
            closeAll(groups, groups.length);
            throw e;
        }
    }

    /**
     * Union {@code rowSets[0, count)}, nonempty and sorted by first row key, by a radix pass on the block bits.
     *
     * <p>
     * One pass over the inputs sums the {@link SingleRange} and {@link SortedRanges} inputs' entries. When the total
     * fits a {@link SortedRanges} those inputs are few or small, and at that size the merge is cheap whichever way it
     * is done, so everything goes through {@link #mergeInPasses}; {@link RspBitmap} inputs do not count, since they
     * merge in passes either way and the build only pays off over many small inputs. Otherwise the result is built as
     * an {@link RspBitmap} directly. The sum is an upper bound, since overlapping and abutting inputs coalesce, so this
     * is the heuristic that chooses the bitmap build rather than proof of what the union needs; a result the bitmap is
     * oversized for is compacted at the end. Every {@link SingleRange} and {@link SortedRanges} input is split into
     * block-local pieces bucketed by block, and each block's container is built once from its own pieces, see
     * {@link RspBitmap#makeFromBlockPieces}. That is linear in the input, lays the span array out exactly once, and
     * coalesces pieces that abut whichever inputs they came from, which a merge in passes achieves only through its
     * passes. {@link RspBitmap} inputs, whose insert walks both span arrays, still merge in passes, and the two results
     * are combined by inserting the smaller into the larger. Inputs whose implementation cannot be read merge in passes
     * as well. Blocks are indexed by offset from the first block when the inputs' block range is narrow, and through a
     * hash of the block index when it is wide, as it is for any union spanning two regions of a table addressed by
     * region, so the cost follows the blocks touched.
     */
    private static WritableRowSet unionWithRadix(final RowSet[] rowSets, final int count) {
        // Only the small inputs are built by radix; bitmap inputs merge in passes whatever else is present. So the
        // decision counts the small inputs' entries alone: a bitmap beside a handful of small ranges is a merge, not a
        // build.
        long entries = 0;
        for (int ii = 0; ii < count; ++ii) {
            final OrderedLongSet inner = innerSet(rowSets[ii]);
            if (inner instanceof SingleRange) {
                entries += 2;
            } else if (inner instanceof SortedRanges) {
                entries += ((SortedRanges) inner).count();
            }
            if (entries > SortedRanges.MAX_CAPACITY) {
                break;
            }
        }
        if (entries <= SortedRanges.MAX_CAPACITY) {
            return mergeInPasses(rowSets, count);
        }

        long firstBlock = Long.MAX_VALUE;
        long lastBlock = -1;
        for (int ii = 0; ii < count; ++ii) {
            final RowSet rowSet = rowSets[ii];
            final OrderedLongSet inner = innerSet(rowSet);
            if (inner instanceof SingleRange || inner instanceof SortedRanges) {
                firstBlock = Math.min(firstBlock, rowSet.firstRowKey() >> RspArray.BITS_PER_BLOCK);
                lastBlock = Math.max(lastBlock, rowSet.lastRowKey() >> RspArray.BITS_PER_BLOCK);
            }
        }
        if (lastBlock < 0) {
            return mergeInPasses(rowSets, count);
        }
        final RspBitmap radix = radixSeed(rowSets, count, firstBlock, lastBlock);
        if (radix == null) {
            return mergeInPasses(rowSets, count);
        }

        final WritableRowSet small = new WritableRowSetImpl(radix);
        // The entry sum that chose the bitmap build is an upper bound; where the inputs coalesced into something a
        // smaller representation holds, take it. This is O(1) unless the result is small enough to convert.
        small.compact();
        // Bitmap inputs, and any implementation that cannot be read, stay in first-key order at the front.
        int large = 0;
        for (int ii = 0; ii < count; ++ii) {
            final RowSet rowSet = rowSets[ii];
            rowSets[ii] = null;
            final OrderedLongSet inner = innerSet(rowSet);
            if (!(inner instanceof SingleRange || inner instanceof SortedRanges)) {
                rowSets[large++] = rowSet;
            }
        }
        if (large == 0) {
            return small;
        }
        final WritableRowSet merged;
        try {
            merged = mergeInPasses(rowSets, large);
        } catch (final RuntimeException | Error e) {
            small.close();
            throw e;
        }
        // Insert the smaller into the larger: a bitmap insert walks the accumulator's spans. Whatever happens, each of
        // the two is closed exactly once: the one inserted from always, the one inserted into only on failure.
        final WritableRowSet into;
        final WritableRowSet from;
        if (small.size() >= merged.size()) {
            into = small;
            from = merged;
        } else {
            into = merged;
            from = small;
        }
        try (from) {
            into.insert(from);
        } catch (final RuntimeException | Error e) {
            into.close();
            throw e;
        }
        return into;
    }

    /**
     * Whether inserting {@code next} into {@code accumulator} only extends it past its last row key, which the row set
     * implementations satisfy by splicing rather than by merging range by range.
     */
    private static boolean appends(final RowSet accumulator, final RowSet next) {
        return next.firstRowKey() > accumulator.lastRowKey();
    }

    private static OrderedLongSet innerSet(final RowSet rowSet) {
        return rowSet instanceof WritableRowSetImpl ? ((WritableRowSetImpl) rowSet).getInnerSet() : null;
    }

    /**
     * Widest block range indexed by block offset from the first block, at two ints a block: 8 MB at the limit. Wider
     * ranges, which a table addressed by region produces as soon as its inputs span two regions, index the blocks
     * touched through a hash instead, so their cost follows the blocks touched and not the span between them.
     */
    private static final long RADIX_DENSE_MAX_BLOCKS = 1L << 20;

    /**
     * Build the union of the small inputs by a radix pass on the block bits. One walk over their ranges counts the
     * block-local pieces each block receives and records the runs of blocks a range covers whole; a second walk places
     * every piece in its block's slice of one array; and {@link RspBitmap#makeFromBlockPieces} then builds every
     * block's container once from that slice. Every range is visited twice and every piece written once, which is
     * linear in the input apart from sorting the runs of full blocks, and the result's span array is laid out exactly
     * once.
     *
     * <p>
     * A piece is a range clipped to one block, held as its two 16-bit block-local ends in one int. A range that spans
     * blocks contributes at most two pieces, its ends, and one run of full blocks between them.
     *
     * <p>
     * Blocks are indexed by offset from the first block when the range of blocks is narrow enough for arrays, and
     * through a hash of the block index otherwise, so a union whose inputs are far apart in the key space costs what
     * its touched blocks cost and no more.
     *
     * @return The union of the small inputs, or null when the piece count does not fit an int
     */
    private static RspBitmap radixSeed(
            final RowSet[] rowSets,
            final int count,
            final long firstBlock,
            final long lastBlock) {
        final long blockSpan = lastBlock - firstBlock + 1;
        final BlockIndex index = blockSpan <= RADIX_DENSE_MAX_BLOCKS
                ? new DenseBlockIndex(firstBlock, (int) blockSpan)
                : new HashedBlockIndex();
        final PieceBucketer bucketer = new PieceBucketer(index);
        for (int ii = 0; ii < count; ++ii) {
            final RowSet rowSet = rowSets[ii];
            final OrderedLongSet inner = innerSet(rowSet);
            if (inner instanceof SingleRange || inner instanceof SortedRanges) {
                rowSet.forAllRowKeyRanges(bucketer);
            }
        }
        final int[] pieces = index.finishCounting();
        if (pieces == null) {
            return null;
        }
        bucketer.startPlacing(pieces);
        for (int ii = 0; ii < count; ++ii) {
            final RowSet rowSet = rowSets[ii];
            final OrderedLongSet inner = innerSet(rowSet);
            if (inner instanceof SingleRange || inner instanceof SortedRanges) {
                rowSet.forAllRowKeyRanges(bucketer);
            }
        }
        return index.build(pieces);
    }

    /**
     * Where a block's pieces go. Counts pieces per block and records full runs during the first walk, hands out
     * placement positions during the second, and finally lays the blocks out in order for the builder.
     */
    private abstract static class BlockIndex {
        /** Runs of full blocks as first, last pairs, in the order the ranges were seen. */
        long[] fullRuns = new long[16];
        int fullRunCount;

        /** Called only on the first walk. */
        abstract void countPiece(long block);

        void addFullRun(final long first, final long last) {
            if (2 * fullRunCount + 2 > fullRuns.length) {
                fullRuns = Arrays.copyOf(fullRuns, fullRuns.length * 2);
            }
            fullRuns[2 * fullRunCount] = first;
            fullRuns[2 * fullRunCount + 1] = last;
            ++fullRunCount;
        }

        /**
         * Turn the counts into placement positions.
         *
         * @return The piece array to place into, or null when the piece count does not fit an int
         */
        abstract int[] finishCounting();

        /** Called only on the second walk: where the next piece of {@code block} goes. */
        abstract int nextPosition(long block);

        abstract RspBitmap build(int[] pieces);

        /**
         * Sort the runs by first block and coalesce runs that overlap or touch, in place. The runs are the ranges that
         * covered whole blocks, so this is O(F log F) in their number, not in the rows or pieces.
         *
         * @return The number of runs left
         */
        final int coalesceFullRuns() {
            if (fullRunCount <= 1) {
                return fullRunCount;
            }
            final long[] firsts = new long[fullRunCount];
            final long[] lasts = new long[fullRunCount];
            for (int r = 0; r < fullRunCount; ++r) {
                firsts[r] = fullRuns[2 * r];
                lasts[r] = fullRuns[2 * r + 1];
            }
            LongArrays.quickSort(firsts, lasts);
            int out = 0;
            for (int r = 0; r < fullRunCount; ++r) {
                if (out > 0 && firsts[r] <= fullRuns[2 * out - 1] + 1) {
                    fullRuns[2 * out - 1] = Math.max(fullRuns[2 * out - 1], lasts[r]);
                } else {
                    fullRuns[2 * out] = firsts[r];
                    fullRuns[2 * out + 1] = lasts[r];
                    ++out;
                }
            }
            fullRunCount = out;
            return out;
        }
    }

    /** Blocks indexed by offset from the first block, in arrays over the whole block range. */
    private static final class DenseBlockIndex extends BlockIndex {
        private final long firstBlock;
        private final int blockSpan;
        /** Piece counts at {@code b + 1} while counting; where block {@code b}'s pieces begin once finished. */
        private final int[] offsets;
        private int[] next;

        DenseBlockIndex(final long firstBlock, final int blockSpan) {
            this.firstBlock = firstBlock;
            this.blockSpan = blockSpan;
            offsets = new int[blockSpan + 1];
        }

        @Override
        void countPiece(final long block) {
            ++offsets[(int) (block - firstBlock) + 1];
        }

        @Override
        int[] finishCounting() {
            // Block b's count sits at b + 1, so the running sum in place leaves offsets[b] as where block b's pieces
            // begin and offsets[b + 1] as where they end.
            for (int b = 0; b < blockSpan; ++b) {
                final long sum = (long) offsets[b + 1] + offsets[b];
                if (sum > Integer.MAX_VALUE - 8) {
                    return null;
                }
                offsets[b + 1] = (int) sum;
            }
            next = Arrays.copyOf(offsets, blockSpan);
            return new int[offsets[blockSpan]];
        }

        @Override
        int nextPosition(final long block) {
            return next[(int) (block - firstBlock)]++;
        }

        @Override
        RspBitmap build(final int[] pieces) {
            // Compact to the blocks that received pieces: their indices in order, and their slices.
            int touched = 0;
            for (int b = 0; b < blockSpan; ++b) {
                if (offsets[b + 1] > offsets[b]) {
                    ++touched;
                }
            }
            final long[] blocks = new long[touched];
            final int[] compact = new int[touched + 1];
            int k = 0;
            for (int b = 0; b < blockSpan; ++b) {
                if (offsets[b + 1] > offsets[b]) {
                    blocks[k] = firstBlock + b;
                    compact[k] = offsets[b];
                    ++k;
                }
            }
            compact[touched] = offsets[blockSpan];
            final int runCount = coalesceFullRuns();
            return RspBitmap.makeFromBlockPieces(blocks, touched, compact, pieces, fullRuns, runCount);
        }
    }

    /**
     * Blocks indexed through a hash of the block index, for a block range too wide for arrays. Slots are handed out in
     * discovery order and ranked into block order once counting is complete.
     */
    private static final class HashedBlockIndex extends BlockIndex {
        private final Long2IntOpenHashMap slotOf = new Long2IntOpenHashMap();
        private long[] slotBlock = new long[64];
        private int[] slotCount = new int[64];
        private int slots;
        /** Where each slot's block falls in block order, once counting is complete. */
        private int[] rankOf;
        private long[] blocksInOrder;
        private int[] offsets;
        private int[] next;

        HashedBlockIndex() {
            slotOf.defaultReturnValue(-1);
        }

        @Override
        void countPiece(final long block) {
            int slot = slotOf.get(block);
            if (slot < 0) {
                slot = slots++;
                slotOf.put(block, slot);
                if (slot == slotBlock.length) {
                    slotBlock = Arrays.copyOf(slotBlock, 2 * slot);
                    slotCount = Arrays.copyOf(slotCount, 2 * slot);
                }
                slotBlock[slot] = block;
            }
            ++slotCount[slot];
        }

        @Override
        int[] finishCounting() {
            blocksInOrder = Arrays.copyOf(slotBlock, slots);
            Arrays.sort(blocksInOrder);
            rankOf = new int[slots];
            offsets = new int[slots + 1];
            for (int slot = 0; slot < slots; ++slot) {
                final int rank = Arrays.binarySearch(blocksInOrder, slotBlock[slot]);
                rankOf[slot] = rank;
                offsets[rank + 1] = slotCount[slot];
            }
            for (int k = 0; k < slots; ++k) {
                final long sum = (long) offsets[k + 1] + offsets[k];
                if (sum > Integer.MAX_VALUE - 8) {
                    return null;
                }
                offsets[k + 1] = (int) sum;
            }
            next = Arrays.copyOf(offsets, slots);
            return new int[offsets[slots]];
        }

        @Override
        int nextPosition(final long block) {
            return next[rankOf[slotOf.get(block)]]++;
        }

        @Override
        RspBitmap build(final int[] pieces) {
            final int runCount = coalesceFullRuns();
            return RspBitmap.makeFromBlockPieces(blocksInOrder, slots, offsets, pieces, fullRuns, runCount);
        }
    }

    /**
     * Splits ranges into block-local pieces. Counts pieces per block until {@link #startPlacing} hands it the array to
     * place them in, then places them.
     */
    private static final class PieceBucketer implements LongRangeConsumer {
        private final BlockIndex index;
        private int[] pieces;

        PieceBucketer(final BlockIndex index) {
            this.index = index;
        }

        void startPlacing(final int[] pieces) {
            this.pieces = pieces;
        }

        @Override
        public void accept(final long start, final long end) {
            final long firstBlock = start >> RspArray.BITS_PER_BLOCK;
            final long lastBlock = end >> RspArray.BITS_PER_BLOCK;
            final int startLow = (int) (start & RspArray.BLOCK_LAST);
            final int endLow = (int) (end & RspArray.BLOCK_LAST);
            if (firstBlock == lastBlock) {
                if (startLow == 0 && endLow == RspArray.BLOCK_LAST) {
                    full(firstBlock, firstBlock);
                } else {
                    piece(firstBlock, startLow, endLow);
                }
                return;
            }
            // The blocks strictly between the ends are full; each end is full only if the range reaches its edge.
            final long firstFull = startLow == 0 ? firstBlock : firstBlock + 1;
            final long lastFull = endLow == RspArray.BLOCK_LAST ? lastBlock : lastBlock - 1;
            if (firstFull <= lastFull) {
                full(firstFull, lastFull);
            }
            if (startLow != 0) {
                piece(firstBlock, startLow, RspArray.BLOCK_LAST);
            }
            if (endLow != RspArray.BLOCK_LAST) {
                piece(lastBlock, 0, endLow);
            }
        }

        private void full(final long firstBlock, final long lastBlock) {
            if (pieces == null) {
                index.addFullRun(firstBlock, lastBlock); // recorded on the first walk only
            }
        }

        private void piece(final long block, final int startLow, final int endLow) {
            if (pieces == null) {
                index.countPiece(block);
                return;
            }
            pieces[index.nextPosition(block)] = (startLow << 16) | endLow;
        }
    }

    /**
     * Insert {@code next} into {@code accumulator}.
     *
     * @return The number of rows of {@code next} that {@code accumulator} already held. An append cannot duplicate
     *         anything, so it does not need to be measured.
     */
    private static long absorb(
            final WritableRowSet accumulator,
            final RowSet next,
            final boolean appends) {
        if (appends) {
            accumulator.insert(next);
            return 0;
        }
        final long accumulatorSize = accumulator.size();
        final long nextSize = next.size();
        accumulator.insert(next);
        return nextSize - (accumulator.size() - accumulatorSize);
    }

    private static void closeAll(final RowSet[] rowSets, final int size) {
        for (int ii = 0; ii < size; ++ii) {
            final RowSet rowSet = rowSets[ii];
            if (rowSet != null) {
                rowSets[ii] = null;
                rowSet.close();
            }
        }
    }
}
