//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;

import java.util.Arrays;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_SIZE;
import static io.deephaven.engine.rowset.impl.rsp.RspArray.highBits;

public class OrderedLongSetBuilderSequential extends RspBitmapBuilderSequential {
    private SortedRanges pendingSr;

    private static final RowSetCounts rowSetCounts = new RowSetCounts("orderedLongSetBuilderSequential");

    public OrderedLongSetBuilderSequential() {
        this(false);
    }

    public OrderedLongSetBuilderSequential(final boolean disposable) {
        super(disposable);
    }

    @Override
    public OrderedLongSet getOrderedLongSet() {
        if (pendingStart != -1) {
            if (pendingSr == null && pendingContainerKey == -1 && rb == null) {
                final SingleRange r = SingleRange.make(pendingStart, pendingEnd);
                rowSetCounts.sampleSingleRange(r);
                return r;
            }
            flushPendingRange();
        }
        if (pendingSr != null) {
            pendingSr = pendingSr.tryCompactUnsafe(4);
            rowSetCounts.sampleSortedRanges(pendingSr);
            return pendingSr;
        }
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }
        if (rb == null) {
            rowSetCounts.sampleEmpty();
            return OrderedLongSet.EMPTY;
        }
        rb.tryCompactUnsafe(4);
        rb.finishMutations();
        rowSetCounts.sampleRsp(rb);
        final RspBitmap ans = rb;
        rb = null;
        return ans;
    }

    @TestUseOnly
    public RspBitmap getRspBitmap() {
        if (pendingStart != -1) {
            flushPendingRange();
        }
        if (pendingSr != null) {
            flushSrToRsp();
        }
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }
        if (rb == null) {
            return null;
        }
        rb.tryCompactUnsafe(4);
        rb.finishMutations();
        final RspBitmap ans = rb;
        rb = null;
        return ans;
    }

    @Override
    public void appendOrderedLongSet(final long shiftAmount, final OrderedLongSet ix, final boolean acquire) {
        if (ix.ixIsEmpty()) {
            return;
        }
        if (!(ix instanceof RspBitmap) || rb == null) {
            ix.ixForEachLongRange((final long start, final long end) -> {
                appendRange(start + shiftAmount, end + shiftAmount);
                return true;
            });
            return;
        }
        if (pendingStart != -1) {
            flushPendingRange();
        }
        if (pendingSr != null) {
            flushSrToRsp();
        }
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }
        if (rb.isEmpty()) {
            rb.ixInsert(ix);
            return;
        }
        rb.appendShiftedUnsafeNoWriteCheck(shiftAmount, (RspBitmap) ix, acquire);
    }

    @Override
    protected void flushPendingRange() {
        final long pendingStartOnEntry = pendingStart;
        pendingStart = -1;
        if (pendingContainerKey != -1 || rb != null) {
            flushRangeToPendingContainer(pendingStartOnEntry, pendingEnd);
            return;
        }
        if (pendingSr == null) {
            if (maxKeyHint != -1) {
                pendingSr = SortedRanges.makeForKnownRange(pendingStartOnEntry, maxKeyHint, false);
                pendingSr = pendingSr.appendRangeUnsafe(pendingStartOnEntry, pendingEnd);
            } else {
                pendingSr = SortedRanges.makeSingleRange(pendingStartOnEntry, pendingEnd);
            }
            return;
        }
        final SortedRanges ans = pendingSr.appendRangeUnsafe(pendingStartOnEntry, pendingEnd);
        if (ans == null) {
            flushSrToRsp();
            flushRangeToPendingContainer(pendingStartOnEntry, pendingEnd);
            return;
        }
        pendingSr = ans;
    }

    private void flushSrToRsp() {
        final SortedRanges sr = pendingSr;
        pendingSr = null;
        // Every range is already known here, so measure up front what each block will receive. That lets the RSP
        // builder create each container at its final size, and in the representation that suits it: a container grown
        // one range at a time reallocates its backing array on every append, which is quadratic in its cardinality.
        final BlockSizeCounts counts = new BlockSizeCounts();
        sr.forEachLongRange(counts);
        blockSizes = counts;
        try {
            sr.forEachLongRange((final long start, final long end) -> {
                flushRangeToPendingContainer(start, end);
                return true;
            });
        } finally {
            blockSizes = null;
        }
    }

    /**
     * Measures how many values, in how many ranges, a sequence of ordered, disjoint ranges contributes to each RSP
     * block, then serves those back as sizing hints. Blocks are visited in ascending order both while counting and
     * while looking up, so a single cursor suffices.
     */
    private static final class BlockSizeCounts
            implements LongRangeAbortableConsumer, RspBitmapBuilderSequential.BlockSizes {

        private long[] blockKeys = new long[16];
        private int[] cardinalities = new int[16];
        private int[] rangeCounts = new int[16];
        private int size;
        private int cursor;

        @Override
        public boolean accept(final long start, final long end) {
            final long firstBlockKey = highBits(start);
            final long lastBlockKey = highBits(end);
            if (firstBlockKey == lastBlockKey) {
                add(firstBlockKey, (int) (end - start + 1));
                return true;
            }
            // Blocks strictly between the first and last are covered in full and become full block spans rather than
            // containers, so they need no count -- which also keeps this O(1) for ranges spanning many blocks.
            add(firstBlockKey, (int) (firstBlockKey + BLOCK_SIZE - start));
            add(lastBlockKey, (int) (end - lastBlockKey + 1));
            return true;
        }

        /**
         * Account for one range of {@code cardinality} values landing in {@code blockKey}. Ranges arrive in ascending
         * order and are never adjacent, so each call is a distinct run within its block.
         */
        private void add(final long blockKey, final int cardinality) {
            if (size > 0 && blockKeys[size - 1] == blockKey) {
                cardinalities[size - 1] += cardinality;
                ++rangeCounts[size - 1];
                return;
            }
            if (size == blockKeys.length) {
                blockKeys = Arrays.copyOf(blockKeys, size * 2);
                cardinalities = Arrays.copyOf(cardinalities, size * 2);
                rangeCounts = Arrays.copyOf(rangeCounts, size * 2);
            }
            blockKeys[size] = blockKey;
            cardinalities[size] = cardinality;
            rangeCounts[size] = 1;
            ++size;
        }

        @Override
        public int cardinalityForBlock(final long blockKey) {
            return seek(blockKey) ? cardinalities[cursor] : NOT_KNOWN;
        }

        @Override
        public int rangeCountForBlock(final long blockKey) {
            return seek(blockKey) ? rangeCounts[cursor] : NOT_KNOWN;
        }

        private boolean seek(final long blockKey) {
            while (cursor < size && blockKeys[cursor] < blockKey) {
                ++cursor;
            }
            return cursor < size && blockKeys[cursor] == blockKey;
        }
    }
}
