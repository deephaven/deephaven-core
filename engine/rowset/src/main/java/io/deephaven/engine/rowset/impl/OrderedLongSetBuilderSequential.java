//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.annotations.TestUseOnly;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.BLOCK_LAST;
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
        checkAndMarkBuilt();
        if (pendingStart != -1) {
            if (pendingSr == null && pendingContainerKey == -1 && rb == null) {
                final SingleRange r = SingleRange.make(pendingStart, pendingEnd);
                rowSetCounts.sampleSingleRange(r);
                pendingStart = -1;
                return r;
            }
            flushPendingRange();
        }
        if (pendingSr != null) {
            // Give up our reference to the result, like the other branches do; otherwise later builder use
            // would mutate (or double-release) the set we returned.
            final SortedRanges ans = pendingSr.tryCompactUnsafe(4);
            pendingSr = null;
            rowSetCounts.sampleSortedRanges(ans);
            return ans;
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
        checkAndMarkBuilt();
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
    public void appendOrderedLongSet(final long shiftAmount, final OrderedLongSet ix) {
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
        // Every path that creates rb appends to it immediately, so rb is never empty here. That matters
        // because appendShiftedUnsafeNoWriteCheck reads rb's last span (lastValue(), spanInfos[size - 1]),
        // which is not valid on an empty bitmap.
        Assert.eqFalse(rb.isEmpty(), "rb.isEmpty()");
        rb.appendShiftedUnsafeNoWriteCheck(shiftAmount, (RspBitmap) ix);
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

    /**
     * Move the accumulated {@link SortedRanges} into the RSP.
     *
     * <p>
     * Every range is already known at this point, so rather than growing each container one range at a time -- which
     * reallocates its backing storage on every append, and is therefore quadratic in the container's cardinality -- we
     * walk two cursors over the ranges. One feeds ranges to the builder; the other runs ahead to the end of the block
     * being started, so the container for it can be created at its final size and in the representation that suits it.
     */
    private void flushSrToRsp() {
        final SortedRanges sr = pendingSr;
        pendingSr = null;
        try (final RowSet.RangeIterator appendIter = sr.getRangeIterator();
                final RowSet.RangeIterator lookaheadIter = sr.getRangeIterator()) {
            // The range the lookahead is currently sitting on. It is only partially consumed when it carries over into
            // a later block, in which case lookStart is advanced to that block and the range is measured again there.
            boolean lookValid = false;
            long lookStart = 0;
            long lookEnd = 0;
            long sizedBlock = -1;
            while (appendIter.hasNext()) {
                appendIter.next();
                final long start = appendIter.currentRangeStart();
                final long end = appendIter.currentRangeEnd();
                // A range that spans blocks leaves a container pending for the block its *end* falls in; any container
                // for an earlier block is appended whole and never grown, so needs no sizing.
                final long blockKey = highBits(end);
                if (blockKey != sizedBlock) {
                    final long blockLast = blockKey + BLOCK_LAST;
                    int cardinality = 0;
                    int rangeCount = 0;
                    while (true) {
                        if (!lookValid) {
                            if (!lookaheadIter.hasNext()) {
                                break;
                            }
                            lookaheadIter.next();
                            lookStart = lookaheadIter.currentRangeStart();
                            lookEnd = lookaheadIter.currentRangeEnd();
                            lookValid = true;
                        }
                        if (lookStart > blockLast) {
                            break;
                        }
                        if (lookEnd < blockKey) {
                            // Entirely behind the block being measured; nothing here to count.
                            lookValid = false;
                            continue;
                        }
                        // Clip a range that began in an earlier block; only its part in this block belongs here.
                        lookStart = Math.max(lookStart, blockKey);
                        cardinality += (int) (Math.min(lookEnd, blockLast) - lookStart + 1);
                        ++rangeCount;
                        if (lookEnd > blockLast) {
                            // Carries over; leave it current, positioned at the start of the next block.
                            lookStart = blockLast + 1;
                            break;
                        }
                        lookValid = false;
                    }
                    setSizedBlock(blockKey, cardinality, rangeCount);
                    sizedBlock = blockKey;
                }
                flushRangeToPendingContainer(start, end);
            }
        } finally {
            clearSizedBlock();
        }
    }
}
