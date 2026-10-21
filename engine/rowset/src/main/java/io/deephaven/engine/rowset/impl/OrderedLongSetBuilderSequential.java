//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.annotations.TestUseOnly;

public class OrderedLongSetBuilderSequential extends RspBitmapBuilderSequential {
    /**
     * The ranges accumulated before the builder commits to an RSP. Only ever non-null while {@link #rb} and the pending
     * container are absent, so the base class's paths that work on {@link #rb} never have it to flush.
     */
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
            // The result belongs to the caller, so we give up our reference to it.
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

    /**
     * Only commit to a bitmap when {@code ix} alone certainly has more ranges than a {@link SortedRanges} can hold, so
     * that the ranges would overflow one anyway; otherwise a result that fits in one stays one.
     */
    @Override
    protected boolean commitToBitmapFor(final RspBitmap ix) {
        if (!ix.hasMoreRangesThan(SortedRanges.MAX_CAPACITY)) {
            return false;
        }
        if (pendingSr != null) {
            flushSrToRsp();
        }
        return super.commitToBitmapFor(ix);
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
     */
    private void flushSrToRsp() {
        final SortedRanges sr = pendingSr;
        pendingSr = null;
        appendSortedRangesSizingBlocks(sr);
    }
}
