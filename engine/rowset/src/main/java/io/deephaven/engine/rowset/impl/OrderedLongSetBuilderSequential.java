//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.singlerange.SingleRange;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.annotations.TestUseOnly;

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

    private void flushSrToRsp() {
        pendingSr.forEachLongRange((final long start, final long end) -> {
            flushRangeToPendingContainer(start, end);
            return true;
        });
        pendingSr = null;
    }
}
