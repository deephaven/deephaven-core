package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.rowset.impl.singlerange.*;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
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
    public OrderedLongSet getTreeIndexImpl() {
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
    public void appendTreeIndexImpl(final long shiftAmount, final OrderedLongSet ix, final boolean acquire) {
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
        pendingSr.forEachLongRange((final long start, final long end) -> {
            flushRangeToPendingContainer(start, end);
            return true;
        });
        pendingSr = null;
    }
}
