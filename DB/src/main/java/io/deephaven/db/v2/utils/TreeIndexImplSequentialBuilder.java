package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.singlerange.*;
import io.deephaven.db.v2.utils.sortedranges.SortedRanges;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.util.annotations.TestUseOnly;

public class TreeIndexImplSequentialBuilder extends RspBitmapSequentialBuilder {
    private SortedRanges pendingSr;

    private static final IndexCounts indexCounts = new IndexCounts("sequentialBuilder");

    public TreeIndexImplSequentialBuilder() {
        this(false);
    }

    public TreeIndexImplSequentialBuilder(final boolean disposable) {
        super(disposable);
    }

    @Override
    public TreeIndexImpl getTreeIndexImpl() {
        if (pendingStart != -1) {
            if (pendingSr == null && pendingContainerKey == -1 && rb == null) {
                final SingleRange r = SingleRange.make(pendingStart, pendingEnd);
                indexCounts.sampleSingleRange(r);
                return r;
            }
            flushPendingRange();
        }
        if (pendingSr != null) {
            pendingSr = pendingSr.tryCompactUnsafe(4);
            indexCounts.sampleSortedRanges(pendingSr);
            return pendingSr;
        }
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }
        if (rb == null) {
            indexCounts.sampleEmpty();
            return TreeIndexImpl.EMPTY;
        }
        rb.tryCompactUnsafe(4);
        rb.finishMutations();
        indexCounts.sampleRsp(rb);
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
    public void appendTreeIndexImpl(final long shiftAmount, final TreeIndexImpl ix, final boolean acquire) {
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
