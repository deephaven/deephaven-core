package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.utils.rsp.DisposableRspBitmap;
import io.deephaven.db.v2.utils.rsp.RspArray;
import io.deephaven.db.v2.utils.rsp.RspBitmap;
import io.deephaven.db.v2.utils.rsp.container.Container;

import static io.deephaven.db.v2.utils.rsp.RspArray.*;
import static io.deephaven.db.v2.utils.rsp.RspBitmap.*;

public class RspBitmapSequentialBuilder implements TreeIndexImpl.SequentialBuilder {
    protected final boolean disposable;

    protected long pendingStart = -1;
    protected long pendingEnd = -1;
    protected long pendingContainerKey = -1;
    protected Container pendingContainer;
    protected RspBitmap rb;
    protected long maxKeyHint = -1;

    public RspBitmapSequentialBuilder() {
        this(false);
    }

    public RspBitmapSequentialBuilder(final boolean disposable) {
        this.disposable = disposable;
    }

    @Override
    public void setDomain(final long firstKeyUnused, final long maxKey) {
        maxKeyHint = (maxKey == Index.NULL_KEY) ? -1 : maxKey;
    }

    @Override
    public TreeIndexImpl getTreeIndexImpl() {
        if (pendingStart != -1) {
            flushPendingRange();
        }
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }
        if (rb == null) {
            return TreeIndexImpl.EMPTY;
        }
        rb.tryCompactUnsafe(4);
        rb.finishMutations();
        final RspBitmap ans = rb;
        rb = null;
        return ans;
    }

    @Override
    public void appendKey(final long v) {
        if (pendingStart != -1) {
            if (check && v <= pendingEnd) {
                throw new IllegalArgumentException(outOfOrderKeyErrorMsg +
                    "last=" + pendingEnd + " while appending value=" + v);
            }
            if (pendingEnd + 1 == v) {
                pendingEnd = v;
                return;
            }
            flushPendingRange();
        }
        pendingStart = pendingEnd = v;
    }

    @Override
    public void appendRange(final long start, final long end) {
        if (RspArray.debug) {
            if (start > end) {
                throw new IllegalArgumentException("start (= " + start + ") > end (= " + end + ")");
            }
        }
        if (pendingStart != -1) {
            if (check && start <= pendingEnd) {
                throw new IllegalArgumentException(outOfOrderKeyErrorMsg +
                    "last=" + pendingEnd + " while appending range start=" + start + ", end="
                    + end);
            }
            if (pendingEnd + 1 == start) {
                pendingEnd = end;
                return;
            }
            flushPendingRange();
        }
        pendingStart = start;
        pendingEnd = end;
    }

    @Override
    public void appendTreeIndexImpl(final long shiftAmount, final TreeIndexImpl ix,
        final boolean acquire) {
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
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }
        if (rb.isEmpty()) {
            rb.ixInsert(ix);
            return;
        }
        rb.appendShiftedUnsafeNoWriteCheck(shiftAmount, (RspBitmap) ix, acquire);
    }

    protected void flushPendingRange() {
        final long pendingStartOnEntry = pendingStart;
        pendingStart = -1;
        flushRangeToPendingContainer(pendingStartOnEntry, pendingEnd);
    }

    protected void flushRangeToPendingContainer(final long start, final long end) {
        final long highStart = highBits(start);
        final int lowStart = RspArray.lowBitsAsInt(start);
        final long highEnd = highBits(end);
        final int lowEnd = RspArray.lowBitsAsInt(end);
        final boolean singleBlock = highStart == highEnd;
        if (singleBlock) { // short path.
            final long pendingContainerBlockKey = highBits(pendingContainerKey);
            if (pendingContainerKey != -1 && pendingContainerBlockKey == highStart) { // short path.
                if (pendingContainer == null) {
                    pendingContainer = containerForLowValueAndRange(
                        lowBitsAsInt(pendingContainerKey), lowStart, lowEnd);
                    pendingContainerKey = highBits(pendingContainerKey);
                } else {
                    pendingContainer = pendingContainer.iappend(lowStart, lowEnd + 1);
                }
                return;
            }
            if (pendingContainerKey != -1) {
                if (check && pendingContainerKey > highStart) {
                    throw new IllegalStateException(outOfOrderKeyErrorMsg +
                        "last=" + end + " while appending value=" + pendingContainer.last());
                }
                flushPendingContainer();
            }
            if (lowStart == 0 && lowEnd == BLOCK_LAST) {
                ensureRb();
                rb.appendFullBlockSpanUnsafeNoWriteCheck(highStart, 1);
                return;
            }
            if (start == end) {
                pendingContainerKey = start;
                pendingContainer = null;
            } else {
                pendingContainerKey = highStart;
                pendingContainer = Container.rangeOfOnes(lowStart, lowEnd + 1);
            }
            return;
        }

        //
        // A range may involve at most 3 spans, any of which may or may not be present:
        // * a block for an initial container.
        // * a full block span
        // * a block for a final container.
        // Note we must have at least two of these, given code above already handled the case for a
        // single block range.
        // If we don't have a particular one, we set its key to -1.
        final long initialContainerKey;
        final int initialContainerStart;
        final int initialContainerEnd;
        final long midFullBlockSpanKey;
        final long midFullBlockSpanLen;
        final long endingContainerKey;
        // final int endingContainerStart; The start of the ending container can only be 0.
        final int endingContainerEnd;

        // Let's see if we have an initial container block.
        if (lowStart > 0) {
            initialContainerKey = highStart;
            initialContainerStart = lowStart;
            initialContainerEnd = BLOCK_LAST;
        } else {
            // we don't have an initial container block.
            initialContainerKey = -1;
            // These two are not used in this case.
            initialContainerStart = 0;
            initialContainerEnd = 0;
        }

        // Let's see if we have a full block span.
        long slen = ((highEnd - highStart) >> 16) - 1;
        if (lowStart == 0) {
            ++slen;
        }
        if (lowEnd == BLOCK_LAST) {
            ++slen;
        }
        if (slen > 0) {
            midFullBlockSpanKey = (lowStart == 0) ? highStart : highStart + BLOCK_SIZE;
            midFullBlockSpanLen = slen;
        } else {
            // we don't have a full block span.
            midFullBlockSpanKey = -1;
            midFullBlockSpanLen = 0; // not used in this case.
        }

        // Let's see if we have an ending container block.
        if (lowEnd < BLOCK_LAST) {
            endingContainerKey = highEnd;
            endingContainerEnd = lowEnd;
        } else {
            // we don't have an ending container.
            endingContainerKey = -1;
            endingContainerEnd = 0; // not used in this case.
        }

        if (initialContainerKey != -1) {
            // If we have an initial container block, and we have a pending container,
            // we need to see how they relate.
            if (pendingContainerKey != -1 && highBits(pendingContainerKey) == initialContainerKey) {
                if (pendingContainer == null) {
                    pendingContainer = containerForLowValueAndRange(
                        lowBitsAsInt(pendingContainerKey), initialContainerStart,
                        initialContainerEnd);
                    pendingContainerKey = highBits(pendingContainerKey);
                } else {
                    pendingContainer =
                        pendingContainer.iappend(initialContainerStart, initialContainerEnd + 1);
                }
                flushPendingContainer();
            } else {
                if (pendingContainerKey != -1) {
                    flushPendingContainer();
                }
                final Container initialContainer =
                    Container.rangeOfOnes(initialContainerStart, initialContainerEnd + 1);
                ensureRb();
                rb.appendContainerUnsafeNoWriteCheck(initialContainerKey, initialContainer);
            }
        }

        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }

        if (midFullBlockSpanKey != -1) {
            ensureRb();
            rb.appendFullBlockSpanUnsafeNoWriteCheck(midFullBlockSpanKey, midFullBlockSpanLen);
        }

        if (endingContainerKey != -1) {
            pendingContainerKey = endingContainerKey;
            pendingContainer = Container.rangeOfOnes(0, endingContainerEnd + 1);
        }
    }

    private void ensureRb() {
        if (rb == null) {
            rb = disposable ? new DisposableRspBitmap() : new RspBitmap();
        }
    }

    protected void flushPendingContainer() {
        ensureRb();
        if (pendingContainer != null) {
            pendingContainer = pendingContainer.runOptimize();
        }
        rb.appendContainerUnsafeNoWriteCheck(pendingContainerKey, pendingContainer);
        pendingContainerKey = -1;
        pendingContainer = null;
    }
}
