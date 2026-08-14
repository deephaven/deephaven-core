//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.DisposableRspBitmap;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.rsp.container.Container;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.*;
import static io.deephaven.engine.rowset.impl.rsp.RspBitmap.*;

public class RspBitmapBuilderSequential implements BuilderSequential {
    protected final boolean disposable;

    protected long pendingStart = -1;
    protected long pendingEnd = -1;
    protected long pendingContainerKey = -1;
    protected Container pendingContainer;
    protected RspBitmap rb;
    protected long maxKeyHint = -1;

    /**
     * The block that {@link #sizedBlockCardinality} and {@link #sizedBlockRangeCount} describe, or -1 when nothing is
     * known ahead of time. A subclass that can see what a block will receive before its container is filled should
     * publish it here, so the container can be created at its final size and in the representation that fits it best;
     * containers are otherwise grown one range at a time, which reallocates their backing storage on every append. See
     * {@link Container#emptySizedFor(int, int)}.
     *
     * <p>
     * These are only sizing hints. Stale or inaccurate values cost memory or reallocation, never correctness, and the
     * block key is checked before they are used.
     */
    private long sizedBlockKey = -1;
    private int sizedBlockCardinality;
    private int sizedBlockRangeCount;

    /**
     * Declare what {@code blockKey} will receive in total, for use when its container is created.
     */
    protected final void setSizedBlock(final long blockKey, final int cardinality, final int rangeCount) {
        sizedBlockKey = blockKey;
        sizedBlockCardinality = cardinality;
        sizedBlockRangeCount = rangeCount;
    }

    protected final void clearSizedBlock() {
        sizedBlockKey = -1;
    }

    public RspBitmapBuilderSequential() {
        this(false);
    }

    public RspBitmapBuilderSequential(final boolean disposable) {
        this.disposable = disposable;
    }

    @Override
    public void setDomain(final long minRowKey, final long maxRowKey) {
        maxKeyHint = (maxRowKey == RowSequence.NULL_ROW_KEY) ? -1 : maxRowKey;
    }

    @Override
    public OrderedLongSet getOrderedLongSet() {
        if (pendingStart != -1) {
            flushPendingRange();
        }
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }
        if (rb == null) {
            return OrderedLongSet.EMPTY;
        }
        rb.tryCompactUnsafe(4);
        rb.finishMutations();
        final RspBitmap ans = rb;
        rb = null;
        return ans;
    }

    @Override
    public void appendKey(final long rowKey) {
        if (pendingStart != -1) {
            if (check && rowKey <= pendingEnd) {
                throw new IllegalArgumentException(outOfOrderKeyErrorMsg +
                        "last=" + pendingEnd + " while appending value=" + rowKey);
            }
            if (pendingEnd + 1 == rowKey) {
                pendingEnd = rowKey;
                return;
            }
            flushPendingRange();
        }
        pendingStart = pendingEnd = rowKey;
    }

    @Override
    public void appendRange(final long rangeFirstRowKey, final long rangeLastRowKey) {
        if (RspArray.debug) {
            if (rangeFirstRowKey > rangeLastRowKey) {
                throw new IllegalArgumentException(
                        "start (= " + rangeFirstRowKey + ") > end (= " + rangeLastRowKey + ")");
            }
        }
        if (pendingStart != -1) {
            if (check && rangeFirstRowKey <= pendingEnd) {
                throw new IllegalArgumentException(outOfOrderKeyErrorMsg +
                        "last=" + pendingEnd + " while appending range start=" + rangeFirstRowKey + ", end="
                        + rangeLastRowKey);
            }
            if (pendingEnd + 1 == rangeFirstRowKey) {
                pendingEnd = rangeLastRowKey;
                return;
            }
            flushPendingRange();
        }
        pendingStart = rangeFirstRowKey;
        pendingEnd = rangeLastRowKey;
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
    public void appendOrderedRowKeysChunk(LongChunk<OrderedRowKeys> chunk, int offset, int length) {
        if (length == 0) {
            return;
        }

        if (rb != null) {
            appendKeyChunkRb(chunk, offset, length);
        } else {
            appendKeyChunk(chunk, offset, length);
        }
    }

    private void appendKeyChunkRb(LongChunk<OrderedRowKeys> chunk, int offset, int length) {
        // flush to the rb before appending
        if (pendingStart != -1) {
            flushPendingRange();
        }
        if (pendingContainerKey != -1) {
            flushPendingContainer();
        }

        // single key?
        if (length == 1) {
            rb.appendUnsafeNoWriteCheck(chunk.get(offset));
            return;
        }

        // single range?
        final int lastOffsetInclusive = offset + length - 1;
        final long first = chunk.get(offset);
        final long last = chunk.get(lastOffsetInclusive);
        if (last - first + 1 == length) {
            rb.appendRangeUnsafeNoWriteCheck(first, last);
            return;
        }

        rb.addValuesUnsafeNoWriteCheck(chunk, offset, length);
    }

    private void appendKeyChunk(LongChunk<OrderedRowKeys> chunk, int offset, int length) {
        // single key?
        if (length == 1) {
            appendKey(chunk.get(offset));
            return;
        }

        // single range?
        final int lastOffsetInclusive = offset + length - 1;
        final long first = chunk.get(offset);
        final long last = chunk.get(lastOffsetInclusive);
        if (last - first + 1 == length) {
            appendRange(first, last);
            return;
        }

        final LongChunkIterator it = new LongChunkIterator(chunk, offset, length);
        while (it.hasNext()) {
            appendKey(it.nextLong());
        }
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
                    pendingContainer = newContainerForLowValueAndRange(
                            highStart, lowBitsAsInt(pendingContainerKey), lowStart, lowEnd);
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
                pendingContainer = newContainerForRange(highStart, lowStart, lowEnd);
            }
            return;
        }

        //
        // A range may involve at most 3 spans, any of which may or may not be present:
        // * a block for an initial container.
        // * a full block span
        // * a block for a final container.
        // Note we must have at least two of these, given code above already handled the case for a single block range.
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
                            lowBitsAsInt(pendingContainerKey), initialContainerStart, initialContainerEnd);
                    pendingContainerKey = highBits(pendingContainerKey);
                } else {
                    pendingContainer = pendingContainer.iappend(initialContainerStart, initialContainerEnd + 1);
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
            pendingContainer = newContainerForRange(endingContainerKey, 0, endingContainerEnd);
        }
    }

    /**
     * Create the container that will accumulate {@code blockKey}, initially holding {@code [lowStart, lowEnd]}.
     *
     * <p>
     * When the block's final cardinality is known and exceeds this first range, the container is created at that size
     * so that the appends to follow do not have to grow it. Otherwise this range is all the block gets, and the compact
     * single-range representation is both smaller and free to build.
     */
    private Container newContainerForRange(final long blockKey, final int lowStart, final int lowEnd) {
        // Pre-size only when more is coming for this block than this first range; otherwise this range is all it gets,
        // and the compact single-range representation is both smaller and free to build.
        if (blockKey == sizedBlockKey && sizedBlockCardinality > lowEnd - lowStart + 1) {
            return Container.emptySizedFor(sizedBlockCardinality, sizedBlockRangeCount)
                    .iappend(lowStart, lowEnd + 1);
        }
        return Container.rangeOfOnes(lowStart, lowEnd + 1);
    }

    /**
     * As {@link #newContainerForRange}, for the case where a single value was already pending for {@code blockKey} and
     * is now joined by {@code [lowStart, lowEnd]}.
     */
    private Container newContainerForLowValueAndRange(
            final long blockKey, final int lowValue, final int lowStart, final int lowEnd) {
        if (blockKey == sizedBlockKey && sizedBlockCardinality > 1 + lowEnd - lowStart + 1) {
            return Container.emptySizedFor(sizedBlockCardinality, sizedBlockRangeCount)
                    .iappend(lowValue, lowValue + 1)
                    .iappend(lowStart, lowEnd + 1);
        }
        return containerForLowValueAndRange(lowValue, lowStart, lowEnd);
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
