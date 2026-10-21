//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.util.LongChunkIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.rsp.DisposableRspBitmap;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.rowset.impl.rsp.RspBitmap;
import io.deephaven.engine.rowset.impl.rsp.container.Container;
import io.deephaven.engine.rowset.impl.sortedranges.SortedRanges;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;

import static io.deephaven.engine.rowset.impl.rsp.RspArray.*;
import static io.deephaven.engine.rowset.impl.rsp.RspBitmap.*;

public class RspBitmapBuilderSequential implements BuilderSequential {
    protected final boolean disposable;

    protected long pendingStart = -1;
    protected long pendingEnd = -1;
    protected long pendingContainerKey = -1;
    protected Container pendingContainer;

    /**
     * Whether {@link #pendingContainer} was taken back out of {@link #rb}, rather than started for a block rb did not
     * yet have.
     */
    private boolean pendingContainerReopened;
    protected RspBitmap rb;
    protected long maxKeyHint = -1;

    /**
     * The greatest key moved out of the pending range, into {@link #pendingContainer} or {@link #rb}, or -1 when it is
     * not known. Order checks against {@link #rb} use this rather than {@code rb.lastValue()}, which has to scan a
     * bitmap container backward over the words past its last value.
     */
    protected long lastFlushedKey = -1;

    private boolean built;

    /**
     * Builders are single use: a second build fails instead. Only the build methods check this; appends stay unchecked
     * to keep the hot path free of conditionals, so the effect of appending after a build is undefined rather than
     * detected.
     */
    protected final void checkAndMarkBuilt() {
        if (built) {
            throw new IllegalStateException("Builder was already used to build a result; builders are single use");
        }
        built = true;
    }

    /**
     * The block that {@link #sizedBlockCardinality} and {@link #sizedBlockRangeCount} describe, or -1 when nothing is
     * known ahead of time. When what a block will receive is known before its container is filled, it is published
     * here, so the container can be created at its final size and in the representation that fits it best; containers
     * are otherwise grown one range at a time, reallocating their backing storage as they fill. See
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
    private void setSizedBlock(final long blockKey, final int cardinality, final int rangeCount) {
        sizedBlockKey = blockKey;
        sizedBlockCardinality = cardinality;
        sizedBlockRangeCount = rangeCount;
    }

    private void clearSizedBlock() {
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
        checkAndMarkBuilt();
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
        } else {
            // Only the first key needs this check; every later one is order checked against a nonnegative key.
            Assert.geqZero(rowKey, "rowKey");
        }
        pendingStart = pendingEnd = rowKey;
    }

    @Override
    public void appendRange(final long rangeFirstRowKey, final long rangeLastRowKey) {
        // Checked on every call: the order check below looks only at the start of the range. Together these also keep
        // the end nonnegative.
        Assert.geqZero(rangeFirstRowKey, "rangeFirstRowKey");
        Assert.leq(rangeFirstRowKey, "rangeFirstRowKey", rangeLastRowKey, "rangeLastRowKey");
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
    public void appendOrderedLongSet(final long shiftAmount, final OrderedLongSet ix) {
        if (ix.ixIsEmpty()) {
            return;
        }
        // A negative shift can move the first key below zero; a positive one can carry the last past Long.MAX_VALUE.
        // For a bitmap, the bounds of its first and last blocks usually settle that; its exact first or last key may
        // require scanning a bitmap container.
        if (shiftAmount < 0) {
            final long firstKeyBound = ix instanceof RspBitmap ? ((RspBitmap) ix).keyForFirstBlock() : ix.ixFirstKey();
            if (firstKeyBound + shiftAmount < 0) {
                Assert.geqZero(ix.ixFirstKey() + shiftAmount, "ix.ixFirstKey() + shiftAmount");
            }
        } else if (shiftAmount > 0) {
            final long lastKeyBound =
                    ix instanceof RspBitmap ? ((RspBitmap) ix).keyForLastBlock() + BLOCK_LAST : ix.ixLastKey();
            if (lastKeyBound + shiftAmount < 0) {
                Assert.geqZero(ix.ixLastKey() + shiftAmount, "ix.ixLastKey() + shiftAmount");
            }
        }
        // A bitmap's spans can be shared when the shift keeps them block aligned, from the first one that starts in a
        // block after everything we hold.
        final RspBitmap ixRb = ix instanceof RspBitmap && (shiftAmount & BLOCK_LAST) == 0 ? (RspBitmap) ix : null;
        if (rb == null && (ixRb == null || !commitToBitmapFor(ixRb))) {
            ix.ixForEachLongRange((final long start, final long end) -> {
                appendRange(start + shiftAmount, end + shiftAmount);
                return true;
            });
            return;
        }
        boolean shareAfterFirstSpan = false;
        if (ixRb != null) {
            final long heldLastBlockKey;
            if (pendingStart != -1) {
                heldLastBlockKey = highBits(pendingEnd);
            } else if (pendingContainerKey != -1) {
                heldLastBlockKey = highBits(pendingContainerKey);
            } else {
                // With nothing pending, rb is empty only when it was just created for ix, and we hold nothing.
                heldLastBlockKey = rb.isEmpty() ? -1 : rb.keyForLastBlock();
            }
            final long ixFirstBlockKey = ixRb.keyForFirstBlock() + shiftAmount;
            if (ixFirstBlockKey > heldLastBlockKey) {
                if (pendingStart != -1) {
                    flushPendingRange();
                }
                if (pendingContainerKey != -1) {
                    flushPendingContainer();
                }
                if (rb.tryAppendShiftedUnsafeNoWriteCheck(shiftAmount, ixRb)) {
                    // rb now ends with ix's last span; its last key is looked up only if an order check needs it.
                    lastFlushedKey = -1;
                    return;
                }
            }
            shareAfterFirstSpan = ixFirstBlockKey == heldLastBlockKey;
        }
        if (pendingStart != -1) {
            flushPendingRange();
        }
        // ix's ranges are ordered and disjoint, so they need checking only against the keys we already hold; comparing
        // every one with that same bound is as cheap as tracking which one is first. Past that check they go straight
        // to the containers.
        final long heldLastKey = check ? (lastFlushedKey != -1 ? lastFlushedKey : rb.lastValue()) : -1;
        final LongRangeAbortableConsumer appendAfterHeld = (final long start, final long end) -> {
            final long shiftedStart = start + shiftAmount;
            if (shiftedStart <= heldLastKey) {
                throw new IllegalArgumentException(outOfOrderKeyErrorMsg +
                        "last=" + heldLastKey + " while appending value=" + shiftedStart);
            }
            flushRangeToPendingContainer(shiftedStart, end + shiftAmount);
            return true;
        };
        if (!shareAfterFirstSpan) {
            ix.ixForEachLongRange(appendAfterHeld);
            return;
        }
        // Only ix's first span starts in the block we end with. Its ranges go in one at a time; every later span
        // starts in a later block, so those are shared.
        ixRb.forEachLongRangeInSpanWithOffset(0, 0, appendAfterHeld);
        if (ixRb.keyForLastBlock() + shiftAmount == highBits(lastFlushedKey)) {
            // ix has no later span. Its last block stays pending for whatever is appended next.
            return;
        }
        // ix's first span lies in a block we already hold, so its ranges went into the pending container: either the
        // one already open, or rb's last span taken back out. A full block span cannot start in such a block without
        // overlapping a key we hold.
        Assert.neq(pendingContainerKey, "pendingContainerKey", -1);
        flushPendingContainer();
        Assert.eqTrue(rb.tryAppendShiftedUnsafeNoWriteCheck(shiftAmount, ixRb, 1),
                "rb.tryAppendShiftedUnsafeNoWriteCheck(shiftAmount, ixRb, 1)");
        // As above, rb now ends with ix's last span.
        lastFlushedKey = -1;
    }

    /**
     * Called when {@code ix}, a bitmap whose spans could be shared, is appended before {@link #rb} exists: create rb
     * now, so that they are, or leave it and have {@code ix} appended a range at a time.
     *
     * <p>
     * This builder's result is a bitmap whatever it holds, so it always creates rb. A builder that can produce another
     * representation decides whether giving that up pays.
     *
     * @return Whether rb now exists
     */
    protected boolean commitToBitmapFor(final RspBitmap ix) {
        ensureRb();
        return true;
    }

    @Override
    public void appendOrderedRowKeysChunk(LongChunk<OrderedRowKeys> chunk, int offset, int length) {
        if (length == 0) {
            return;
        }
        // The keys are ordered, so the first one is the least, and checking it covers the whole chunk.
        Assert.geqZero(chunk.get(offset), "chunk.get(offset)");

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
            // The chunk may add to the pending container's block, and later ranges may take the block back out as the
            // pending container, so it is not optimized on every such round trip. Moving on to a later block
            // optimizes it.
            movePendingContainerToRb();
        }

        // The keys are ordered, so the first one is the least and the last one the greatest.
        final long first = chunk.get(offset);
        if (check) {
            final long heldLastKey = lastFlushedKey != -1 ? lastFlushedKey : rb.lastValue();
            if (first <= heldLastKey) {
                throw new IllegalArgumentException(outOfOrderKeyErrorMsg +
                        "last=" + heldLastKey + " while appending value=" + first);
            }
        }
        final int lastOffsetInclusive = offset + length - 1;
        final long last = chunk.get(lastOffsetInclusive);
        lastFlushedKey = last;

        // single key?
        if (length == 1) {
            rb.appendUnsafeNoWriteCheck(last);
            return;
        }

        // single range?
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
        final long lastFlushedKeyOnEntry = lastFlushedKey;
        // A pending container is only ever filled from here, so lastFlushedKey is its last key. Ranges reach this
        // point unchecked when nothing was pending in the range, and the container appends below do not check order.
        if (check && pendingContainerKey != -1 && start <= lastFlushedKeyOnEntry) {
            throw new IllegalStateException(outOfOrderKeyErrorMsg +
                    "last=" + lastFlushedKeyOnEntry + " while appending value=" + start);
        }
        // Every path below leaves end as the greatest key held outside the pending range.
        lastFlushedKey = end;
        final long highStart = highBits(start);
        final int lowStart = RspArray.lowBitsAsInt(start);
        final long highEnd = highBits(end);
        final int lowEnd = RspArray.lowBitsAsInt(end);
        final boolean singleBlock = highStart == highEnd;
        if (pendingContainerKey == -1 && rb != null && !rb.isEmpty()) {
            // A key chunk or a set appended in bitmap mode can go straight into rb and leave nothing pending, so the
            // block this range starts in may be the one rb already ends with. Appending a container to rb for that
            // block would give the block a second span. Instead the block's span is taken back out of rb to be the
            // pending container, extended like any other, and optimized when it is flushed back.
            final long lastBlockKey = rb.keyForLastBlock();
            if (lastBlockKey >= highStart) {
                if (check) {
                    final long rbLastKey = lastFlushedKeyOnEntry != -1 ? lastFlushedKeyOnEntry : rb.lastValue();
                    if (lastBlockKey > highStart || start <= rbLastKey) {
                        throw new IllegalStateException(outOfOrderKeyErrorMsg +
                                "last=" + rbLastKey + " while appending value=" + start);
                    }
                }
                final Container lastSpan = lastBlockKey == highStart ? rb.removeLastSpanUnsafeNoWriteCheck() : null;
                if (lastSpan == null) {
                    // rb ends with a full block span, or, when order is not checked, past this range's start; rb is
                    // extended in place.
                    if (singleBlock) {
                        rb.appendRangeUnsafeNoWriteCheck(start, end);
                        return;
                    }
                    rb.appendRangeUnsafeNoWriteCheck(start, highStart | BLOCK_LAST);
                    flushRangeToPendingContainer(highStart + BLOCK_SIZE, end);
                    return;
                }
                pendingContainerKey = lastBlockKey;
                pendingContainer = lastSpan;
                pendingContainerReopened = true;
            }
        }
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
     * Append the ranges of {@code sr}, which must all come after every key this builder holds.
     *
     * <p>
     * Every range is already known, so rather than growing each container one range at a time -- which reallocates its
     * backing storage as it fills, and may pass through representations it does not end in -- we walk two cursors over
     * the ranges. One feeds ranges to the builder; the other runs ahead to the end of the block being started, so the
     * container for it can be created at its final size and in the representation that suits it.
     */
    protected final void appendSortedRangesSizingBlocks(final SortedRanges sr) {
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

    /**
     * An {@link RspBitmap} holding the keys of {@code sr}, with each container created at its final size and in the
     * representation that suits it.
     */
    public static RspBitmap buildRsp(final SortedRanges sr) {
        final RspBitmapBuilderSequential builder = new RspBitmapBuilderSequential();
        builder.appendSortedRangesSizingBlocks(sr);
        final OrderedLongSet result = builder.getOrderedLongSet();
        return result instanceof RspBitmap ? (RspBitmap) result : new RspBitmap();
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
        if (pendingContainer != null) {
            pendingContainer = pendingContainer.runOptimize();
        }
        movePendingContainerToRb();
    }

    private void movePendingContainerToRb() {
        ensureRb();
        if (pendingContainerReopened) {
            // Appending would optimize the span before it again, which costs a pass over that span on every round
            // trip through the pending container.
            rb.restoreLastSpanUnsafeNoWriteCheck(pendingContainerKey, pendingContainer);
            pendingContainerReopened = false;
        } else {
            rb.appendContainerUnsafeNoWriteCheck(pendingContainerKey, pendingContainer);
        }
        pendingContainerKey = -1;
        pendingContainer = null;
    }
}
