package io.deephaven.db.v2.utils.rsp;

import static io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.*;
import org.apache.commons.lang3.mutable.MutableLong;

public class RspOrderedKeys extends OrderedKeysAsChunkImpl {
    private RspArray arr;
    private long firstKey; // cached first key value or -1 if cache has not been populated yet.
    private long lastKey; // cached last key value or -1 if cache has not been populated yet.
    private int startIdx; // span index on arr for where our view start.
    private int endIdx; // span index on arr for where our view ends (inclusive).
    private long startOffset; // position offset inside the start span where our view starts.
    private long endOffset; // position offset inside the end span where our view ends (inclusive).
    private long cardBeforeStartIdx; // total cardinality in spans before startIdx.
    private long cardBeforeEndIdx; // total cardinality in spans before endIdx.

    // Potentially useful for testing.
    private static RspArray wrapRspArray(final RspArray arr) {
        return arr; // Note no acquire.
    }

    RspOrderedKeys(
            final RspArray arr,
            final int startIdx, final long startOffset, final long cardBeforeStartIdx,
            final int endIdx, final long endOffset, final long cardBeforeEndIdx) {
        if (RspBitmap.debug) {
            if (endIdx < startIdx ||
                    (endIdx == startIdx && endOffset < startOffset)) {
                throw new IllegalArgumentException("Empty " + RspOrderedKeys.class.getSimpleName() + " :" +
                        "startIdx=" + startIdx + ", startOffset=" + startOffset +
                        ", endIdx=" + endIdx + ", endOffset=" + endOffset);
            }
        }
        arr.acquire();
        this.arr = wrapRspArray(arr);
        this.startIdx = startIdx;
        this.endIdx = endIdx;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.cardBeforeStartIdx = cardBeforeStartIdx;
        this.cardBeforeEndIdx = cardBeforeEndIdx;
        firstKey = -1;
        lastKey = -1;
    }

    @Override
    public void close() {
        closeRspOrderedKeys();
    }

    protected final void closeRspOrderedKeys() {
        if (arr == null) {
            return;
        }
        arr.release();
        arr = null;
        closeOrderedKeysAsChunkImpl();
    }

    private RspOrderedKeys(final RspArray arr) {
        this.arr = wrapRspArray(arr);
        startIdx = -1;
    }

    @Override
    public long firstKey() {
        if (firstKey == -1) {
            firstKey = arr.get(startIdx, startOffset);
        }
        return firstKey;
    }

    @Override
    public long lastKey() {
        if (lastKey == -1) {
            lastKey = arr.get(endIdx, endOffset);
        }
        return lastKey;
    }

    @Override
    public long getAverageRunLengthEstimate() {
        final long sz = size();
        if (sz < 32) {
            return 1; // don't bother.
        }
        long estimate = arr.getAverageRunLengthEstimate(startIdx, endIdx);
        if (estimate > sz) {
            // Given how arr.getAverageRunLengthEstimate works, we know that
            // there are relatively big runs (compared to us) runs around us
            // (note how estimate is not considering our offsets).
            // An alternative to the code below would be to just return sz,
            // but we want to avoid suggesting we are a single run when we don't know
            // for sure.
            return Math.max(sz / 2, 1);
        }
        return estimate;
    }

    @Override
    public long rangesCountUpperBound() {
        return arr.rangesCountUpperBound(startIdx, endIdx);
    }

    public RspOrderedKeys copy(final RspOrderedKeys other) {
        return new RspOrderedKeys(
                other.arr,
                other.startIdx, other.startOffset, other.cardBeforeStartIdx,
                other.endIdx, other.endOffset, other.cardBeforeEndIdx);
    }

    // For object reuse in order to avoid allocations.
    private void reset(final int startIdx, final long startOffset, final long cardBeforeStartIdx,
            final int endIdx, final long endOffset, final long cardBeforeEndIdx,
            final long firstKey) {
        this.startIdx = startIdx;
        this.endIdx = endIdx;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.cardBeforeStartIdx = cardBeforeStartIdx;
        this.cardBeforeEndIdx = cardBeforeEndIdx;
        this.firstKey = firstKey;
        closeOrderedKeysAsChunkImpl();
        lastKey = -1;
    }

    @Override
    public Iterator getOrderedKeysIterator() {
        return new Iterator(this);
    }

    @Override
    public OrderedKeys getOrderedKeysByPosition(long startPositionInclusive, long length) {
        final long absoluteStart = startPositionInclusive + absoluteStartPos();
        if (absoluteStart > absoluteEndPos()) {
            return OrderedKeys.EMPTY;
        }
        final long sizeLeftFromStart = size() - startPositionInclusive;
        return arr.getOrderedKeysByPosition(absoluteStart, Math.min(sizeLeftFromStart, length));
    }

    @Override
    public OrderedKeys getOrderedKeysByKeyRange(long startKeyInclusive, long endKeyInclusive) {
        return arr.getOrderedKeysByKeyRangeConstrainedToIndexAndOffsetRange(startKeyInclusive, endKeyInclusive,
                startIdx, startOffset, cardBeforeStartIdx, endIdx, endOffset);
    }

    @Override
    public Index asIndex() {
        final RspBitmap newArr = new RspBitmap(arr, startIdx, startOffset, endIdx, endOffset);
        return new TreeIndex(newArr);
    }

    @Override
    public void fillKeyIndicesChunk(final WritableLongChunk<? extends KeyIndices> chunkToFill) {
        final RspIterator it = new RspIterator(new RspArray.SpanCursorForwardImpl(arr, startIdx), startOffset);
        int n = it.copyTo(chunkToFill, 0, intSize());
        chunkToFill.setSize(n);
    }

    @Override
    public void fillKeyRangesChunk(final WritableLongChunk<OrderedKeyRanges> chunkToFill) {
        chunkToFill.setSize(0);
        final RspRangeBatchIterator it =
                new RspRangeBatchIterator(new RspArray.SpanCursorForwardImpl(arr, startIdx), startOffset, size());
        int nRanges = 0;
        while (it.hasNext()) {
            final int n = it.fillRangeChunk(chunkToFill, 2 * nRanges);
            nRanges += n;
        }
        chunkToFill.setSize(2 * nRanges);
    }

    private long absoluteStartPos() {
        return cardBeforeStartIdx + startOffset;
    }

    private long absoluteEndPos() {
        return cardBeforeEndIdx + endOffset;
    }

    @Override
    public long size() {
        return absoluteEndPos() - absoluteStartPos() + 1;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean forEachLong(final LongAbortableConsumer lac) {
        if (startIdx == endIdx) {
            return arr.forEachLongInSpanWithOffsetAndMaxCount(startIdx, startOffset, lac, size());
        }
        if (!arr.forEachLongInSpanWithOffset(startIdx, startOffset, lac)) {
            return false;
        }
        for (int i = startIdx + 1; i <= endIdx - 1; ++i) {
            if (!arr.forEachLongInSpan(i, lac)) {
                return false;
            }
        }
        if (!arr.forEachLongInSpanWithMaxCount(endIdx, lac, endOffset + 1)) {
            return false;
        }
        return true;
    }

    @Override
    public boolean forEachLongRange(final LongRangeAbortableConsumer lrac) {
        if (startIdx == endIdx) {
            final long remaining = endOffset - startOffset + 1;
            return arr.forEachLongRangeInSpanWithOffsetAndMaxCardinality(startIdx, startOffset, remaining, lrac);
        }

        final long[] pendingRange = new long[2];
        final LongRangeAbortableConsumer wrapper = RspArray.makeAdjacentRangesCollapsingWrapper(pendingRange, lrac);
        if (!arr.forEachLongRangeInSpanWithOffset(startIdx, startOffset, wrapper)) {
            return false;
        }
        for (int i = startIdx + 1; i < endIdx; ++i) {
            if (!arr.forEachLongRangeInSpanWithOffset(i, 0, wrapper)) {
                return false;
            }
        }
        final long remaining = endOffset + 1;
        if (!arr.forEachLongRangeInSpanWithOffsetAndMaxCardinality(endIdx, 0, remaining, wrapper)) {
            return false;
        }
        if (pendingRange[0] != -2) {
            return lrac.accept(pendingRange[0], pendingRange[1]);
        }
        return true;
    }

    // Note unlike Index.Iterator, this Iterator will /not/ automatically release its underlying Index representation
    // when iteration is exhausted. The API for OK.Iterator makes that impossible.
    static class Iterator implements OrderedKeys.Iterator {
        private static class OKWrapper extends RspOrderedKeys {
            OKWrapper(final RspArray arr) {
                super(arr);
            }

            @Override
            public void close() {
                if (RspArray.debug) {
                    throw new IllegalStateException();
                }
                // We purposely /do not/ close the RspOrderedKeys part as it will get reused.
                // The API doc for Iterator states that clients should /never/ call close. So that we eneded up here
                // means
                // there is some kind of bug.
                closeOrderedKeysAsChunkImpl();
            }
        }

        private RspArray arr;

        // Internal buffer used as a return value for next... methods. Note this object and the
        // position reflected by curr{Start,End}{Idx,Offset} may not be in sync if there
        // were any intermediate calls to advance.
        private final RspOrderedKeys currBuf;

        // The following four fields match the last values
        // returned for getNext* methods; when we have not returned any yet,
        // currStart* matches the start of the OrderedKeys from which we were constructed,
        // and currEndIdx == -1 to signal the iterator has not been started yet.
        private int currStartIdx;
        private int currEndIdx;
        private long currStartOffset;
        private long currEndOffset;
        private long currCardBeforeStartIdx;
        private long currCardBeforeEndIdx;

        private long sizeLeft; // number of keys left for subsequent calls to next* methods.
        private final int oksEndIdx;
        private final long oksEndOffset;

        // cached value for the first key on the call to any getNext* method, or -1 if cache has not been populated yet.
        private long nextKey;


        public Iterator(final RspOrderedKeys oks) {
            oks.arr.acquire();
            this.arr = oks.arr;
            sizeLeft = oks.size();
            currStartIdx = oks.startIdx;
            currStartOffset = oks.startOffset;
            currCardBeforeStartIdx = oks.cardBeforeStartIdx;
            oksEndIdx = oks.endIdx;
            oksEndOffset = oks.endOffset;
            currEndIdx = -1;
            currEndOffset = -1;
            currCardBeforeEndIdx = -1;
            currBuf = new OKWrapper(arr);
            nextKey = -1;
        }

        @Override
        public void close() {
            currBuf.closeOrderedKeysAsChunkImpl();
            if (arr == null) {
                return;
            }
            arr.release();
            arr = null;
        }

        @Override
        public boolean hasMore() {
            return sizeLeft > 0;
        }

        @Override
        public long peekNextKey() {
            if (sizeLeft <= 0) {
                return -1;
            }
            if (nextKey == -1) {
                final int nextStartIdx;
                final long nextStartOffset;
                if (currEndIdx == -1) {
                    nextStartIdx = currStartIdx;
                    nextStartOffset = currStartOffset;
                } else {
                    final long spanCardinalityAtCurrEndIdx = arr.getSpanCardinalityAtIndexMaybeAcc(currEndIdx);
                    if (currEndOffset + 1 < spanCardinalityAtCurrEndIdx) {
                        nextStartIdx = currEndIdx;
                        nextStartOffset = currEndOffset + 1;
                    } else {
                        nextStartIdx = currEndIdx + 1;
                        nextStartOffset = 0;
                    }
                }
                nextKey = arr.get(nextStartIdx, nextStartOffset);
            }
            return nextKey;
        }

        @Override
        public OrderedKeys getNextOrderedKeysThrough(final long maxKey) {
            if (maxKey < 0) {
                return OrderedKeys.EMPTY;
            }
            final long firstKey = nextKey;
            if (!updateCurrThrough(maxKey)) {
                return OrderedKeys.EMPTY;
            }
            currBuf.reset(
                    currStartIdx, currStartOffset, currCardBeforeStartIdx,
                    currEndIdx, currEndOffset, currCardBeforeEndIdx,
                    firstKey);
            return currBuf;
        }

        private int endIndex(
                final int fromIndex, final long fromOffset, final long cardBeforeIndex,
                final long deltaNumberOfKeys, final MutableLong prevCardMu) {
            final long cardTarget = cardBeforeIndex + fromOffset + deltaNumberOfKeys;
            if (prevCardMu == null) {
                int j = RspArray.unsignedBinarySearch(idx -> arr.acc[idx], fromIndex, arr.size, cardTarget);
                if (j < 0) {
                    j = -j - 1;
                    if (j == arr.size) {
                        --j;
                    }
                }
                return j;
            }

            long prevCard = cardBeforeIndex;
            int i = fromIndex;
            while (true) {
                if (i == arr.size - 1) {
                    prevCardMu.setValue(prevCard);
                    return i;
                }
                final long spanCard = arr.getSpanCardinalityAtIndex(i);
                final long card = prevCard + spanCard;
                if (cardTarget <= card) {
                    prevCardMu.setValue(prevCard);
                    return i;
                }
                ++i;
                prevCard = card;
            }
        }

        @Override
        public OrderedKeys getNextOrderedKeysWithLength(final long desiredNumberOfKeys) {
            final long firstKey = nextKey;
            final long actualNumberOfKeys = nextOrderedKeysWithLength(desiredNumberOfKeys);
            if (actualNumberOfKeys == 0) {
                return OrderedKeys.EMPTY;
            }
            sizeLeft -= actualNumberOfKeys;
            currBuf.reset(
                    currStartIdx, currStartOffset, currCardBeforeStartIdx,
                    currEndIdx, currEndOffset, currCardBeforeEndIdx,
                    firstKey);
            nextKey = -1;
            return currBuf;
        }

        private long nextOrderedKeysWithLength(final long desiredNumberOfKeys) {
            final long boundedNumberOfKeys = Math.min(desiredNumberOfKeys, sizeLeft);
            if (boundedNumberOfKeys <= 0) {
                return 0;
            }
            final MutableLong prevCardMu = (arr.acc == null) ? new MutableLong() : null;
            if (currEndIdx == -1) {
                currEndIdx = endIndex(currStartIdx, currStartOffset, currCardBeforeStartIdx, boundedNumberOfKeys,
                        prevCardMu);
                if (currEndIdx == currStartIdx) {
                    currCardBeforeEndIdx = currCardBeforeStartIdx;
                    currEndOffset = currStartOffset + boundedNumberOfKeys - 1;
                } else {
                    currCardBeforeEndIdx =
                            (prevCardMu != null) ? prevCardMu.longValue() : arr.cardinalityBeforeWithAcc(currEndIdx);
                    final long spanCardAtStartIdx = arr.getSpanCardinalityAtIndexMaybeAcc(currStartIdx);
                    final long cardAtStartIdx = currCardBeforeStartIdx + spanCardAtStartIdx;
                    final long firstSpanCount = spanCardAtStartIdx - currStartOffset;
                    final long deltaCount = currCardBeforeEndIdx - cardAtStartIdx;
                    final long remainingForEndSpan = boundedNumberOfKeys - firstSpanCount - deltaCount;
                    currEndOffset = remainingForEndSpan - 1;
                }
                return boundedNumberOfKeys;
            }
            final long spanCardinality = arr.getSpanCardinalityAtIndexMaybeAcc(currEndIdx);
            final long currStartIdxSpanCardinality;
            final long keysAvailableInStartSpan;
            if (currEndOffset + 1 < spanCardinality) {
                currStartIdx = currEndIdx;
                currStartOffset = currEndOffset + 1;
                currCardBeforeStartIdx = currCardBeforeEndIdx;
                currStartIdxSpanCardinality = spanCardinality;
                keysAvailableInStartSpan = spanCardinality - currStartOffset;
            } else {
                // currEndIdx + 1 < arr.size, otherwise we would have returned on the bounderNumberOfKeys <= 0 check.
                if (RspArray.debug) {
                    Assert.lt(currEndIdx + 1, "currEndIdx + 1",
                            arr.size, "arr.size");
                }
                currStartIdx = currEndIdx + 1;
                currStartOffset = 0;
                currCardBeforeStartIdx = currCardBeforeEndIdx + spanCardinality;
                keysAvailableInStartSpan =
                        currStartIdxSpanCardinality = arr.getSpanCardinalityAtIndexMaybeAcc(currStartIdx);
            }
            if (keysAvailableInStartSpan >= boundedNumberOfKeys) {
                currEndIdx = currStartIdx;
                currEndOffset = currStartOffset + boundedNumberOfKeys - 1;
                currCardBeforeEndIdx = currCardBeforeStartIdx;
                return boundedNumberOfKeys;
            }
            currEndIdx =
                    endIndex(currStartIdx, currStartOffset, currCardBeforeStartIdx, boundedNumberOfKeys, prevCardMu);
            currCardBeforeEndIdx =
                    (prevCardMu != null) ? prevCardMu.longValue() : arr.cardinalityBeforeWithAcc(currEndIdx);
            final long keysBeforeLastSpan =
                    keysAvailableInStartSpan + currCardBeforeEndIdx - currCardBeforeStartIdx
                            - currStartIdxSpanCardinality;
            final long keysLeftInLastSpan = boundedNumberOfKeys - keysBeforeLastSpan;
            if (keysLeftInLastSpan <= 0) {
                throw new IllegalStateException("Internal error");
            }
            currEndOffset = keysLeftInLastSpan - 1;
            return boundedNumberOfKeys;
        }

        @Override
        public boolean advance(final long toKey) {
            if (sizeLeft <= 0) {
                return false;
            }
            final int savedStartIdx = currStartIdx;
            final long savedStartOffset = currStartOffset;
            final boolean found = arr.findOrNext(currStartIdx, oksEndIdx + 1, toKey,
                    (final int index, final long offset) -> {
                        currStartIdx = index;
                        currStartOffset = offset;
                    });
            final boolean revert;
            if (!found) {
                revert = true;
            } else if (currEndIdx == -1) {
                revert = savedStartIdx == currStartIdx && currStartOffset < savedStartOffset;
            } else {
                revert = currEndIdx == currStartIdx && currStartOffset < currEndOffset;
            }
            if (revert) {
                currStartIdx = savedStartIdx;
                currStartOffset = savedStartOffset;
                return true;
            }
            final long cardinalityUpAndIncludingPreviousEnd;
            if (currEndIdx == -1) {
                cardinalityUpAndIncludingPreviousEnd = currCardBeforeStartIdx + savedStartOffset;
            } else {
                cardinalityUpAndIncludingPreviousEnd = currCardBeforeEndIdx + currEndOffset + 1;
            }
            currCardBeforeStartIdx = arr.cardinalityBeforeMaybeAcc(currStartIdx, savedStartIdx, currCardBeforeStartIdx);
            currEndIdx = -1;
            currEndOffset = -1;
            currCardBeforeEndIdx = -1;
            final long cardinalityBeforeStart = currCardBeforeStartIdx + currStartOffset;
            sizeLeft -= cardinalityBeforeStart - cardinalityUpAndIncludingPreviousEnd;
            if (sizeLeft <= 0) {
                sizeLeft = 0;
                nextKey = -1;
                return false;
            }
            nextKey = -1;
            return true;
        }

        // Updates curr{Start,End}{Idx,Offset} to a range starting on the position right after the end
        // at the time of the call, to the last position not greater than toKey.
        private boolean updateCurrThrough(final long toKey) {
            if (sizeLeft <= 0) {
                return false;
            }
            final int savedEndIdx = currEndIdx;
            final long savedEndOffset = currEndOffset;
            final int savedStartIdx = currStartIdx;
            final long savedStartOffset = currStartOffset;
            if (currEndIdx != -1) {
                final long spanCardinality = arr.getSpanCardinalityAtIndexMaybeAcc(currEndIdx);
                if (currEndOffset + 1 < spanCardinality) {
                    currStartIdx = currEndIdx;
                    currStartOffset = currEndOffset + 1;
                } else {
                    // currEndIdx + 1 < arr.size, otherwise we would have returned on the sizeLeft <= 0 check.
                    if (RspArray.debug) {
                        Assert.lt(currEndIdx + 1, "currEndIdx + 1",
                                arr.size, "arr.size");
                    }
                    currStartIdx = currEndIdx + 1;
                    currStartOffset = 0;
                }
            }
            final boolean found = arr.findOrPrev(currStartIdx, oksEndIdx + 1, toKey,
                    (final int index, final long offset) -> {
                        currEndIdx = index;
                        currEndOffset = offset;
                    });
            if (!found || (currEndIdx == currStartIdx && currEndOffset < currStartOffset)) {
                currStartIdx = savedStartIdx;
                currStartOffset = savedStartOffset;
                currEndIdx = savedEndIdx;
                currEndOffset = savedEndOffset;
                return false;
            }
            if (currEndIdx == oksEndIdx && currEndOffset > oksEndOffset) {
                currEndOffset = oksEndOffset;
            }
            if (savedEndIdx != -1) {
                currCardBeforeStartIdx = arr.cardinalityBeforeMaybeAcc(currStartIdx, savedEndIdx, currCardBeforeEndIdx);
            }
            currCardBeforeEndIdx = arr.cardinalityBeforeMaybeAcc(currEndIdx, currStartIdx, currCardBeforeStartIdx);
            final long cardinalityBeforeStart = currCardBeforeStartIdx + currStartOffset;
            final long cardinalityBeforeEnd = currCardBeforeEndIdx + currEndOffset;
            sizeLeft -= cardinalityBeforeEnd - cardinalityBeforeStart + 1;
            nextKey = -1;
            return true;
        }

        @Override
        public long getRelativePosition() {
            return -sizeLeft;
        }
    }
}
