/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.exceptions.SizeException;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.OrderedKeyRanges;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.OrderedChunkUtils;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;

public class OrderedKeysKeyRangesChunkImpl implements OrderedKeys {

    private final long minKeyValue; // artificial min
    private final long maxKeyValue; // artificial max
    private final LongChunk<OrderedKeyRanges> backingChunk;
    private WritableLongChunk<OrderedKeyRanges> toReleaseChunk;
    private WritableLongChunk<OrderedKeyIndices> asKeyIndicesChunk;
    private WritableLongChunk<OrderedKeyRanges> asKeyRangesChunk;

    // Calculating size is O(n), so once computed let's cache the result.
    private long cachedSize = 0;

    private OrderedKeysKeyRangesChunkImpl(final LongChunk<OrderedKeyRanges> backingChunk,
            final WritableLongChunk<OrderedKeyRanges> toReleaseChunk) {
        this.minKeyValue = 0;
        this.maxKeyValue = Long.MAX_VALUE;
        this.backingChunk = backingChunk;
        this.toReleaseChunk = toReleaseChunk;

        if (backingChunk.size() % 2 != 0) {
            throw new IllegalArgumentException("the backingChunk.size() must be a multiple of two ("
                    + backingChunk.size() + " % 2 != 0)");
        }
    }

    private OrderedKeysKeyRangesChunkImpl(final WritableLongChunk<OrderedKeyRanges> backingChunkToOwn) {
        this(backingChunkToOwn, backingChunkToOwn);
    }

    static OrderedKeysKeyRangesChunkImpl makeByTaking(final WritableLongChunk<OrderedKeyRanges> backingChunkToOwn) {
        return new OrderedKeysKeyRangesChunkImpl(backingChunkToOwn);
    }

    private OrderedKeysKeyRangesChunkImpl(final LongChunk<OrderedKeyRanges> backingChunk) {
        this(backingChunk, null);
    }

    static OrderedKeysKeyRangesChunkImpl makeByWrapping(final LongChunk<OrderedKeyRanges> backingChunk) {
        return new OrderedKeysKeyRangesChunkImpl(backingChunk);
    }

    private OrderedKeysKeyRangesChunkImpl(final LongChunk<OrderedKeyRanges> backingChunk,
            final WritableLongChunk<OrderedKeyRanges> toReleaseChunk,
            final long minKeyValue,
            final long maxKeyValue) {
        this.minKeyValue = minKeyValue;
        this.maxKeyValue = maxKeyValue;
        this.backingChunk = backingChunk;
        this.toReleaseChunk = toReleaseChunk;

        if (backingChunk.size() % 2 != 0) {
            throw new IllegalArgumentException("the backingChunk.size() must be a multiple of two ("
                    + backingChunk.size() + " % 2 != 0)");
        }

        if (backingChunk.size() > 0) {
            if (this.minKeyValue > backingChunk.get(1)) {
                throw new IllegalArgumentException("minKeyValue is only allowed to apply to first range in chunk ("
                        + this.minKeyValue + " is > " + backingChunk.get(1) + ")");
            }
            if (this.maxKeyValue < backingChunk.get(backingChunk.size() - 2)) {
                throw new IllegalArgumentException("maxKeyValue is only allowed to apply to last range in chunk ("
                        + this.maxKeyValue + " is < " + backingChunk.get(backingChunk.size() - 2) + ")");
            }
        }
    }

    private OrderedKeysKeyRangesChunkImpl(final LongChunk<OrderedKeyRanges> backingChunk,
            final long minKeyValue,
            final long maxKeyValue) {
        this(backingChunk, null, minKeyValue, maxKeyValue);

    }

    private OrderedKeysKeyRangesChunkImpl(final WritableLongChunk<OrderedKeyRanges> backingChunkToOwn,
            final long minKeyValue,
            final long maxKeyValue) {
        this(backingChunkToOwn, backingChunkToOwn, minKeyValue, maxKeyValue);

    }

    private class OffsetHelper {
        public int offset = 0;
        public long currKeyValue = Math.max(backingChunk.get(offset), minKeyValue);

        /**
         * Advances {@code offset} and {@code currKeyValue} to the new values after skipping {@code numberOfKeys} items.
         * 
         * @param numberOfKeys the number of items to skip
         * @return true iff we haven't fallen off the end of the container
         */
        boolean advanceInPositionSpace(long numberOfKeys) {
            for (int idx = offset; idx + 1 < backingChunk.size(); idx += 2) {
                final long start = Math.max(currKeyValue, backingChunk.get(idx));
                final long range = Math.min(maxKeyValue, backingChunk.get(idx + 1)) - start + 1;
                final boolean overflowed = range < 0;
                if (range > numberOfKeys || overflowed) {
                    offset = idx;
                    currKeyValue = start + numberOfKeys;
                    return true;
                }
                numberOfKeys -= range;
            }

            // exhausted container
            offset = backingChunk.size();
            currKeyValue = maxKeyValue;
            return false;
        }

        boolean isEmpty() {
            return offset >= backingChunk.size() || currKeyValue > maxKeyValue;
        }
    }

    private class Iterator implements OrderedKeys.Iterator {
        private final OffsetHelper helper = new OffsetHelper();
        private int cachedRelativePositionOffset = 0;
        private long cachedRelativePosition = 0;
        private OrderedKeysKeyRangesChunkImpl pendingClose;

        private void tryClosePendingClose() {
            if (pendingClose != null) {
                pendingClose.close();
                pendingClose = null;
            }
        }

        @Override
        public final void close() {
            tryClosePendingClose();
        }

        @Override
        public boolean hasMore() {
            return !helper.isEmpty();
        }

        @Override
        public long peekNextKey() {
            return helper.currKeyValue;
        }

        @Override
        public OrderedKeys getNextOrderedKeysThrough(long maxKeyInclusive) {
            tryClosePendingClose();
            final int newStartOffset = helper.offset;
            final long newMinKeyValue = helper.currKeyValue;

            advance(maxKeyInclusive);
            if (helper.currKeyValue == maxKeyInclusive) {
                helper.advanceInPositionSpace(1);
            }
            // include this range if our maxKey is in it
            int newEndOffset = helper.offset;
            if (newEndOffset < backingChunk.size() && backingChunk.get(newEndOffset) <= maxKeyInclusive) {
                newEndOffset += 2;
            }
            final int newLen = newEndOffset - newStartOffset;

            if (newLen == 0) {
                return OrderedKeys.EMPTY;
            }

            pendingClose = new OrderedKeysKeyRangesChunkImpl(
                    backingChunk.slice(newStartOffset, newLen), newMinKeyValue, maxKeyInclusive);
            return pendingClose;
        }

        @Override
        public OrderedKeys getNextOrderedKeysWithLength(long numberOfKeys) {
            tryClosePendingClose();
            if (numberOfKeys <= 0) {
                return OrderedKeys.EMPTY;
            }

            final int newStartOffset = helper.offset;
            final long newMinKeyValue = helper.currKeyValue;
            helper.advanceInPositionSpace(numberOfKeys - 1);
            final int newLen = Math.min(backingChunk.size(), helper.offset + 2) - newStartOffset;
            final long newMaxKeyValue = helper.currKeyValue;
            helper.advanceInPositionSpace(1);

            if (newLen == 0) {
                return OrderedKeys.EMPTY;
            }

            pendingClose = new OrderedKeysKeyRangesChunkImpl(
                    backingChunk.slice(newStartOffset, newLen), newMinKeyValue, newMaxKeyValue);
            return pendingClose;
        }

        @Override
        public boolean advance(long nextKey) {
            nextKey = Math.max(helper.currKeyValue, nextKey);
            final int newEndOffset = OrderedChunkUtils.findInChunk(backingChunk, nextKey, helper.offset,
                    backingChunk.size());
            helper.offset = newEndOffset - (newEndOffset % 2);
            boolean hasMore = helper.offset < backingChunk.size();
            if (hasMore) {
                helper.currKeyValue = Math.max(nextKey, backingChunk.get(helper.offset));
            }
            return hasMore;
        }

        @Override
        public long getRelativePosition() {
            if (helper.offset >= backingChunk.size()) {
                long pos = cachedRelativePosition;
                for (int idx = cachedRelativePositionOffset; idx < backingChunk.size(); idx += 2) {
                    final long rangeStart = backingChunk.get(idx);
                    final long rangeEnd = backingChunk.get(idx + 1);
                    if (rangeEnd <= maxKeyValue) {
                        final long d = rangeEnd - rangeStart + 1;
                        cachedRelativePosition += d;
                        cachedRelativePositionOffset = idx + 2;
                        pos += d;
                    } else {
                        pos += maxKeyValue - rangeStart + 1;
                    }
                }
                return pos + 1;
            }
            long pos = cachedRelativePosition;
            for (int idx = cachedRelativePositionOffset; idx <= helper.offset; idx += 2) {
                final long rangeStart = backingChunk.get(idx);
                final long rangeEnd = backingChunk.get(idx + 1);
                if (rangeEnd <= helper.currKeyValue) {
                    final long d = rangeEnd - rangeStart + 1;
                    cachedRelativePosition += d;
                    cachedRelativePositionOffset = idx + 2;
                    pos += d;
                } else {
                    pos += helper.currKeyValue - rangeStart + 1;
                }
            }
            return pos;
        }
    }

    @Override
    public OrderedKeys.Iterator getOrderedKeysIterator() {
        return new Iterator();
    }

    @Override
    public OrderedKeys getOrderedKeysByPosition(final long startPositionInclusive, final long length) {
        if (length <= 0) {
            return OrderedKeys.EMPTY;
        }

        OffsetHelper helper = new OffsetHelper();
        if (!helper.advanceInPositionSpace(startPositionInclusive)) {
            return OrderedKeys.EMPTY;
        }

        final int newStartOffset = helper.offset;
        final long newMinKeyValue = helper.currKeyValue;

        helper.advanceInPositionSpace(length - 1);
        final int newLen = Math.min(backingChunk.size(), helper.offset + 2) - newStartOffset;
        final long newMaxKeyValue = helper.currKeyValue;

        if (newLen == 0) {
            return OrderedKeys.EMPTY;
        }

        return new OrderedKeysKeyRangesChunkImpl(backingChunk.slice(newStartOffset, newLen), newMinKeyValue,
                newMaxKeyValue);
    }

    @Override
    public OrderedKeys getOrderedKeysByKeyRange(long startKeyInclusive, long endKeyInclusive) {
        // Apply this container's bounds to the requested bounds.
        startKeyInclusive = Math.max(startKeyInclusive, minKeyValue);
        endKeyInclusive = Math.min(endKeyInclusive, maxKeyValue);

        int newStartOffset = OrderedChunkUtils.findInChunk(backingChunk, startKeyInclusive);
        newStartOffset -= newStartOffset % 2; // beginning of range
        int newEndOffset = OrderedChunkUtils.findInChunk(backingChunk, endKeyInclusive);
        newEndOffset += newEndOffset % 2; // include range if point at end
        // check if range begins with our inclusive key
        if (newEndOffset < backingChunk.size() && backingChunk.get(newEndOffset) == endKeyInclusive) {
            newEndOffset += 2;
        }

        final int newLen = newEndOffset - newStartOffset;
        if (newLen == 0) {
            return OrderedKeys.EMPTY;
        }

        return new OrderedKeysKeyRangesChunkImpl(backingChunk.slice(newStartOffset, newLen), startKeyInclusive,
                endKeyInclusive);
    }

    @Override
    public Index asIndex() {
        if (backingChunk.size() == 0) {
            return Index.FACTORY.getEmptyIndex();
        }

        final Index.SequentialBuilder builder = Index.FACTORY.getSequentialBuilder();
        final long chunkFirst = backingChunk.get(0);
        final long chunkLast = backingChunk.get(backingChunk.size() - 1);
        final boolean specialStart = minKeyValue > chunkFirst;
        final boolean specialEnd = maxKeyValue < chunkLast;
        builder.setDomain(specialStart ? minKeyValue : chunkFirst, specialEnd ? maxKeyValue : chunkLast);

        if (specialStart || (specialEnd && backingChunk.size() == 2)) {
            builder.appendRange(Math.max(minKeyValue, backingChunk.get(0)), Math.min(maxKeyValue, backingChunk.get(1)));
        }
        final int startOffset = specialStart ? 2 : 0;
        // note it me be true that innerLength < 0 if there is a single range and both min and max keys restrict
        final int innerLength = backingChunk.size() - startOffset - (specialEnd ? 2 : 0);
        if (innerLength > 0) {
            builder.appendOrderedKeyRangesChunk(backingChunk.slice(startOffset, innerLength));
        }
        if (specialEnd && backingChunk.size() > 2) {
            builder.appendRange(backingChunk.get(backingChunk.size() - 2),
                    Math.min(maxKeyValue, backingChunk.get(backingChunk.size() - 1)));
        }
        return builder.getIndex();
    }

    @Override
    public LongChunk<OrderedKeyIndices> asKeyIndicesChunk() {
        if (backingChunk.size() <= 0) {
            return LongChunk.getEmptyChunk();
        }
        if (asKeyIndicesChunk == null) {
            final long chunkSize = size();
            if (chunkSize > LongChunk.MAXIMUM_SIZE) {
                throw new SizeException("Cannot create LongChunk<OrderedKeyIndices>; too many values.", size(),
                        LongChunk.MAXIMUM_SIZE);
            }
            asKeyIndicesChunk = WritableLongChunk.makeWritableChunk(Math.toIntExact(chunkSize));
            fillKeyIndicesChunk(asKeyIndicesChunk);
        }
        return asKeyIndicesChunk;
    }

    @Override
    public LongChunk<OrderedKeyRanges> asKeyRangesChunk() {
        if (backingChunk.size() <= 0) {
            return LongChunk.getEmptyChunk();
        }
        if (asKeyRangesChunk == null) {
            asKeyRangesChunk = WritableLongChunk.makeWritableChunk(backingChunk.size());
            backingChunk.copyToChunk(0, asKeyRangesChunk, 0, backingChunk.size());
            asKeyRangesChunk.set(0, Math.max(minKeyValue, asKeyRangesChunk.get(0)));
            asKeyRangesChunk.set(backingChunk.size() - 1,
                    Math.min(maxKeyValue, asKeyRangesChunk.get(backingChunk.size() - 1)));
        }
        return asKeyRangesChunk;
    }

    @Override
    public void fillKeyIndicesChunk(final WritableLongChunk<? extends KeyIndices> chunkToFill) {
        chunkToFill.setSize(0);
        perKeyIndex((v) -> {
            chunkToFill.add(v);
            return true;
        });
    }

    @Override
    public void fillKeyRangesChunk(final WritableLongChunk<OrderedKeyRanges> chunkToFill) {
        int newSize = backingChunk.size();
        newSize -= newSize & 1;
        backingChunk.copyToChunk(0, chunkToFill, 0, newSize);
        chunkToFill.setSize(newSize);
        chunkToFill.set(0, Math.max(minKeyValue, chunkToFill.get(0)));
        chunkToFill.set(newSize - 1, Math.min(maxKeyValue, chunkToFill.get(newSize - 1)));
    }

    @Override
    public boolean isEmpty() {
        return backingChunk.size() == 0;
    }

    @Override
    public long firstKey() {
        return backingChunk.size() == 0 ? Index.NULL_KEY : Math.max(minKeyValue, backingChunk.get(0));
    }

    @Override
    public long lastKey() {
        final int sz = backingChunk.size();
        return sz == 0 ? Index.NULL_KEY : Math.min(maxKeyValue, backingChunk.get(sz - 1));
    }

    @Override
    public long size() {
        if (cachedSize > 0) {
            return cachedSize;
        }

        for (int idx = 0; idx + 1 < backingChunk.size(); idx += 2) {
            final long start = Math.max(minKeyValue, backingChunk.get(idx));
            cachedSize += Math.min(maxKeyValue, backingChunk.get(idx + 1)) - start + 1;
        }
        if (cachedSize < 0) {
            cachedSize = Long.MAX_VALUE;
        }
        return cachedSize;
    }

    @Override
    public long getAverageRunLengthEstimate() {
        final int numRanges = backingChunk.size() / 2;
        return numRanges == 0 ? 1 : Math.max(1, size() / numRanges);
    }

    private boolean forEachInRange(final long start, final long endInclusive, final LongAbortableConsumer lc) {
        for (long v = start; v <= endInclusive; ++v) {
            if (!lc.accept(v)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean forEachLong(final LongAbortableConsumer lc) {
        if (backingChunk.size() == 0) {
            return true;
        }
        final long s0 = backingChunk.get(0);
        final long e0 = backingChunk.get(1);
        if (backingChunk.size() == 2) {
            return forEachInRange(
                    Math.max(minKeyValue, s0),
                    Math.min(maxKeyValue, e0),
                    lc);
        }
        if (!forEachInRange(
                Math.max(minKeyValue, s0),
                e0,
                lc)) {
            return false;
        }
        int i = 2;
        for (; i < backingChunk.size() - 2; i += 2) {
            final long s = backingChunk.get(i);
            final long e = backingChunk.get(i + 1);
            if (!forEachInRange(s, e, lc)) {
                return false;
            }
        }
        final long s = backingChunk.get(i);
        final long e = backingChunk.get(i + 1);
        return forEachInRange(
                s,
                Math.min(maxKeyValue, e),
                lc);
    }

    @Override
    public boolean forEachLongRange(final LongRangeAbortableConsumer lrac) {
        if (backingChunk.size() == 0) {
            return true;
        }
        final long s0 = backingChunk.get(0);
        final long e0 = backingChunk.get(1);
        if (backingChunk.size() == 2) {
            return lrac.accept(
                    Math.max(minKeyValue, s0),
                    Math.min(maxKeyValue, e0));
        }
        if (!lrac.accept(
                Math.max(minKeyValue, s0),
                e0)) {
            return false;
        }
        int i = 2;
        for (; i < backingChunk.size() - 2; i += 2) {
            final long s = backingChunk.get(i);
            final long e = backingChunk.get(i + 1);
            if (!lrac.accept(s, e)) {
                return false;
            }
        }
        final long s = backingChunk.get(i);
        final long e = backingChunk.get(i + 1);
        return lrac.accept(
                s,
                Math.min(maxKeyValue, e));
    }

    @Override
    public void close() {
        if (toReleaseChunk != null) {
            toReleaseChunk.close();
            toReleaseChunk = null;
        }
        if (asKeyIndicesChunk != null) {
            asKeyIndicesChunk.close();
            asKeyIndicesChunk = null;
        }
        if (asKeyRangesChunk != null) {
            asKeyRangesChunk.close();
            asKeyRangesChunk = null;
        }
    }

    private void perKeyIndex(LongAbortableConsumer consumer) {
        for (int idx = 0; idx + 1 < backingChunk.size(); idx += 2) {
            final long start = Math.max(minKeyValue, backingChunk.get(idx));
            long range = Math.min(maxKeyValue, backingChunk.get(idx + 1)) - start + 1;
            boolean overflowed = range < 0;
            if (overflowed) {
                range = Long.MAX_VALUE;
            }
            for (long jdx = 0; jdx < range; ++jdx) {
                if (!consumer.accept(start + jdx)) {
                    return;
                }
            }
            if (overflowed) {
                // Note: start better be zero here.
                consumer.accept(Long.MAX_VALUE);
            }

        }
    }
}
