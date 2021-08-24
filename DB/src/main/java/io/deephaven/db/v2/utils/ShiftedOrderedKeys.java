package io.deephaven.db.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import org.apache.commons.lang3.mutable.MutableInt;

public class ShiftedOrderedKeys extends OrderedKeysAsChunkImpl implements OrderedKeys {

    public static OrderedKeys wrap(OrderedKeys toWrap, long shiftAmount) {
        if (toWrap instanceof ShiftedOrderedKeys) {
            final ShiftedOrderedKeys orig = ((ShiftedOrderedKeys) toWrap);
            toWrap = orig.wrappedOK;
            shiftAmount += orig.shiftAmount;
        }
        return (shiftAmount == 0) ? toWrap : new ShiftedOrderedKeys(toWrap, shiftAmount);
    }

    private long shiftAmount;
    private OrderedKeys wrappedOK;

    private ShiftedOrderedKeys(final OrderedKeys wrappedOK, final long shiftAmount) {
        Assert.assertion(!(wrappedOK instanceof ShiftedOrderedKeys),
            "Wrapped Ordered Keys must not be a ShiftedOrderedKeys");
        this.shiftAmount = shiftAmount;
        this.wrappedOK = wrappedOK;
    }

    public ShiftedOrderedKeys() {
        this.shiftAmount = 0;
        this.wrappedOK = null;
    }

    public void reset(OrderedKeys toWrap, long shiftAmount) {
        if (toWrap instanceof ShiftedOrderedKeys) {
            final ShiftedOrderedKeys orig = ((ShiftedOrderedKeys) toWrap);
            this.shiftAmount = shiftAmount + orig.shiftAmount;
            this.wrappedOK = orig.wrappedOK;
        } else {
            this.shiftAmount = shiftAmount;
            this.wrappedOK = toWrap;
        }
        invalidateOrderedKeysAsChunkImpl();
    }

    public final void clear() {
        this.shiftAmount = 0;
        this.wrappedOK = null;
        invalidateOrderedKeysAsChunkImpl();
    }

    private class Iterator implements OrderedKeys.Iterator {
        OrderedKeys.Iterator wrappedIt = wrappedOK.getOrderedKeysIterator();
        ShiftedOrderedKeys reusableOK = new ShiftedOrderedKeys(null, shiftAmount);

        @Override
        public void close() {
            if (reusableOK != null) {
                reusableOK.close();
                reusableOK = null;
                wrappedIt.close();
                wrappedIt = null;
            }
        }

        @Override
        public boolean hasMore() {
            return wrappedIt.hasMore();
        }

        @Override
        public long peekNextKey() {
            return wrappedIt.peekNextKey() + shiftAmount;
        }

        @Override
        public OrderedKeys getNextOrderedKeysThrough(long maxKeyInclusive) {
            reusableOK.reset(wrappedIt.getNextOrderedKeysThrough(maxKeyInclusive - shiftAmount),
                shiftAmount);
            return reusableOK;
        }

        @Override
        public OrderedKeys getNextOrderedKeysWithLength(long numberOfKeys) {
            reusableOK.reset(wrappedIt.getNextOrderedKeysWithLength(numberOfKeys), shiftAmount);
            return reusableOK;
        }

        @Override
        public boolean advance(long nextKey) {
            return wrappedIt.advance(nextKey - shiftAmount);
        }

        @Override
        public long getRelativePosition() {
            return wrappedIt.getRelativePosition();
        }
    }

    @Override
    public Iterator getOrderedKeysIterator() {
        return new Iterator();
    }

    @Override
    public OrderedKeys getOrderedKeysByPosition(long startPositionInclusive, long length) {
        return wrap(wrappedOK.getOrderedKeysByPosition(startPositionInclusive, length),
            shiftAmount);
    }

    @Override
    public OrderedKeys getOrderedKeysByKeyRange(long startKeyInclusive, long endKeyInclusive) {
        return wrap(wrappedOK.getOrderedKeysByKeyRange(startKeyInclusive - shiftAmount,
            endKeyInclusive - shiftAmount), shiftAmount);
    }

    @Override
    public Index asIndex() {
        return wrappedOK.asIndex().shift(shiftAmount);
    }

    @Override
    public void fillKeyIndicesChunk(WritableLongChunk<? extends KeyIndices> chunkToFill) {
        wrappedOK.fillKeyIndicesChunk(chunkToFill);
        shiftIndicesChunk(chunkToFill);
    }

    @Override
    public void fillKeyRangesChunk(WritableLongChunk<OrderedKeyRanges> chunkToFill) {
        wrappedOK.fillKeyRangesChunk(chunkToFill);
        shiftKeyRangesChunk(chunkToFill);
    }

    @Override
    public boolean isEmpty() {
        return wrappedOK.isEmpty();
    }

    @Override
    public long firstKey() {
        return wrappedOK.firstKey() + shiftAmount;
    }

    @Override
    public long lastKey() {
        return wrappedOK.lastKey() + shiftAmount;
    }

    @Override
    public long size() {
        return wrappedOK.size();
    }

    @Override
    public long getAverageRunLengthEstimate() {
        return wrappedOK.getAverageRunLengthEstimate();
    }

    @Override
    public boolean forEachLong(LongAbortableConsumer consumer) {
        return wrappedOK.forEachLong((ii) -> consumer.accept(ii + shiftAmount));
    }

    @Override
    public boolean forEachLongRange(LongRangeAbortableConsumer consumer) {
        return wrappedOK
            .forEachLongRange((s, e) -> consumer.accept(s + shiftAmount, e + shiftAmount));
    }

    @Override
    public void close() {
        closeOrderedKeysAsChunkImpl();
        clear();
    }

    @Override
    public long rangesCountUpperBound() {
        final MutableInt mi = new MutableInt(0);
        wrappedOK.forAllLongRanges((final long start, final long end) -> mi.increment());
        return mi.intValue();
    }

    private void shiftIndicesChunk(WritableLongChunk<? extends KeyIndices> chunkToFill) {
        for (int ii = 0; ii < chunkToFill.size(); ++ii) {
            chunkToFill.set(ii, chunkToFill.get(ii) + shiftAmount);
        }
    }

    private void shiftKeyRangesChunk(WritableLongChunk<OrderedKeyRanges> chunkToFill) {
        for (int ii = 0; ii < chunkToFill.size(); ++ii) {
            chunkToFill.set(ii, chunkToFill.get(ii) + shiftAmount);
        }
    }
}
