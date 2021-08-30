package io.deephaven.db.v2.sources.deltaaware;

import io.deephaven.db.v2.sources.chunk.Attributes;
import io.deephaven.db.v2.sources.chunk.LongChunk;
import io.deephaven.db.v2.sources.chunk.WritableLongChunk;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.LongAbortableConsumer;
import io.deephaven.db.v2.utils.LongRangeAbortableConsumer;
import io.deephaven.db.v2.utils.OrderedKeys;

class SoleKey implements OrderedKeys {
    private long key;
    private final WritableLongChunk<Attributes.OrderedKeyIndices> keyIndicesChunk;
    private final WritableLongChunk<Attributes.OrderedKeyRanges> keyRangesChunk;

    SoleKey(final long key) {
        keyIndicesChunk = WritableLongChunk.makeWritableChunk(1);
        keyRangesChunk = WritableLongChunk.makeWritableChunk(2);
        setKey(key);
    }

    void setKey(final long key) {
        this.key = key;
        keyIndicesChunk.set(0, key);
        keyRangesChunk.set(0, key);
        keyRangesChunk.set(1, key);
    }

    @Override
    public Iterator getOrderedKeysIterator() {
        return new SoleKeyIterator(key);
    }

    @Override
    public OrderedKeys getOrderedKeysByPosition(long startPositionInclusive, long length) {
        if (startPositionInclusive == 0 && length > 0) {
            return this;
        }
        return Index.EMPTY;
    }

    @Override
    public OrderedKeys getOrderedKeysByKeyRange(long startKeyInclusive, long endKeyInclusive) {
        if (startKeyInclusive <= key && endKeyInclusive >= key) {
            return this;
        }
        return Index.EMPTY;
    }

    @Override
    public Index asIndex() {
        return Index.FACTORY.getIndexByValues(key);
    }

    @Override
    public LongChunk<Attributes.OrderedKeyIndices> asKeyIndicesChunk() {
        return keyIndicesChunk;
    }

    @Override
    public LongChunk<Attributes.OrderedKeyRanges> asKeyRangesChunk() {
        return keyRangesChunk;
    }

    @Override
    public void fillKeyIndicesChunk(
        WritableLongChunk<? extends Attributes.KeyIndices> chunkToFill) {
        chunkToFill.set(0, key);
        chunkToFill.setSize(1);
    }

    @Override
    public void fillKeyRangesChunk(WritableLongChunk<Attributes.OrderedKeyRanges> chunkToFill) {
        chunkToFill.set(0, key);
        chunkToFill.set(1, key);
        chunkToFill.setSize(2);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long firstKey() {
        return key;
    }

    @Override
    public long lastKey() {
        return key;
    }

    @Override
    public long size() {
        return 1;
    }

    @Override
    public long getAverageRunLengthEstimate() {
        return 1;
    }

    @Override
    public boolean forEachLong(LongAbortableConsumer lac) {
        return lac.accept(key);
    }

    @Override
    public boolean forEachLongRange(LongRangeAbortableConsumer larc) {
        return larc.accept(key, key);
    }

    static class SoleKeyIterator implements Iterator {
        private final long key;
        private boolean hasMore;
        private final SoleKey internalFixedSoloKey;

        SoleKeyIterator(final long key) {
            this.key = key;
            this.hasMore = true;
            this.internalFixedSoloKey = new SoleKey(key);
        }

        @Override
        public boolean hasMore() {
            return hasMore;
        }

        @Override
        public long peekNextKey() {
            return hasMore ? key : Index.NULL_KEY;
        }

        @Override
        public OrderedKeys getNextOrderedKeysThrough(long maxKeyInclusive) {
            if (!hasMore || maxKeyInclusive < key) {
                return OrderedKeys.EMPTY;
            }
            hasMore = false;
            return internalFixedSoloKey;
        }

        @Override
        public OrderedKeys getNextOrderedKeysWithLength(long numberOfKeys) {
            if (!hasMore || numberOfKeys == 0) {
                return OrderedKeys.EMPTY;
            }
            hasMore = false;
            return internalFixedSoloKey;
        }

        @Override
        public boolean advance(long nextKey) {
            if (!hasMore) {
                return false;
            }
            if (nextKey > key) {
                hasMore = false;
                return false;
            }
            return true;
        }

        @Override
        public long getRelativePosition() {
            return 0;
        }
    }
}
