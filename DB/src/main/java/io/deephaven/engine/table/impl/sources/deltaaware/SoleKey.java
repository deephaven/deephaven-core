package io.deephaven.engine.table.impl.sources.deltaaware;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;

class SoleKey implements RowSequence {
    private long key;
    private final WritableLongChunk<Attributes.OrderedRowKeys> keyIndicesChunk;
    private final WritableLongChunk<Attributes.OrderedRowKeyRanges> keyRangesChunk;

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
    public Iterator getRowSequenceIterator() {
        return new SoleKeyIterator(key);
    }

    @Override
    public RowSequence getRowSequenceByPosition(long startPositionInclusive, long length) {
        if (startPositionInclusive == 0 && length > 0) {
            return this;
        }
        return RowSequenceFactory.EMPTY;
    }

    @Override
    public RowSequence getRowSequenceByKeyRange(long startRowKeyInclusive, long endRowKeyInclusive) {
        if (startRowKeyInclusive <= key && endRowKeyInclusive >= key) {
            return this;
        }
        return RowSequenceFactory.EMPTY;
    }

    @Override
    public RowSet asRowSet() {
        return RowSetFactory.fromKeys(key);
    }

    @Override
    public LongChunk<Attributes.OrderedRowKeys> asRowKeyChunk() {
        return keyIndicesChunk;
    }

    @Override
    public LongChunk<Attributes.OrderedRowKeyRanges> asRowKeyRangesChunk() {
        return keyRangesChunk;
    }

    @Override
    public void fillRowKeyChunk(WritableLongChunk<? extends Attributes.RowKeys> chunkToFill) {
        chunkToFill.set(0, key);
        chunkToFill.setSize(1);
    }

    @Override
    public void fillRowKeyRangesChunk(WritableLongChunk<Attributes.OrderedRowKeyRanges> chunkToFill) {
        chunkToFill.set(0, key);
        chunkToFill.set(1, key);
        chunkToFill.setSize(2);
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long firstRowKey() {
        return key;
    }

    @Override
    public long lastRowKey() {
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
    public boolean forEachRowKey(LongAbortableConsumer lac) {
        return lac.accept(key);
    }

    @Override
    public boolean forEachRowKeyRange(LongRangeAbortableConsumer larc) {
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
            return hasMore ? key : RowSequence.NULL_ROW_KEY;
        }

        @Override
        public RowSequence getNextRowSequenceThrough(long maxKeyInclusive) {
            if (!hasMore || maxKeyInclusive < key) {
                return RowSequenceFactory.EMPTY;
            }
            hasMore = false;
            return internalFixedSoloKey;
        }

        @Override
        public RowSequence getNextRowSequenceWithLength(long numberOfKeys) {
            if (!hasMore || numberOfKeys == 0) {
                return RowSequenceFactory.EMPTY;
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
