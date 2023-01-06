/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset;

import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.impl.RowSequenceKeyRangesChunkImpl;
import io.deephaven.engine.rowset.impl.RowSequenceRowKeysChunkImpl;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.impl.singlerange.SingleRangeRowSequence;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;

/**
 * Helper methods for constructing {@link RowSequence} instances.
 */
public class RowSequenceFactory {

    /**
     * Immutable, re-usable empty {@link RowSequence} instance.
     */
    public static final RowSequence EMPTY = new RowSequence() {

        @Override
        public Iterator getRowSequenceIterator() {
            return EMPTY_ITERATOR;
        }

        @Override
        public RowSequence getRowSequenceByPosition(long startPositionInclusive, long length) {
            return this;
        }

        @Override
        public RowSequence getRowSequenceByKeyRange(long startRowKeyInclusive, long endRowKeyInclusive) {
            return this;
        }

        @Override
        public RowSet asRowSet() {
            return RowSetFactory.empty();
        }

        @Override
        public LongChunk<OrderedRowKeys> asRowKeyChunk() {
            return WritableLongChunk.getEmptyChunk();
        }

        @Override
        public LongChunk<OrderedRowKeyRanges> asRowKeyRangesChunk() {
            return WritableLongChunk.getEmptyChunk();
        }

        @Override
        public void fillRowKeyChunk(WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
            chunkToFill.setSize(0);
        }

        @Override
        public void fillRowKeyRangesChunk(WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
            chunkToFill.setSize(0);
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public long firstRowKey() {
            return RowSequence.NULL_ROW_KEY;
        }

        @Override
        public long lastRowKey() {
            return RowSequence.NULL_ROW_KEY;
        }

        @Override
        public long size() {
            return 0;
        }

        @Override
        public boolean isContiguous() {
            return true;
        }

        @Override
        public long getAverageRunLengthEstimate() {
            return 1;
        }

        @Override
        public boolean forEachRowKey(final LongAbortableConsumer lac) {
            return true;
        }

        @Override
        public boolean forEachRowKeyRange(final LongRangeAbortableConsumer larc) {
            return true;
        }

        @Override
        public String toString() {
            return "RowSequence.EMPTY";
        }
    };

    /**
     * Immutable, re-usable {@link RowSequence.Iterator} for an empty {@code RowSequence}.
     */
    public static final RowSequence.Iterator EMPTY_ITERATOR = new RowSequence.Iterator() {

        @Override
        public boolean hasMore() {
            return false;
        }

        @Override
        public long peekNextKey() {
            return RowSequence.NULL_ROW_KEY;
        }

        @Override
        public RowSequence getNextRowSequenceThrough(long maxKeyInclusive) {
            return EMPTY;
        }

        @Override
        public RowSequence getNextRowSequenceWithLength(long numberOfKeys) {
            return EMPTY;
        }

        @Override
        public boolean advance(long nextKey) {
            return false;
        }

        @Override
        public long getRelativePosition() {
            return 0;
        }
    };

    /**
     * Wrap a LongChunk as an {@link RowSequence}.
     * 
     * @param longChunk A {@link LongChunk chunk} to wrap as a new {@link RowSequence} object.
     * @return A new {@link RowSequence} object, who does not own the passed chunk.
     */
    public static RowSequence wrapRowKeysChunkAsRowSequence(final LongChunk<OrderedRowKeys> longChunk) {
        return RowSequenceRowKeysChunkImpl.makeByWrapping(longChunk);
    }

    /**
     * Wrap a LongChunk as an {@link RowSequence}.
     * 
     * @param longChunk A {@link LongChunk chunk} to wrap as a new {@link RowSequence} object.
     * @return A new {@link RowSequence} object, who does not own the passed chunk.
     */
    public static RowSequence wrapKeyRangesChunkAsRowSequence(
            final LongChunk<OrderedRowKeyRanges> longChunk) {
        return RowSequenceKeyRangesChunkImpl.makeByWrapping(longChunk);
    }

    /**
     * Create and return a new {@link RowSequence} object from the provided WritableLongChunk.
     * 
     * @param longChunk The input {@link WritableLongChunk chunk}. The returned object will take ownership of this
     *        chunk.
     * @return A new {@link RowSequence} object, who owns the passed chunk.
     */
    public static RowSequence takeRowKeysChunkAndMakeRowSequence(
            final WritableLongChunk<OrderedRowKeys> longChunk) {
        return RowSequenceRowKeysChunkImpl.makeByTaking(longChunk);
    }

    /**
     * Create and return a new {@link RowSequence} object from the provided WritableLongChunk.
     * 
     * @param longChunk The input {@link WritableLongChunk chunk}. The returned object will take ownership of this
     *        chunk.
     * @return A new {@link RowSequence} object, who owns the passed chunk.
     */
    public static RowSequence takeKeyRangesChunkAndMakeRowSequence(
            final WritableLongChunk<OrderedRowKeyRanges> longChunk) {
        return RowSequenceKeyRangesChunkImpl.makeByTaking(longChunk);
    }

    /**
     * Create and return a new {@link RowSequence} object from the supplied closed range.
     *
     * @param firstRowKey The first row key (inclusive) in the range
     * @param lastRowKey The last row key (inclusive) in the range
     * @return A new {@link RowSequence} object covering the requested range of row keys
     */
    public static RowSequence forRange(final long firstRowKey, final long lastRowKey) {
        return new SingleRangeRowSequence(firstRowKey, lastRowKey);
    }
}
