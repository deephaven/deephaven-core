/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import org.apache.commons.lang3.mutable.MutableInt;

public class ShiftedRowSequence extends RowSequenceAsChunkImpl implements RowSequence {

    public static RowSequence wrap(RowSequence toWrap, long shiftAmount) {
        if (toWrap instanceof ShiftedRowSequence) {
            final ShiftedRowSequence orig = ((ShiftedRowSequence) toWrap);
            toWrap = orig.wrappedOK;
            shiftAmount += orig.shiftAmount;
        }
        return (shiftAmount == 0) ? toWrap : new ShiftedRowSequence(toWrap, shiftAmount);
    }

    private long shiftAmount;
    private RowSequence wrappedOK;

    private ShiftedRowSequence(final RowSequence wrappedOK, final long shiftAmount) {
        Assert.assertion(!(wrappedOK instanceof ShiftedRowSequence),
                "Wrapped Ordered Indices must not be a ShiftedRowSequence");
        this.shiftAmount = shiftAmount;
        this.wrappedOK = wrappedOK;
    }

    public ShiftedRowSequence() {
        this.shiftAmount = 0;
        this.wrappedOK = null;
    }

    public RowSequence reset(RowSequence toWrap, long shiftAmount) {
        if (toWrap instanceof ShiftedRowSequence) {
            final ShiftedRowSequence orig = ((ShiftedRowSequence) toWrap);
            this.shiftAmount = shiftAmount + orig.shiftAmount;
            this.wrappedOK = orig.wrappedOK;
        } else {
            this.shiftAmount = shiftAmount;
            this.wrappedOK = toWrap;
        }
        invalidateRowSequenceAsChunkImpl();
        return this;
    }

    public final void clear() {
        this.shiftAmount = 0;
        this.wrappedOK = null;
        invalidateRowSequenceAsChunkImpl();
    }

    private class Iterator implements RowSequence.Iterator {
        RowSequence.Iterator wrappedIt = wrappedOK.getRowSequenceIterator();
        ShiftedRowSequence reusableOK = new ShiftedRowSequence(null, shiftAmount);

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
        public RowSequence getNextRowSequenceThrough(long maxKeyInclusive) {
            reusableOK.reset(wrappedIt.getNextRowSequenceThrough(maxKeyInclusive - shiftAmount), shiftAmount);
            return reusableOK;
        }

        @Override
        public RowSequence getNextRowSequenceWithLength(long numberOfKeys) {
            reusableOK.reset(wrappedIt.getNextRowSequenceWithLength(numberOfKeys), shiftAmount);
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
    public Iterator getRowSequenceIterator() {
        return new Iterator();
    }

    @Override
    public RowSequence getRowSequenceByPosition(long startPositionInclusive, long length) {
        return wrap(wrappedOK.getRowSequenceByPosition(startPositionInclusive, length), shiftAmount);
    }

    @Override
    public RowSequence getRowSequenceByKeyRange(long startRowKeyInclusive, long endRowKeyInclusive) {
        return wrap(
                wrappedOK.getRowSequenceByKeyRange(startRowKeyInclusive - shiftAmount,
                        endRowKeyInclusive - shiftAmount),
                shiftAmount);
    }

    @Override
    public RowSet asRowSet() {
        try (final RowSet wrappedAsRowSet = wrappedOK.asRowSet()) {
            return wrappedAsRowSet.shift(shiftAmount);
        }
    }

    @Override
    public void fillRowKeyChunk(WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        wrappedOK.fillRowKeyChunk(chunkToFill);
        shiftIndicesChunk(chunkToFill);
    }

    @Override
    public void fillRowKeyRangesChunk(WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        wrappedOK.fillRowKeyRangesChunk(chunkToFill);
        shiftKeyRangesChunk(chunkToFill);
    }

    @Override
    public boolean isEmpty() {
        return wrappedOK.isEmpty();
    }

    @Override
    public long firstRowKey() {
        return wrappedOK.firstRowKey() + shiftAmount;
    }

    @Override
    public long lastRowKey() {
        return wrappedOK.lastRowKey() + shiftAmount;
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
    public boolean forEachRowKey(LongAbortableConsumer consumer) {
        return wrappedOK.forEachRowKey((ii) -> consumer.accept(ii + shiftAmount));
    }

    @Override
    public boolean forEachRowKeyRange(LongRangeAbortableConsumer consumer) {
        return wrappedOK.forEachRowKeyRange((s, e) -> consumer.accept(s + shiftAmount, e + shiftAmount));
    }

    @Override
    public void close() {
        closeRowSequenceAsChunkImpl();
        clear();
    }

    @Override
    public long rangesCountUpperBound() {
        final MutableInt mi = new MutableInt(0);
        wrappedOK.forAllRowKeyRanges((final long start, final long end) -> mi.increment());
        return mi.intValue();
    }

    private void shiftIndicesChunk(WritableLongChunk<? super OrderedRowKeys> chunkToFill) {
        for (int ii = 0; ii < chunkToFill.size(); ++ii) {
            chunkToFill.set(ii, chunkToFill.get(ii) + shiftAmount);
        }
    }

    private void shiftKeyRangesChunk(WritableLongChunk<OrderedRowKeyRanges> chunkToFill) {
        for (int ii = 0; ii < chunkToFill.size(); ++ii) {
            chunkToFill.set(ii, chunkToFill.get(ii) + shiftAmount);
        }
    }
}
