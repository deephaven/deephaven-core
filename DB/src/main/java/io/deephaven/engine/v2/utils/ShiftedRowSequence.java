package io.deephaven.engine.v2.utils;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.structures.RowSequence;
import io.deephaven.engine.structures.rowsequence.RowSequenceAsChunkImpl;
import io.deephaven.engine.v2.sources.chunk.Attributes.*;
import io.deephaven.engine.v2.sources.chunk.WritableLongChunk;
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

    public void reset(RowSequence toWrap, long shiftAmount) {
        if (toWrap instanceof ShiftedRowSequence) {
            final ShiftedRowSequence orig = ((ShiftedRowSequence) toWrap);
            this.shiftAmount = shiftAmount + orig.shiftAmount;
            this.wrappedOK = orig.wrappedOK;
        } else {
            this.shiftAmount = shiftAmount;
            this.wrappedOK = toWrap;
        }
        invalidateRowSequenceAsChunkImpl();
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
    public Index asIndex() {
        return wrappedOK.asIndex().shift(shiftAmount);
    }

    @Override
    public void fillRowKeyChunk(WritableLongChunk<? extends RowKeys> chunkToFill) {
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
    public boolean forEachLong(LongAbortableConsumer consumer) {
        return wrappedOK.forEachLong((ii) -> consumer.accept(ii + shiftAmount));
    }

    @Override
    public boolean forEachLongRange(LongRangeAbortableConsumer consumer) {
        return wrappedOK.forEachLongRange((s, e) -> consumer.accept(s + shiftAmount, e + shiftAmount));
    }

    @Override
    public void close() {
        closeRowSequenceAsChunkImpl();
        clear();
    }

    @Override
    public long rangesCountUpperBound() {
        final MutableInt mi = new MutableInt(0);
        wrappedOK.forAllLongRanges((final long start, final long end) -> mi.increment());
        return mi.intValue();
    }

    private void shiftIndicesChunk(WritableLongChunk<? extends RowKeys> chunkToFill) {
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
