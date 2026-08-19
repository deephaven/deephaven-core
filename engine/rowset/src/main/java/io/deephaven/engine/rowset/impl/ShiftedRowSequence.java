//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import io.deephaven.util.mutable.MutableInt;

public class ShiftedRowSequence extends RowSequenceAsChunkImpl implements RowSequence {

    public static RowSequence wrap(RowSequence toWrap, long shiftAmount) {
        if (toWrap instanceof ShiftedRowSequence) {
            final ShiftedRowSequence orig = ((ShiftedRowSequence) toWrap);
            toWrap = orig.wrappedOK;
            shiftAmount += orig.shiftAmount;
        }
        validateShift(toWrap, shiftAmount);
        return (shiftAmount == 0) ? toWrap : new ShiftedRowSequence(toWrap, shiftAmount);
    }

    /**
     * Every key this sequence exposes must be a legal row key: the shift must not push the wrapped sequence's first key
     * below zero, nor its last key past {@link Long#MAX_VALUE}. Validating once here keeps the per-call paths free of
     * overflow concerns for the keys themselves; only query arguments, which may legally be {@code Long.MAX_VALUE} in
     * shifted space, still require care.
     */
    private static void validateShift(final RowSequence toWrap, final long shiftAmount) {
        if (shiftAmount == 0 || toWrap == null || toWrap.isEmpty()) {
            return;
        }
        if (shiftAmount < 0) {
            final long first = toWrap.firstRowKey();
            if (first + shiftAmount < 0) {
                throw new IllegalArgumentException("Invalid shift: shiftAmount=" + shiftAmount
                        + " would make firstRowKey=" + first + " negative");
            }
        } else {
            final long last = toWrap.lastRowKey();
            if (last + shiftAmount < 0) {
                throw new IllegalArgumentException("Invalid shift: shiftAmount=" + shiftAmount
                        + " overflows lastRowKey=" + last);
            }
        }
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
        validateShift(this.wrappedOK, this.shiftAmount);
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
            if (!hasMore()) {
                return RowSet.NULL_ROW_KEY;
            }
            return wrappedIt.peekNextKey() + shiftAmount;
        }

        @Override
        public RowSequence getNextRowSequenceThrough(long maxKeyInclusive) {
            reusableOK.reset(wrappedIt.getNextRowSequenceThrough(unshiftSaturated(maxKeyInclusive)), shiftAmount);
            return reusableOK;
        }

        @Override
        public RowSequence getNextRowSequenceWithLength(long numberOfKeys) {
            reusableOK.reset(wrappedIt.getNextRowSequenceWithLength(numberOfKeys), shiftAmount);
            return reusableOK;
        }

        @Override
        public boolean advance(long nextKey) {
            final long unshifted = nextKey - shiftAmount;
            if (shiftAmount < 0 && unshifted < nextKey) {
                // The requested key is beyond any key this sequence can contain; saturating would position
                // us before the requested key, so exhaust the wrapped iterator instead.
                if (wrappedIt.advance(Long.MAX_VALUE)) {
                    wrappedIt.getNextRowSequenceWithLength(Long.MAX_VALUE);
                }
                return false;
            }
            return wrappedIt.advance(unshifted);
        }

        @Override
        public long getRelativePosition() {
            return wrappedIt.getRelativePosition();
        }
    }

    @Override
    public RowSequence.Iterator getRowSequenceIterator() {
        return new Iterator();
    }

    @Override
    public RowSequence getRowSequenceByPosition(long startPositionInclusive, long length) {
        return wrap(wrappedOK.getRowSequenceByPosition(startPositionInclusive, length), shiftAmount);
    }

    @Override
    public RowSequence getRowSequenceByKeyRange(long startRowKeyInclusive, long endRowKeyInclusive) {
        final long unshiftedStart = startRowKeyInclusive - shiftAmount;
        if (shiftAmount < 0 && unshiftedStart < startRowKeyInclusive) {
            // The unshifted start is past the end of the key space; nothing can qualify.
            return RowSequenceFactory.EMPTY;
        }
        return wrap(
                wrappedOK.getRowSequenceByKeyRange(unshiftedStart, unshiftSaturated(endRowKeyInclusive)),
                shiftAmount);
    }

    /**
     * Remove our shift from a key provided in shifted space, saturating at {@link Long#MAX_VALUE} rather than
     * overflowing; e.g. {@code Long.MAX_VALUE} used as a "no upper bound" argument combined with a negative shift must
     * keep meaning "no upper bound". Only valid for inclusive upper bounds; positioning operations like {@code advance}
     * must treat an overflowing target as "past the end" instead.
     */
    private long unshiftSaturated(final long shiftedKey) {
        final long unshifted = shiftedKey - shiftAmount;
        if (shiftAmount < 0 && unshifted < shiftedKey) {
            return Long.MAX_VALUE;
        }
        return unshifted;
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
        if (wrappedOK.isEmpty()) {
            return RowSet.NULL_ROW_KEY;
        }
        return wrappedOK.firstRowKey() + shiftAmount;
    }

    @Override
    public long lastRowKey() {
        if (wrappedOK.isEmpty()) {
            return RowSet.NULL_ROW_KEY;
        }
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
        super.close();
        clear();
    }

    @Override
    public long rangesCountUpperBound() {
        final MutableInt mi = new MutableInt(0);
        wrappedOK.forAllRowKeyRanges((final long start, final long end) -> mi.increment());
        return mi.get();
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
