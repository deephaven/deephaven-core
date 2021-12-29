package io.deephaven.engine.rowset.impl.singlerange;

import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongRangeAbortableConsumer;
import io.deephaven.engine.rowset.RowSequence;

/**
 * This interface is really a mixin to avoid code duplication in the classes that implement it.
 */
public interface SingleRangeMixin extends RowSequence {
    default boolean forEachRowKey(final LongAbortableConsumer lc) {
        for (long v = rangeStart(); v <= rangeEnd(); ++v) {
            if (!lc.accept(v)) {
                return false;
            }
        }
        return true;
    }

    default boolean forEachRowKeyRange(final LongRangeAbortableConsumer larc) {
        return larc.accept(rangeStart(), rangeEnd());
    }

    default RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        if (startPositionInclusive >= size() || length == 0) {
            return RowSequenceFactory.EMPTY;
        }
        final long s = rangeStart() + startPositionInclusive;
        final long e = Math.min(s + length - 1, rangeEnd());
        return new SingleRangeRowSequence(s, e);
    }

    default RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        if (startRowKeyInclusive > rangeEnd() ||
                endRowKeyInclusive < rangeStart() ||
                endRowKeyInclusive < startRowKeyInclusive) {
            return RowSequenceFactory.EMPTY;
        }
        return new SingleRangeRowSequence(
                Math.max(startRowKeyInclusive, rangeStart()),
                Math.min(endRowKeyInclusive, rangeEnd()));
    }

    default Iterator getRowSequenceIterator() {
        return new SingleRangeRowSequence.Iterator(rangeStart(), rangeEnd());
    }

    default long rangesCountUpperBound() {
        return 1;
    }

    default long getAverageRunLengthEstimate() {
        return size();
    }

    default boolean isContiguous() {
        return true;
    }

    long size();

    long rangeStart();

    long rangeEnd();
}
