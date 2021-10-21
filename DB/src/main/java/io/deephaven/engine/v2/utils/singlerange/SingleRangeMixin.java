package io.deephaven.engine.v2.utils.singlerange;

import io.deephaven.engine.v2.utils.LongAbortableConsumer;
import io.deephaven.engine.v2.utils.LongRangeAbortableConsumer;
import io.deephaven.engine.structures.RowSequence;

/**
 * This interface is really a mixin to avoid code duplication in the classes that implement it.
 */
public interface SingleRangeMixin extends RowSequence {
    default boolean forEachLong(final LongAbortableConsumer lc) {
        for (long v = rangeStart(); v <= rangeEnd(); ++v) {
            if (!lc.accept(v)) {
                return false;
            }
        }
        return true;
    }

    default boolean forEachLongRange(final LongRangeAbortableConsumer larc) {
        return larc.accept(rangeStart(), rangeEnd());
    }

    default RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        if (startPositionInclusive >= size() || length == 0) {
            return RowSequence.EMPTY;
        }
        final long s = rangeStart() + startPositionInclusive;
        final long e = Math.min(s + length - 1, rangeEnd());
        return new SingleRangeRowSequence(s, e);
    }

    default RowSequence getRowSequenceByKeyRange(final long startRowKeyInclusive, final long endRowKeyInclusive) {
        if (startRowKeyInclusive > rangeEnd() ||
                endRowKeyInclusive < rangeStart() ||
                endRowKeyInclusive < startRowKeyInclusive) {
            return RowSequence.EMPTY;
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
