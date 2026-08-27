//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
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
        final long end = rangeEnd();
        for (long v = rangeStart(); v <= end; ++v) {
            if (!lc.accept(v)) {
                return false;
            }
            if (v == end) {
                // Stepping past the end would wrap when it is the last key of the key space, and the wrapped value
                // compares as still inside the range.
                break;
            }
        }
        return true;
    }

    default boolean forEachRowKeyRange(final LongRangeAbortableConsumer larc) {
        return larc.accept(rangeStart(), rangeEnd());
    }

    default RowSequence getRowSequenceByPosition(final long startPositionInclusive, final long length) {
        // A length of zero or less asks for nothing. Falling through with a negative one would build a row sequence
        // whose end lies before its start, reporting a negative size rather than an empty one.
        if (startPositionInclusive >= size() || length <= 0) {
            return RowSequenceFactory.EMPTY;
        }
        final long s = rangeStart() + startPositionInclusive;
        // Clamped as a remaining count rather than as s + length - 1, which overflows for a very large length and
        // would put the end below the start.
        final long remaining = size() - startPositionInclusive;
        final long e = s + Math.min(length, remaining) - 1;
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
