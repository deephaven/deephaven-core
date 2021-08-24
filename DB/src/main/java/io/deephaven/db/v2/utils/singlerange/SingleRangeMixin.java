package io.deephaven.db.v2.utils.singlerange;

import io.deephaven.db.v2.utils.LongAbortableConsumer;
import io.deephaven.db.v2.utils.LongRangeAbortableConsumer;
import io.deephaven.db.v2.utils.OrderedKeys;

/**
 * This interface is really a mixin to avoid code duplication in the classes that implement it.
 */
public interface SingleRangeMixin extends OrderedKeys {
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

    default OrderedKeys getOrderedKeysByPosition(final long startPositionInclusive,
        final long length) {
        if (startPositionInclusive >= size() || length == 0) {
            return OrderedKeys.EMPTY;
        }
        final long s = rangeStart() + startPositionInclusive;
        final long e = Math.min(s + length - 1, rangeEnd());
        return new SingleRangeOrderedKeys(s, e);
    }

    default OrderedKeys getOrderedKeysByKeyRange(final long startKeyInclusive,
        final long endKeyInclusive) {
        if (startKeyInclusive > rangeEnd() ||
            endKeyInclusive < rangeStart() ||
            endKeyInclusive < startKeyInclusive) {
            return OrderedKeys.EMPTY;
        }
        return new SingleRangeOrderedKeys(
            Math.max(startKeyInclusive, rangeStart()),
            Math.min(endKeyInclusive, rangeEnd()));
    }

    default OrderedKeys.Iterator getOrderedKeysIterator() {
        return new SingleRangeOrderedKeys.OKIterator(rangeStart(), rangeEnd());
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
