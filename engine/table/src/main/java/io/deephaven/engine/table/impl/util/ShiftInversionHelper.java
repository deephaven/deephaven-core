package io.deephaven.engine.table.impl.util;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.RowSetShiftData;

import java.util.function.LongUnaryOperator;

/**
 * Helper utility for inverting row key shifts.
 */
public class ShiftInversionHelper {

    private final RowSetShiftData shifted;
    private final boolean reverseOrder;

    private int destShiftIdx;

    public ShiftInversionHelper(final RowSetShiftData shifted) {
        // if not specified, assume forward viewport ordering
        this(shifted, false);
    }

    public ShiftInversionHelper(final RowSetShiftData shifted, final boolean reverseOrder) {
        this.shifted = shifted;
        this.reverseOrder = reverseOrder;
        this.destShiftIdx = reverseOrder ? shifted.size() : 0;
    }

    private void advanceDestShiftIdx(final long destKey) {
        Assert.geq(destKey, "destKey", 0);
        destShiftIdx = (int) binarySearch(
                reverseOrder ? 0 : destShiftIdx,
                reverseOrder ? destShiftIdx : shifted.size(),
                innerShiftIdx -> {
                    long destEnd =
                            shifted.getEndRange((int) innerShiftIdx) + shifted.getShiftDelta((int) innerShiftIdx);
                    // due to destKey's expected range, we know this subtraction will not overflow
                    return destEnd - destKey;
                });
    }

    /**
     * Converts post-keyspace key to pre-keyspace key. It expects to be invoked in ascending key order.
     */
    public long mapToPrevKeyspace(final long key, final boolean isEnd) {
        if (shifted.empty()) {
            return key;
        }

        advanceDestShiftIdx(key);

        final long retval;
        final int idx = destShiftIdx;

        if (idx < shifted.size() && shifted.getBeginRange(idx) + shifted.getShiftDelta(idx) <= key) {
            // inside of a destination shift; this is easy to map to prev
            retval = key - shifted.getShiftDelta(idx);
        } else if (idx < shifted.size() && shifted.getShiftDelta(idx) > 0 && shifted.getBeginRange(idx) <= key) {
            // our key is left of the destination but to right of the shift start
            retval = shifted.getBeginRange(idx) - (isEnd ? 1 : 0);
        } else if (idx > 0 && shifted.getShiftDelta(idx - 1) < 0 && key <= shifted.getEndRange(idx - 1)) {
            // our key is right of the destination but left of the shift start
            retval = shifted.getEndRange(idx - 1) + (isEnd ? 0 : 1);
        } else {
            retval = key;
        }
        return retval;
    }

    private static long binarySearch(int low, int high, final LongUnaryOperator evaluate) {
        while (low < high) {
            final int mid = (low + high) / 2;
            final long res = evaluate.applyAsLong(mid);
            if (res < 0) {
                low = mid + 1;
            } else if (res > 0) {
                high = mid;
            } else {
                return mid;
            }
        }
        return low;
    }
}
