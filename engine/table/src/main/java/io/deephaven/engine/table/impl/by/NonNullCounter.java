package io.deephaven.engine.table.impl.by;

import io.deephaven.util.QueryConstants;
import io.deephaven.engine.table.impl.sources.LongArraySource;

public final class NonNullCounter {
    private final LongArraySource nonNullCount = new LongArraySource();

    void incrementNonNull(long destPos) {
        addNonNull(destPos, 1);
    }

    /**
     * Add non-null values to destPos, return the updated count.
     * 
     * @param destPos the destination to mark non-nulls for
     * @param count how many additional non-nulls
     * @return the new count
     */
    long addNonNull(long destPos, int count) {
        nonNullCount.ensureCapacity(destPos + 1);
        return addNonNullUnsafe(destPos, count);
    }

    /**
     * Add non-null values to destPos, return the updated count.
     *
     * Note, this function does not check for capacity
     *
     * @param destPos the destination to mark non-nulls for
     * @param count how many additional non-nulls
     * @return the new count
     */
    long addNonNullUnsafe(long destPos, int count) {
        long value = nonNullCount.getUnsafe(destPos);
        if (value == QueryConstants.NULL_LONG) {
            value = count;
        } else {
            if (count == 0) {
                return value;
            }
            value += count;
        }
        nonNullCount.set(destPos, value);
        return value;
    }

    long decrementNonNull(long destPos) {
        final long value = nonNullCount.getUnsafe(destPos) - 1;
        nonNullCount.set(destPos, value);
        return value;
    }

    boolean onlyNulls(long destPos) {
        final long aLong = nonNullCount.getLong(destPos);
        return aLong == 0 || aLong == QueryConstants.NULL_LONG;
    }

    long getCount(long destPos) {
        final long aLong = nonNullCount.getLong(destPos);
        return aLong == QueryConstants.NULL_LONG ? 0 : aLong;
    }

    boolean onlyNullsUnsafe(long destPos) {
        final long aLong = nonNullCount.getUnsafe(destPos);
        return aLong == 0 || aLong == QueryConstants.NULL_LONG;
    }

    long getCountUnsafe(long destPos) {
        final long aLong = nonNullCount.getUnsafe(destPos);
        return aLong == QueryConstants.NULL_LONG ? 0 : aLong;
    }

    public void ensureCapacity(long tableSize) {
        nonNullCount.ensureCapacity(tableSize);
    }

    LongArraySource getColumnSource() {
        return nonNullCount;
    }

    void startTrackingPrevValues() {
        nonNullCount.startTrackingPrevValues();
    }
}
