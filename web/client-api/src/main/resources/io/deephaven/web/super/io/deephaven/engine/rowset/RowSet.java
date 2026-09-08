//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongAbortableConsumer;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.util.PrimitiveIterator;

public interface RowSet extends RowSequence, LongSizedDataStructure, SafeCloseable {
    WritableRowSet copy();
    long get(long rowPosition);

    WritableRowSet intersect(RowSet rowSet);

    boolean overlapsRange(long startKey, long endKey);

    boolean subsetOf(@NotNull RowSet other);

    default WritableRowSet invert(RowSet keys) {
        return invert(keys, Long.MAX_VALUE);
    }
    WritableRowSet invert(RowSet keys, long maximumPosition);

    WritableRowSet minus(RowSet rowSetToRemove);

    SearchIterator searchIterator();

    SearchIterator reverseIterator();

    RangeIterator rangeIterator();

    WritableRowSet shift(long shiftAmount);

    WritableRowSet subSetByKeyRange(long startKey, long endKey);


    interface TargetComparator {
        int compareTargetTo(long rKey, int direction);
    }

    interface Iterator extends PrimitiveIterator.OfLong, SafeCloseable {
        default boolean forEachLong(final LongAbortableConsumer lc) {
            while (hasNext()) {
                final long v = nextLong();
                final boolean wantMore = lc.accept(v);
                if (!wantMore) {
                    return false;
                }
            }
            return true;
        }
    }

    interface SearchIterator extends RowSet.Iterator {
        long currentValue();

        boolean advance(long v);

        long binarySearchValue(RowSet.TargetComparator comp, int dir);
    }

    interface RangeIterator extends SafeCloseable {
        void close();

        boolean hasNext();

        boolean advance(long v);

        void postpone(long v);

        long currentRangeStart();

        long currentRangeEnd();

        long next();

        RangeIterator empty = new RangeIterator() {
            @Override
            public void close() {}

            @Override
            public boolean hasNext() {
                return false;
            }

            @Override
            public void postpone(final long v) {
                throw new IllegalStateException("empty iterator.");
            }

            @Override
            public boolean advance(long v) {
                return false;
            }

            @Override
            public long currentRangeStart() {
                return -1;
            }

            @Override
            public long currentRangeEnd() {
                return -1;
            }

            @Override
            public long next() {
                return -1;
            }
        };
    }
}
