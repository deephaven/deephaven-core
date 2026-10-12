//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.staticpercentile;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.QueryTable;
import org.jetbrains.annotations.NotNull;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.util.compare.ObjectComparisons;

import java.util.Arrays;

/**
 * {@link StaticPercentileOperator} for Comparable values, including Boolean, ordered by
 * {@link ObjectComparisons#compare(Object, Object)}. Results are never averaged. When the type's comparison is not
 * consistent with equality, as for BigDecimal values that differ only in scale, the result is the first value the group
 * received among those that compare equal to the selected value.
 */
class ObjectStaticPercentileOperator extends StaticPercentileOperator {
    /**
     * Ranges smaller than this are sorted rather than partitioned.
     */
    private static final int SORT_THRESHOLD = 16;

    private final boolean equalsConsistent;
    private final WritableColumnSource<Object> result;

    /**
     * The values accumulated for each destination, or null if it has received none.
     */
    private final ObjectArraySource<Object[]> arrays = new ObjectArraySource<>(Object[].class);

    ObjectStaticPercentileOperator(final Class<?> type, final boolean equalsConsistent, final double percentile,
            final String name) {
        super(percentile, name);
        this.equalsConsistent = equalsConsistent;
        // noinspection unchecked
        result = (WritableColumnSource<Object>) ArrayBackedColumnSource.getMemoryColumnSource(0, type);
    }

    @Override
    WritableColumnSource<?> resultColumn() {
        return result;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            append(values, startPosition, length.get(ii), destinations.get(startPosition));
            stateModified.set(ii, true);
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        append(values, 0, values.size(), (int) destination);
        return true;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        for (int destination = 0; destination < startingDestinationsCount; ++destination) {
            computeResult(destination);
        }
    }

    /**
     * Append the non-null values in {@code values[start, start + length)} to the array for {@code destination}.
     */
    private void append(final Chunk<? extends Values> values, final int start, final int length,
            final int destination) {
        final ObjectChunk<?, ? extends Values> objectValues = values.asObjectChunk();
        Object[] array = arrays.getUnsafe(destination);
        int size = array == null ? 0 : sizes.getUnsafe(destination);
        final int capacity = array == null ? 0 : array.length;
        if (capacity - size < length) {
            final int newCapacity = grownCapacity(capacity, size, length);
            array = array == null ? new Object[newCapacity] : Arrays.copyOf(array, newCapacity);
            arrays.getAndSetUnsafe(destination, array);
        }
        final int end = start + length;
        for (int ii = start; ii < end; ++ii) {
            final Object value = objectValues.get(ii);
            if (value != null) {
                array[size++] = value;
            }
        }
        sizes.getAndSetUnsafe(destination, size);
    }

    /**
     * Select the percentile of the values for {@code destination}, write its result, and release its array.
     */
    private void computeResult(final int destination) {
        final Object[] array = arrays.getAndSetUnsafe(destination, null);
        final int size = array == null ? 0 : sizes.getUnsafe(destination);
        if (size == 0) {
            result.set(destination, null);
            return;
        }
        final int targetLo = (int) Math.round((size - 1) * percentile) + 1;
        if (equalsConsistent) {
            select(array, size, targetLo - 1);
            result.set(destination, array[targetLo - 1]);
            return;
        }
        // select on a copy, so that array keeps the order in which the values were received
        final Object[] selection = Arrays.copyOf(array, size);
        select(selection, size, targetLo - 1);
        final Object selected = selection[targetLo - 1];
        for (int ii = 0; ii < size; ++ii) {
            if (ObjectComparisons.compare(array[ii], selected) == 0) {
                result.set(destination, array[ii]);
                return;
            }
        }
        throw Assert.statementNeverExecuted("selected value is not among the received values");
    }

    @Override
    void ensureArraysCapacity(final long capacity) {
        arrays.ensureCapacity(capacity);
    }

    /**
     * Introselect over {@code array[0, size)} by {@link ObjectComparisons#compare(Object, Object)}; see
     * {@link CharStaticPercentileOperator#select(char[], int, int)}. The array must not contain null values.
     */
    static void select(final Object[] array, final int size, final int position) {
        int lo = 0;
        int hi = size - 1;
        int rounds = 2 * (32 - Integer.numberOfLeadingZeros(size));
        while (hi > lo) {
            if (hi - lo < SORT_THRESHOLD || rounds-- == 0) {
                Arrays.sort(array, lo, hi + 1, ObjectComparisons::compare);
                return;
            }
            final int mid = (lo + hi) >>> 1;
            if (ObjectComparisons.lt(array[mid], array[lo])) {
                swap(array, lo, mid);
            }
            if (ObjectComparisons.lt(array[hi], array[lo])) {
                swap(array, lo, hi);
            }
            if (ObjectComparisons.lt(array[hi], array[mid])) {
                swap(array, mid, hi);
            }
            final Object pivot = array[mid];
            int ii = lo;
            int jj = hi;
            while (ii <= jj) {
                while (ObjectComparisons.lt(array[ii], pivot)) {
                    ++ii;
                }
                while (ObjectComparisons.gt(array[jj], pivot)) {
                    --jj;
                }
                if (ii <= jj) {
                    swap(array, ii++, jj--);
                }
            }
            // [lo, jj] holds no value greater than pivot, [ii, hi] no value smaller, and (jj, ii) only the pivot
            if (position <= jj) {
                hi = jj;
            } else if (position >= ii) {
                lo = ii;
            } else {
                return;
            }
        }
    }

    private static void swap(final Object[] array, final int lhs, final int rhs) {
        final Object tmp = array[lhs];
        array[lhs] = array[rhs];
        array[rhs] = tmp;
    }
}
