//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharStaticPercentileOperator and run "./gradlew replicateStaticPercentile" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.staticpercentile;

import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.QueryTable;
import org.jetbrains.annotations.NotNull;
import io.deephaven.chunk.ByteChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ByteArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

/**
 * {@link StaticPercentileOperator} for byte values.
 */
public class ByteStaticPercentileOperator extends StaticPercentileOperator {
    /**
     * Ranges smaller than this are sorted rather than partitioned.
     */
    private static final int SORT_THRESHOLD = 16;

    // region averageFields
    // endregion averageFields
    private final ByteArraySource result;

    /**
     * The values accumulated for each destination, or null if it has received none.
     */
    private final ObjectArraySource<byte[]> arrays = new ObjectArraySource<>(byte[].class);
    // region nanFields
    // endregion nanFields

    /**
     * @param percentile the percentile to compute
     * @param averageEvenlyDivided see {@link io.deephaven.api.agg.spec.AggSpecPercentile#averageEvenlyDivided()}
     * @param name the name of the result column
     */
    ByteStaticPercentileOperator(final double percentile, final boolean averageEvenlyDivided, final String name) {
        super(percentile, name);
        // region resultConstructor
        result = new ByteArraySource();
        // endregion resultConstructor
    }

    @Override
    WritableColumnSource<?> resultColumn() {
        // region resultColumn
        return result;
        // endregion resultColumn
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
        byte[] array = arrays.getUnsafe(destination);
        // region skipDestination
        // endregion skipDestination
        final ByteChunk<? extends Values> typedValues = values.asByteChunk();
        int size = array == null ? 0 : sizes.getUnsafe(destination);
        final int capacity = array == null ? 0 : array.length;
        if (capacity - size < length) {
            final int newCapacity = grownCapacity(capacity, size, length);
            array = array == null ? new byte[newCapacity] : Arrays.copyOf(array, newCapacity);
            arrays.getAndSetUnsafe(destination, array);
        }
        final int end = start + length;
        for (int ii = start; ii < end; ++ii) {
            final byte value = typedValues.get(ii);
            // region appendValue
            if (value != NULL_BYTE) {
                array[size++] = value;
            }
            // endregion appendValue
        }
        sizes.getAndSetUnsafe(destination, size);
    }

    /**
     * Select the percentile of the values for {@code destination}, write its result, and release its array.
     */
    private void computeResult(final int destination) {
        final byte[] array = arrays.getAndSetUnsafe(destination, null);
        final int size = array == null ? 0 : sizes.getUnsafe(destination);
        // region nanResult
        // endregion nanResult
        // region averagedResult
        // endregion averagedResult
        if (size == 0) {
            result.set(destination, NULL_BYTE);
            return;
        }
        final int targetLo = (int) Math.round((size - 1) * percentile) + 1;
        select(array, size, targetLo - 1);
        result.set(destination, array[targetLo - 1]);
    }

    @Override
    void ensureArraysCapacity(final long capacity) {
        arrays.ensureCapacity(capacity);
    }

    /**
     * Introselect over {@code array[0, size)}. Afterwards {@code array[position]} holds the value it would hold if the
     * prefix were sorted, no value before it is greater, and no value after it is smaller.
     * <p>
     * Each round partitions around a median-of-three pivot with a Hoare partition, which keeps runs of equal values
     * balanced. Ranges smaller than {@link #SORT_THRESHOLD} are sorted, and after {@code 2 * log2(size)} rounds the
     * remaining range is sorted, which bounds the worst case at {@code O(size log size)}. The array must not contain
     * null or NaN values.
     */
    static void select(final byte[] array, final int size, final int position) {
        int lo = 0;
        int hi = size - 1;
        int rounds = 2 * (32 - Integer.numberOfLeadingZeros(size));
        while (hi > lo) {
            if (hi - lo < SORT_THRESHOLD || rounds-- == 0) {
                Arrays.sort(array, lo, hi + 1);
                return;
            }
            final int mid = (lo + hi) >>> 1;
            if (array[mid] < array[lo]) {
                swap(array, lo, mid);
            }
            if (array[hi] < array[lo]) {
                swap(array, lo, hi);
            }
            if (array[hi] < array[mid]) {
                swap(array, mid, hi);
            }
            final byte pivot = array[mid];
            int ii = lo;
            int jj = hi;
            while (ii <= jj) {
                while (array[ii] < pivot) {
                    ++ii;
                }
                while (array[jj] > pivot) {
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

    /**
     * @return the smallest value in {@code array[from, size)}, which must not be empty
     */
    static byte min(final byte[] array, final int from, final int size) {
        byte min = array[from];
        for (int ii = from + 1; ii < size; ++ii) {
            if (array[ii] < min) {
                min = array[ii];
            }
        }
        return min;
    }

    private static void swap(final byte[] array, final int lhs, final int rhs) {
        final byte tmp = array[lhs];
        array[lhs] = array[rhs];
        array[rhs] = tmp;
    }
}
