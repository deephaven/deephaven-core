/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedAddOnlyMinMaxOperator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.DhIntComparisons;
import io.deephaven.db.v2.sources.IntegerArraySource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class IntChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final IntegerArraySource resultColumn;
    private final boolean minimum;
    private final String name;

    IntChunkedAddOnlyMinMaxOperator(
            // region extra constructor params
            // endregion extra constructor params
            boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
        // region resultColumn initialization
        this.resultColumn = new IntegerArraySource();
        // endregion resultColumn initialization
    }

    private int min(IntChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull = 0;
        int value = QueryConstants.NULL_INT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final int candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_INT) {
                if (nonNull++ == 0) {
                    value = candidate;
                } else if (DhIntComparisons.lt(candidate, value)) {
                    value = candidate;
                }
            }
        }
        chunkNonNull.setValue(nonNull);
        return value;
    }

    private int max(IntChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull =0;
        int value = QueryConstants.NULL_INT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final int candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_INT) {
                if (nonNull++ == 0) {
                    value = candidate;
                } else if (DhIntComparisons.gt(candidate, value)) {
                    value = candidate;
                }
            }
        }
        chunkNonNull.setValue(nonNull);
        return value;
    }

    private int min(int a, int b) {
        return DhIntComparisons.lt(a, b) ? a : b;
    }

    private int max(int a, int b) {
        return DhIntComparisons.gt(a, b) ? a : b;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final IntChunk<? extends Values> asIntChunk = values.asIntChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asIntChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return addChunk(values.asIntChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    private boolean addChunk(IntChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final MutableInt chunkNonNull = new MutableInt(0);
        final int chunkEnd = chunkStart + chunkSize;
        final int chunkValue = minimum ? min(values, chunkNonNull, chunkStart, chunkEnd) : max(values, chunkNonNull, chunkStart, chunkEnd);
        if (chunkNonNull.intValue() == 0) {
            return false;
        }

        final int result;
        final int oldValue = resultColumn.getUnsafe(destination);
        if (oldValue == QueryConstants.NULL_INT) {
            // we exclude nulls from the min/max calculation, therefore if the value in our min/max is null we know
            // that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? min(chunkValue, oldValue) : max(chunkValue, oldValue);
        }
        if (!DhIntComparisons.eq(result, oldValue)) {
            resultColumn.set(destination, result);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.<String, ColumnSource<?>>singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }
}
