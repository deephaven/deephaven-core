/* ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharChunkedAddOnlyMinMaxOperator and regenerate
 * ------------------------------------------------------------------------------------------------------------------ */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.DhShortComparisons;
import io.deephaven.db.v2.sources.ShortArraySource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class ShortChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final ShortArraySource resultColumn;
    private final boolean minimum;
    private final String name;

    ShortChunkedAddOnlyMinMaxOperator(
            // region extra constructor params
            // endregion extra constructor params
            boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
        // region resultColumn initialization
        this.resultColumn = new ShortArraySource();
        // endregion resultColumn initialization
    }

    private short min(ShortChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull = 0;
        short value = QueryConstants.NULL_SHORT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final short candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_SHORT) {
                if (nonNull++ == 0) {
                    value = candidate;
                } else if (DhShortComparisons.lt(candidate, value)) {
                    value = candidate;
                }
            }
        }
        chunkNonNull.setValue(nonNull);
        return value;
    }

    private short max(ShortChunk<?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull =0;
        short value = QueryConstants.NULL_SHORT;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final short candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_SHORT) {
                if (nonNull++ == 0) {
                    value = candidate;
                } else if (DhShortComparisons.gt(candidate, value)) {
                    value = candidate;
                }
            }
        }
        chunkNonNull.setValue(nonNull);
        return value;
    }

    private short min(short a, short b) {
        return DhShortComparisons.lt(a, b) ? a : b;
    }

    private short max(short a, short b) {
        return DhShortComparisons.gt(a, b) ? a : b;
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ShortChunk<? extends Values> asShortChunk = values.asShortChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asShortChunk, destination, startPosition, length.get(ii)));
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
        return addChunk(values.asShortChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    private boolean addChunk(ShortChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final MutableInt chunkNonNull = new MutableInt(0);
        final int chunkEnd = chunkStart + chunkSize;
        final short chunkValue = minimum ? min(values, chunkNonNull, chunkStart, chunkEnd) : max(values, chunkNonNull, chunkStart, chunkEnd);
        if (chunkNonNull.intValue() == 0) {
            return false;
        }

        final short result;
        final short oldValue = resultColumn.getUnsafe(destination);
        if (oldValue == QueryConstants.NULL_SHORT) {
            // we exclude nulls from the min/max calculation, therefore if the value in our min/max is null we know
            // that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? min(chunkValue, oldValue) : max(chunkValue, oldValue);
        }
        if (!DhShortComparisons.eq(result, oldValue)) {
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
