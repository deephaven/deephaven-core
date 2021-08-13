/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.DhObjectComparisons;
import io.deephaven.db.v2.sources.BooleanArraySource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Iterative average operator.
 */
class BooleanChunkedAddOnlyMinMaxOperator implements IterativeChunkedAggregationOperator {
    private final BooleanArraySource resultColumn = new BooleanArraySource();
    private final boolean minimum;
    private final String name;

    BooleanChunkedAddOnlyMinMaxOperator(boolean minimum, String name) {
        this.minimum = minimum;
        this.name = name;
    }

    private Boolean min(ObjectChunk<Boolean, ?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull = 0;
        Boolean value = QueryConstants.NULL_BOOLEAN;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final Boolean candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_BOOLEAN) {
                if (nonNull++ == 0) {
                    value = candidate;
                } else if (DhObjectComparisons.lt(candidate, value)) {
                    value = candidate;
                }
            }
        }
        chunkNonNull.setValue(nonNull);
        return value;
    }

    private Boolean max(ObjectChunk<Boolean, ?> values, MutableInt chunkNonNull, int chunkStart, int chunkEnd) {
        int nonNull =0;
        Boolean value = QueryConstants.NULL_BOOLEAN;
        for (int ii = chunkStart; ii < chunkEnd; ++ii) {
            final Boolean candidate = values.get(ii);
            if (candidate != QueryConstants.NULL_BOOLEAN) {
                if (nonNull++ == 0) {
                    value = candidate;
                } else if (DhObjectComparisons.gt(candidate, value)) {
                    value = candidate;
                }
            }
        }
        chunkNonNull.setValue(nonNull);
        return value;
    }

    private Boolean min(Boolean a, Boolean b) {
        return DhObjectComparisons.lt(a, b) ? a : b;
    }

    private Boolean max(Boolean a, Boolean b) {
        return DhObjectComparisons.gt(a, b) ? a : b;
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<Boolean, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return addChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        throw new UnsupportedOperationException();
    }

    private boolean addChunk(ObjectChunk<Boolean, ? extends Values> values, long destination, int chunkStart, int chunkSize) {
        if (chunkSize == 0) {
            return false;
        }
        final MutableInt chunkNonNull = new MutableInt(0);
        final int chunkEnd = chunkStart + chunkSize;
        final Boolean chunkValue = minimum ? min(values, chunkNonNull, chunkStart, chunkEnd) : max(values, chunkNonNull, chunkStart, chunkEnd);
        if (chunkNonNull.intValue() == 0) {
            return false;
        }

        final boolean result;
        final Boolean oldValue = resultColumn.getUnsafe(destination);
        if (oldValue == QueryConstants.NULL_BOOLEAN) {
            // we exclude nulls from the min/max calculation, therefore if the value in our min/max is null we know
            // that it is in fact empty and we should use the value from the chunk
            result = chunkValue;
        } else {
            result = minimum ? min(chunkValue, oldValue) : max(chunkValue, oldValue);
        }
        if (oldValue == null || !Objects.equals(result, oldValue)) {
            resultColumn.set(destination, result);
            return true;
        }
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }
}
