/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.Map;

class CountAggregationOperator extends BasicStateChangeRecorder implements IterativeChunkedAggregationOperator {

    private final String resultName;
    private final LongArraySource countColumnSource;

    /**
     * Construct a count aggregation operator.
     *
     * @param resultName The name of the result column if this operator should expose its results, else {@code null}
     */
    CountAggregationOperator(@Nullable final String resultName) {
        this.resultName = resultName;
        this.countColumnSource = new LongArraySource();
    }

    private boolean exposesResult() {
        return resultName != null;
    }

    private void recordAdd(final long destination, final long rowsAdded) {
        final long oldCount = countColumnSource.getAndAddUnsafe(destination, rowsAdded);
        if (oldCount == 0) {
            onReincarnated(destination);
        }
    }

    private void recordRemove(final long destination, final long rowsRemoved) {
        final long oldCount = countColumnSource.getAndAddUnsafe(destination, -rowsRemoved);
        if (oldCount == rowsRemoved) {
            onEmptied(destination);
        }
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final long rowsAdded = length.get(ii);
            recordAdd(destination, rowsAdded);
        }
        if (exposesResult()) {
            stateModified.fillWithValue(0, startPositions.size(), true);
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            final long rowsRemoved = length.get(ii);
            recordRemove(destination, rowsRemoved);
        }
        if (exposesResult()) {
            stateModified.fillWithValue(0, startPositions.size(), true);
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        recordAdd(destination, chunkSize);
        return true;
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        recordRemove(destination, chunkSize);
        return true;
    }

    @Override
    public boolean unchunkedRowSet() {
        // Optimize initial grouped addition by accepting un-chunked row sets in lieu of iterative calls to
        // addChunk with null values and null inputRowKeys.
        // NB: Count is unusual in allowing this while returning false for requiresRowKeys().
        return true;
    }

    @Override
    public boolean addRowSet(SingletonContext context, RowSet rowSet, long destination) {
        recordAdd(destination, rowSet.size());
        return true;
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public void ensureCapacity(long tableSize) {
        countColumnSource.ensureCapacity(tableSize, false);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return exposesResult() ? Collections.singletonMap(resultName, countColumnSource) : Collections.emptyMap();
    }

    @Override
    public void startTrackingPrevValues() {
        if (exposesResult()) {
            countColumnSource.startTrackingPrevValues();
        }
    }
}
