/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class IntegralChunkedReVarOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource resultColumn;
    private final String name;
    private final boolean std;
    private final DoubleChunkedSumOperator sumSum;
    private final DoubleChunkedSumOperator sum2Sum;
    private final LongChunkedSumOperator nncSum;

    IntegralChunkedReVarOperator(String name, boolean std, DoubleChunkedSumOperator sumSum, DoubleChunkedSumOperator sum2sum, LongChunkedSumOperator nncSum) {
        this.name = name;
        this.std = std;
        this.sumSum = sumSum;
        this.sum2Sum = sum2sum;
        this.nncSum = nncSum;
        resultColumn = new DoubleArraySource();
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReVarContext)context, destinations, startPositions, stateModified);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReVarContext)context, destinations, startPositions, stateModified);
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReVarContext)context, destinations, startPositions, stateModified);
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return updateResult(destination);
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return updateResult(destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        return updateResult(destination);
    }

    private void doBucketedUpdate(ReVarContext context, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, WritableBooleanChunk<Values> stateModified) {
        try (final RowSequence destinationSeq = context.destinationSequenceFromChunks(destinations, startPositions)) {
            updateResult(context, destinationSeq, stateModified);
        }
    }

    private void updateResult(ReVarContext reVarContext, RowSequence destinationOk, WritableBooleanChunk<Values> stateModified) {
        final DoubleChunk<Values> sumSumChunk = sumSum.getChunk(reVarContext.sumSumContext, destinationOk).asDoubleChunk();
        final DoubleChunk<Values> sum2SumChunk = sum2Sum.getChunk(reVarContext.sum2SumContext, destinationOk).asDoubleChunk();
        final LongChunk<? extends Values> nncSumChunk = nncSum.getChunk(reVarContext.nncSumContext, destinationOk).asLongChunk();
        final int size = reVarContext.keyIndices.size();
        final boolean ordered = reVarContext.ordered;
        for (int ii = 0; ii < size; ++ii) {
            final boolean changed = updateResult(reVarContext.keyIndices.get(ii), sumSumChunk.get(ii), sum2SumChunk.get(ii), nncSumChunk.get(ii));
            stateModified.set(ordered ? ii : reVarContext.statePositions.get(ii), changed);
        }
    }

    private boolean updateResult(long destination) {
        final double newSum = sumSum.getResult(destination);
        final double newSum2 = sum2Sum.getResult(destination);
        final long nonNullCount = nncSum.getResult(destination);

        return updateResult(destination, newSum, newSum2, nonNullCount);
    }

    private boolean updateResult(long destination, double newSum, double newSum2, long nonNullCount) {
        if (nonNullCount <= 1) {
            return !Double.isNaN(resultColumn.getAndSetUnsafe(destination, Double.NaN));
        } else {
            final double variance = (newSum2 - (newSum * newSum / nonNullCount)) / (nonNullCount - 1);
            final double newValue =  std ? Math.sqrt(variance) : variance;
            return resultColumn.getAndSetUnsafe(destination, newValue) != newValue;
        }
    }

    private class ReVarContext extends ReAvgVarOrderingContext implements BucketedContext {
        final ChunkSource.GetContext sumSumContext;
        final ChunkSource.GetContext sum2SumContext;
        final ChunkSource.GetContext nncSumContext;

        private ReVarContext(int size) {
            super(size);
            sumSumContext = sumSum.makeGetContext(size);
            sum2SumContext = sum2Sum.makeGetContext(size);
            nncSumContext = nncSum.makeGetContext(size);
        }

        @Override
        public void close() {
            super.close();
            sumSumContext.close();
            sum2SumContext.close();
            nncSumContext.close();
        }
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new ReVarContext(size);
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }
}
