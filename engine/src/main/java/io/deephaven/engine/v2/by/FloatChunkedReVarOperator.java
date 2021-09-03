/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.by;

import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.sources.DoubleArraySource;
import io.deephaven.engine.structures.chunk.*;
import io.deephaven.engine.structures.chunk.Attributes.ChunkLengths;
import io.deephaven.engine.structures.chunk.Attributes.ChunkPositions;
import io.deephaven.engine.structures.chunk.Attributes.KeyIndices;
import io.deephaven.engine.structures.chunk.Attributes.Values;
import io.deephaven.engine.structures.rowsequence.OrderedKeys;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class FloatChunkedReVarOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource resultColumn;
    private final String name;

    private final boolean std;
    private final DoubleChunkedSumOperator sumSum;
    private final DoubleChunkedSumOperator sum2Sum;
    private final LongChunkedSumOperator nncSum;
    private final LongChunkedSumOperator nanSum;
    private final LongChunkedSumOperator picSum;
    private final LongChunkedSumOperator nicSum;

    FloatChunkedReVarOperator(String name, final boolean std, DoubleChunkedSumOperator sumSum, DoubleChunkedSumOperator sum2Sum, LongChunkedSumOperator nncSum, LongChunkedSumOperator nanSum, LongChunkedSumOperator picSum, LongChunkedSumOperator nicSum) {
        this.name = name;
        this.std = std;
        this.sumSum = sumSum;
        this.sum2Sum = sum2Sum;
        this.nncSum = nncSum;
        this.nanSum = nanSum;
        this.picSum = picSum;
        this.nicSum = nicSum;
        resultColumn = new DoubleArraySource();
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReVarContext) context, destinations, startPositions, stateModified);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReVarContext) context, destinations, startPositions, stateModified);
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReVarContext) context, destinations, startPositions, stateModified);
    }

    private void doBucketedUpdate(ReVarContext context, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, WritableBooleanChunk<Values> stateModified) {
        context.keyIndices.setSize(startPositions.size());
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            context.keyIndices.set(ii, destinations.get(startPosition));
        }
        try (final OrderedKeys destinationOk = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(context.keyIndices)) {
            updateResult(context, destinationOk, stateModified);
        }
    }

    private void updateResult(ReVarContext reVarContext, OrderedKeys destinationOk, WritableBooleanChunk<Values> stateModified) {
        final DoubleChunk<Values> sumSumChunk = sumSum.getChunk(reVarContext.sumContext, destinationOk).asDoubleChunk();
        final DoubleChunk<Values> sum2SumChunk = sum2Sum.getChunk(reVarContext.sum2Context, destinationOk).asDoubleChunk();
        final LongChunk<? extends Values> nncSumChunk = nncSum.getChunk(reVarContext.nncContext, destinationOk).asLongChunk();
        final LongChunk<? extends Values> nanSumChunk = nanSum.getChunk(reVarContext.nanContext, destinationOk).asLongChunk();
        final LongChunk<? extends Values> picSumChunk = picSum.getChunk(reVarContext.picContext, destinationOk).asLongChunk();
        final LongChunk<? extends Values> nicSumChunk = nicSum.getChunk(reVarContext.nicContext, destinationOk).asLongChunk();

        final int size = reVarContext.keyIndices.size();
        for (int ii = 0; ii < size; ++ii) {
            stateModified.set(ii, updateResult(reVarContext.keyIndices.get(ii), nncSumChunk.get(ii), nanSumChunk.get(ii), picSumChunk.get(ii), nicSumChunk.get(ii), sumSumChunk.get(ii), sum2SumChunk.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return updateResult(destination);
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return updateResult(destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        return updateResult(destination);
    }

    private boolean updateResult(long destination) {
        final long nncValue = nncSum.getResult(destination);
        final long nanValue = nanSum.getResult(destination);
        final long picValue = picSum.getResult(destination);
        final long nicValue = nicSum.getResult(destination);
        final double newSum = sumSum.getRunningSum(destination);
        final double newSum2 = sum2Sum.getRunningSum(destination);

        return updateResult(destination, nncValue, nanValue, picValue, nicValue, newSum, newSum2);
    }

    private boolean updateResult(long destination, long nncValue, long nanValue, long picValue, long nicValue, double newSum, double newSum2) {
        if (nanValue > 0 || picValue > 0 || nicValue > 0 || nncValue <= 1) {
            return !Double.isNaN(resultColumn.getAndSetUnsafe(destination, Double.NaN));
        } else {
            final double variance = (newSum2 - newSum * newSum / nncValue) / (nncValue - 1);

            final double newValue = std ? Math.sqrt(variance) : variance;

            return resultColumn.getAndSetUnsafe(destination, newValue) != newValue;
        }
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



    private class ReVarContext implements BucketedContext {
        final WritableLongChunk<Attributes.OrderedKeyIndices> keyIndices;
        final ChunkSource.GetContext sumContext;
        final ChunkSource.GetContext sum2Context;
        final ChunkSource.GetContext nncContext;
        final ChunkSource.GetContext nanContext;
        final ChunkSource.GetContext picContext;
        final ChunkSource.GetContext nicContext;

        private ReVarContext(int size) {
            keyIndices = WritableLongChunk.makeWritableChunk(size);
            sumContext = sumSum.makeGetContext(size);
            sum2Context = sum2Sum.makeGetContext(size);
            nncContext = nncSum.makeGetContext(size);
            nanContext = nanSum.makeGetContext(size);
            nicContext = nicSum.makeGetContext(size);
            picContext = picSum.makeGetContext(size);
        }

        @Override
        public void close() {
            keyIndices.close();
            sumContext.close();
            sum2Context.close();
            nncContext.close();
            nanContext.close();
            picContext.close();
            nicContext.close();
        }
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new ReVarContext(size);
    }
}
