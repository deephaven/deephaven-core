/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class BigDecimalChunkedReAvgOperator implements IterativeChunkedAggregationOperator {
    private final ObjectArraySource<BigDecimal> resultColumn;
    private final String name;
    private final BigDecimalChunkedSumOperator sumSum;
    private final LongChunkedSumOperator nncSum;

    BigDecimalChunkedReAvgOperator(String name, BigDecimalChunkedSumOperator sumSum, LongChunkedSumOperator nncSum) {
        this.name = name;
        this.sumSum = sumSum;
        this.nncSum = nncSum;
        resultColumn = new ObjectArraySource<>(BigDecimal.class);
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReAvgContext) context, destinations, startPositions, stateModified);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReAvgContext) context, destinations, startPositions, stateModified);
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReAvgContext) context, destinations, startPositions, stateModified);
    }

    private void doBucketedUpdate(ReAvgContext context, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, WritableBooleanChunk<Values> stateModified) {
        context.keyIndices.setSize(startPositions.size());
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            context.keyIndices.set(ii, destinations.get(startPosition));
        }
        try (final OrderedKeys destinationOk = OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(context.keyIndices)) {
            updateResult(context, destinationOk, stateModified);
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return updateResult(destination);
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return updateResult(destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        return updateResult(destination);
    }

    private boolean updateResult(long destination) {
        final BigDecimal sumSumValue = sumSum.getResult(destination);
        final long nncValue = nncSum.getResult(destination);

        return updateResult(destination, sumSumValue, nncValue);
    }

    private void updateResult(ReAvgContext reAvgContext, OrderedKeys destinationOk,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigDecimal, ? extends Values> sumSumChunk =
                sumSum.getChunk(reAvgContext.sumSumContext, destinationOk).asObjectChunk();
        final LongChunk<? extends Values> nncSumChunk =
                nncSum.getChunk(reAvgContext.nncSumContext, destinationOk).asLongChunk();
        final int size = reAvgContext.keyIndices.size();
        for (int ii = 0; ii < size; ++ii) {
            stateModified.set(ii,
                    updateResult(reAvgContext.keyIndices.get(ii), sumSumChunk.get(ii), nncSumChunk.get(ii)));
        }
    }

    private boolean updateResult(long destination, BigDecimal sumSumValue, long nncValue) {
        if (nncValue > 0) {
            final BigDecimal newValue = sumSumValue.divide(BigDecimal.valueOf(nncValue), BigDecimal.ROUND_HALF_UP);
            return !newValue.equals(resultColumn.getAndSetUnsafe(destination, newValue));
        } else {
            return null != resultColumn.getAndSetUnsafe(destination, null);
        }
    }

    private class ReAvgContext implements BucketedContext {
        final WritableLongChunk<OrderedKeyIndices> keyIndices;
        final ChunkSource.GetContext sumSumContext;
        final ChunkSource.GetContext nncSumContext;

        private ReAvgContext(int size) {
            keyIndices = WritableLongChunk.makeWritableChunk(size);
            sumSumContext = sumSum.makeGetContext(size);
            nncSumContext = nncSum.makeGetContext(size);
        }

        @Override
        public void close() {
            keyIndices.close();
            sumSumContext.close();
            nncSumContext.close();
        }
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new ReAvgContext(size);
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
