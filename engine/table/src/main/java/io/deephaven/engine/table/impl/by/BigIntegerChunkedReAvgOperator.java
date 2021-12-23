/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sort.timsort.LongIntTimsortKernel;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.impl.util.ChunkUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class BigIntegerChunkedReAvgOperator implements IterativeChunkedAggregationOperator {
    private final ObjectArraySource<BigDecimal> resultColumn;
    private final String name;
    private final BigIntegerChunkedSumOperator sumSum;
    private final LongChunkedSumOperator nncSum;

    BigIntegerChunkedReAvgOperator(String name, BigIntegerChunkedSumOperator sumSum, LongChunkedSumOperator nncSum) {
        this.name = name;
        this.sumSum = sumSum;
        this.nncSum = nncSum;
        resultColumn = new ObjectArraySource<>(BigDecimal.class);
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReAvgContext) context, destinations, startPositions, stateModified);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReAvgContext) context, destinations, startPositions, stateModified);
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        doBucketedUpdate((ReAvgContext) context, destinations, startPositions, stateModified);
    }

    private void doBucketedUpdate(ReAvgContext context, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, WritableBooleanChunk<Values> stateModified) {
        try (final RowSequence destinationSeq = context.destinationSequenceFromChunks(destinations, startPositions)) {
            updateResult(context, destinationSeq, stateModified);
        }
    }

    private static RowSequence destinationSequenceFromChunks(IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, ReAvgContext context) {
        context.keyIndices.setSize(startPositions.size());
        long lastDestination = -1L;
        boolean ordered = true;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int destination = destinations.get(startPosition);
            if (destination < lastDestination) {
                ordered = false;
            }
            context.keyIndices.set(ii, destination);
            lastDestination = destination;
        }
        context.ordered = ordered;
        if (!ordered) {
            context.ensureUnordered();
            context.statePositions.setSize(context.keyIndices.size());
            ChunkUtils.fillInOrder(context.statePositions);
            LongIntTimsortKernel.sort(context.sortKernelContext, context.statePositions, context.keyIndices);
        }
        return RowSequenceFactory.wrapRowKeysChunkAsRowSequence(LongChunk.downcast(context.keyIndices));
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

    private boolean updateResult(long destination) {
        final BigInteger sumSumValue = sumSum.getResult(destination);
        final long nncValue = nncSum.getResult(destination);

        return updateResult(destination, sumSumValue, nncValue);
    }

    private void updateResult(ReAvgContext reAvgContext, RowSequence destinationOk, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigInteger, ? extends Values> sumSumChunk = sumSum.getChunk(reAvgContext.sumSumContext, destinationOk).asObjectChunk();
        final LongChunk<? extends Values> nncSumChunk = nncSum.getChunk(reAvgContext.nncSumContext, destinationOk).asLongChunk();
        final int size = reAvgContext.keyIndices.size();
        if (reAvgContext.ordered) {
            for (int ii = 0; ii < size; ++ii) {
                stateModified.set(ii, updateResult(reAvgContext.keyIndices.get(ii), sumSumChunk.get(ii), nncSumChunk.get(ii)));
            }
        } else {
            for (int ii = 0; ii < size; ++ii) {
                stateModified.set(reAvgContext.statePositions.get(ii), updateResult(reAvgContext.keyIndices.get(ii), sumSumChunk.get(ii), nncSumChunk.get(ii)));
            }
        }
    }

    private boolean updateResult(long destination, BigInteger sumSumValue, long nncValue) {
        if (nncValue > 0) {
            final BigDecimal newValue = new BigDecimal(sumSumValue).divide(BigDecimal.valueOf(nncValue), BigDecimal.ROUND_HALF_UP);
            return !newValue.equals(resultColumn.getAndSetUnsafe(destination, newValue));
        } else {
            return null != resultColumn.getAndSetUnsafe(destination, null);
        }
    }

    private class ReAvgContext extends ReAvgVarOrderingContext implements BucketedContext {
        final ChunkSource.GetContext sumSumContext;
        final ChunkSource.GetContext nncSumContext;

        private ReAvgContext(int size) {
            super(size);
            nncSumContext = nncSum.makeGetContext(size);
            sumSumContext = sumSum.makeGetContext(size);
        }

        @Override
        public void close() {
            nncSumContext.close();
            sumSumContext.close();
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
