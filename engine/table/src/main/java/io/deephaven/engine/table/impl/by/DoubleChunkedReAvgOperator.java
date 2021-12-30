/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit FloatChunkedReAvgOperator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;

import java.util.Collections;
import java.util.Map;

/**
 * Iterative average operator.
 */
class DoubleChunkedReAvgOperator implements IterativeChunkedAggregationOperator {
    private final DoubleArraySource resultColumn;
    private final String name;

    private final DoubleChunkedSumOperator sumSum;
    private final LongChunkedSumOperator nncSum;
    private final LongChunkedSumOperator nanSum;
    private final LongChunkedSumOperator picSum;
    private final LongChunkedSumOperator nicSum;

    DoubleChunkedReAvgOperator(String name, DoubleChunkedSumOperator sumSum, LongChunkedSumOperator nncSum, LongChunkedSumOperator nanSum, LongChunkedSumOperator picSum, LongChunkedSumOperator nicSum) {
        this.name = name;
        this.sumSum = sumSum;
        this.nncSum = nncSum;
        this.nanSum = nanSum;
        this.picSum = picSum;
        this.nicSum = nicSum;
        resultColumn = new DoubleArraySource();
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

    private void updateResult(ReAvgContext reAvgContext, RowSequence destinationOk, WritableBooleanChunk<Values> stateModified) {
        final DoubleChunk<Values> sumSumChunk = sumSum.getChunk(reAvgContext.sumContext, destinationOk).asDoubleChunk();
        final LongChunk<? extends Values> nncSumChunk = nncSum.getChunk(reAvgContext.nncContext, destinationOk).asLongChunk();
        final LongChunk<? extends Values> nanSumChunk = nanSum.getChunk(reAvgContext.nanContext, destinationOk).asLongChunk();
        final LongChunk<? extends Values> picSumChunk = picSum.getChunk(reAvgContext.picContext, destinationOk).asLongChunk();
        final LongChunk<? extends Values> nicSumChunk = nicSum.getChunk(reAvgContext.nicContext, destinationOk).asLongChunk();

        final int size = reAvgContext.keyIndices.size();
        final boolean ordered = reAvgContext.ordered;
        for (int ii = 0; ii < size; ++ii) {
            final boolean changed = updateResult(reAvgContext.keyIndices.get(ii), nncSumChunk.get(ii), nanSumChunk.get(ii), picSumChunk.get(ii), nicSumChunk.get(ii), sumSumChunk.get(ii));
            stateModified.set(ordered ? ii : reAvgContext.statePositions.get(ii), changed);
        }
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
        final long nncValue = nncSum.getResult(destination);
        final long nanValue = nanSum.getResult(destination);
        final long picValue = picSum.getResult(destination);
        final long nicValue = nicSum.getResult(destination);
        final double sumSumValue = sumSum.getRunningSum(destination);

        return updateResult(destination, nncValue, nanValue, picValue, nicValue, sumSumValue);
    }

    private boolean updateResult(long destination, long nncValue, long nanValue, long picValue, long nicValue, double sumSumValue) {
        if (nanValue > 0 || (picValue > 0 && nicValue > 0) || nncValue == 0) {
            return !Double.isNaN(resultColumn.getAndSetUnsafe(destination, Double.NaN));
        } else if (picValue > 0) {
            return resultColumn.getAndSetUnsafe(destination, Double.POSITIVE_INFINITY) != Double.POSITIVE_INFINITY;
        } else if (nicValue > 0) {
            return resultColumn.getAndSetUnsafe(destination, Double.NEGATIVE_INFINITY) != Double.NEGATIVE_INFINITY;
        } else {
            final double newValue = (double)(sumSumValue / nncValue);
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

    private class ReAvgContext extends ReAvgVarOrderingContext implements BucketedContext {
        final ChunkSource.GetContext sumContext;
        final ChunkSource.GetContext nncContext;
        final ChunkSource.GetContext nanContext;
        final ChunkSource.GetContext picContext;
        final ChunkSource.GetContext nicContext;

        private ReAvgContext(int size) {
            super(size);
            sumContext = sumSum.makeGetContext(size);
            nncContext = nncSum.makeGetContext(size);
            nanContext = nanSum.makeGetContext(size);
            nicContext = nicSum.makeGetContext(size);
            picContext = picSum.makeGetContext(size);
        }

        @Override
        public void close() {
            super.close();
            sumContext.close();
            nncContext.close();
            nanContext.close();
            picContext.close();
            nicContext.close();
        }
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new ReAvgContext(size);
    }
}
