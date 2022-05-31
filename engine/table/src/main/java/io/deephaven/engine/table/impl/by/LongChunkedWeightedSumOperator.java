package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.QueryConstants;
import io.deephaven.engine.util.NullSafeAddition;
import io.deephaven.chunk.util.hashing.ToLongCast;
import io.deephaven.chunk.util.hashing.ToLongFunctor;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Collections;
import java.util.Map;

class LongChunkedWeightedSumOperator implements IterativeChunkedAggregationOperator {
    private final LongWeightRecordingInternalOperator weightOperator;
    private final String resultName;
    private final ChunkType chunkType;

    private final LongArraySource normalCount;
    private final LongArraySource weightedSum;
    private final LongArraySource resultColumn;

    LongChunkedWeightedSumOperator(ChunkType chunkType, LongWeightRecordingInternalOperator weightOperator, String name) {
        this.chunkType = chunkType;
        this.weightOperator = weightOperator;
        this.resultName = name;

        normalCount = new LongArraySource();
        weightedSum = new LongArraySource();
        resultColumn = new LongArraySource();
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final Context context = (Context)bucketedContext;
        final LongChunk<? extends Values> doubleValues = context.toLongCast.apply(values);
        final LongChunk<? extends Values> weightValues = weightOperator.getAddedWeights();
        Assert.neqNull(weightValues, "weightValues");
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            stateModified.set(ii, addChunk(doubleValues, weightValues, startPosition, length.get(ii), destinations.get(startPosition)));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final Context context = (Context)bucketedContext;
        final LongChunk<? extends Values> doubleValues = context.prevToLongCast.apply(values);
        final LongChunk<? extends Values> weightValues = weightOperator.getRemovedWeights();
        Assert.neqNull(weightValues, "weightValues");
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            stateModified.set(ii, removeChunk(doubleValues, weightValues, startPosition, length.get(ii), destinations.get(startPosition)));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final Context context = (Context)bucketedContext;
        final LongChunk<? extends Values> prevDoubleValues = context.prevToLongCast.apply(previousValues);
        final LongChunk<? extends Values> prevWeightValues = weightOperator.getRemovedWeights();
        final LongChunk<? extends Values> newDoubleValues = context.toLongCast.apply(newValues);
        final LongChunk<? extends Values> newWeightValues = weightOperator.getAddedWeights();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            stateModified.set(ii, modifyChunk(prevDoubleValues, prevWeightValues, newDoubleValues, newWeightValues, startPosition, length.get(ii), destinations.get(startPosition)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final Context context = (Context)singletonContext;
        final LongChunk<? extends Values> doubleValues = context.toLongCast.apply(values);
        final LongChunk<? extends Values> weightValues = weightOperator.getAddedWeights();
        return addChunk(doubleValues, weightValues, 0, values.size(), destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        final Context context = (Context)singletonContext;
        final LongChunk<? extends Values> doubleValues = context.prevToLongCast.apply(values);
        final LongChunk<? extends Values> weightValues = weightOperator.getRemovedWeights();
        return removeChunk(doubleValues, weightValues, 0, values.size(), destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        final Context context = (Context)singletonContext;
        final LongChunk<? extends Values> newDoubleValues = context.toLongCast.apply(newValues);
        final LongChunk<? extends Values> newWeightValues = weightOperator.getAddedWeights();

        final LongChunk<? extends Values> prevDoubleValues = context.prevToLongCast.apply(previousValues);
        final LongChunk<? extends Values> prevWeightValues = weightOperator.getRemovedWeights();

        return modifyChunk(prevDoubleValues, prevWeightValues, newDoubleValues, newWeightValues, 0, newDoubleValues.size(), destination);
    }

    private static void sumChunks(LongChunk<? extends Values> doubleValues, LongChunk<? extends Values> weightValues,
                                  int start,
                                  int length,
                                  MutableInt normalOut,
                                  MutableLong weightedSumOut) {
        long normal = 0;
        long weightedSum = 0;

        for (int ii = 0; ii < length; ++ii) {
            final double weight = weightValues.get(start + ii);
            final double component = doubleValues.get(start + ii);

            if (weight == QueryConstants.NULL_DOUBLE || component == QueryConstants.NULL_DOUBLE) {
                continue;
            }

            normal++;
            weightedSum += weight * component;
        }

        normalOut.setValue(normal);
        weightedSumOut.setValue(weightedSum);
    }

    private boolean addChunk(LongChunk<? extends Values> longValues, LongChunk<? extends Values> weightValues, int start, int length, long destination) {
        final MutableInt normalOut = new MutableInt();
        final MutableLong weightedSumOut = new MutableLong();

        sumChunks(longValues, weightValues, start, length, normalOut, weightedSumOut);

        final long newNormal = normalOut.intValue();
        final long newWeightedSum = weightedSumOut.longValue();

        final long totalNormal;
        final long existingNormal = normalCount.getUnsafe(destination);
        totalNormal = NullSafeAddition.plusLong(existingNormal, newNormal);
        Assert.geq(totalNormal, "totalNormal", newNormal, "newNormal");
        if (newNormal > 0) {
            normalCount.set(destination, totalNormal);
        }

        if (totalNormal > 0) {
            final long existingWeightedSum = weightedSum.getUnsafe(destination);
            final long totalWeightedSum = NullSafeAddition.plusLong(existingWeightedSum, newWeightedSum);

            if (newNormal > 0) {
                weightedSum.set(destination, totalWeightedSum);
            }

            final double existingResult = resultColumn.getAndSetUnsafe(destination, totalWeightedSum);
            return totalWeightedSum != existingResult;
        }
        return false;
    }

    private boolean removeChunk(LongChunk<? extends Values> doubleValues, LongChunk<? extends Values> weightValues, int start, int length, long destination) {
        final MutableInt normalOut = new MutableInt();
        final MutableLong weightedSumOut = new MutableLong();

        sumChunks(doubleValues, weightValues, start, length, normalOut, weightedSumOut);

        final int newNormal = normalOut.intValue();
        final long newWeightedSum = weightedSumOut.longValue();

        final long totalNormal;
        final long existingNormal = normalCount.getUnsafe(destination);
        if (newNormal > 0) {
            totalNormal = NullSafeAddition.minusLong(existingNormal, newNormal);
            normalCount.set(destination, totalNormal);
        } else {
            totalNormal = NullSafeAddition.plusLong(existingNormal, 0);
        }
        Assert.geqZero(totalNormal, "totalNormal");

        final long totalWeightedSum;
        if (newNormal > 0) {
            if (totalNormal == 0) {
                weightedSum.set(destination, totalWeightedSum = 0);
            } else {
                final long existingWeightedSum = weightedSum.getUnsafe(destination);
                totalWeightedSum = existingWeightedSum - newWeightedSum;
                weightedSum.set(destination, totalWeightedSum);
            }
        } else {
            totalWeightedSum = weightedSum.getUnsafe(destination);
        }

        if (totalNormal == 0) {
            if (newNormal > 0) {
                resultColumn.set(destination, QueryConstants.NULL_LONG);
                return true;
            }
            return false;
        } else {
            final long existingResult = resultColumn.getAndSetUnsafe(destination, totalWeightedSum);
            return totalWeightedSum != existingResult;
        }
    }

    private boolean modifyChunk(LongChunk<? extends Values> prevDoubleValues, LongChunk<? extends Values> prevWeightValues, LongChunk<? extends Values> newDoubleValues, LongChunk<? extends Values> newWeightValues, int start, int length, long destination) {
        final MutableInt normalOut = new MutableInt();
        final MutableLong weightedSumOut = new MutableLong();

        sumChunks(prevDoubleValues, prevWeightValues, start, length, normalOut, weightedSumOut);

        final int prevNormal = normalOut.intValue();
        final long prevWeightedSum = weightedSumOut.longValue();

        sumChunks(newDoubleValues, newWeightValues, start, length, normalOut, weightedSumOut);

        final int newNormal = normalOut.intValue();
        final long newWeightedSum = weightedSumOut.longValue();

        final long totalNormal;
        final long existingNormal = normalCount.getUnsafe(destination);
        totalNormal = NullSafeAddition.plusLong(existingNormal, newNormal - prevNormal);
        Assert.geq(totalNormal, "totalNormal", newNormal, "newNormal");
        if (newNormal != prevNormal) {
            normalCount.set(destination, totalNormal);
        }

        if (totalNormal > 0) {
            final long existingWeightedSum = weightedSum.getUnsafe(destination);
            final long totalWeightedSum = NullSafeAddition.plusLong(existingWeightedSum, newWeightedSum - prevWeightedSum);

            if (totalWeightedSum != existingWeightedSum) {
                weightedSum.set(destination, totalWeightedSum);
            }

            final double existingResult = resultColumn.getAndSetUnsafe(destination, totalWeightedSum);
            return totalWeightedSum != existingResult;
        } else {
            if (prevNormal > 0) {
                weightedSum.set(destination, 0L);
                resultColumn.set(destination, QueryConstants.NULL_DOUBLE);
                return true;
            }
            return false;
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        normalCount.ensureCapacity(tableSize);
        weightedSum.ensureCapacity(tableSize);
        resultColumn.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(resultName, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }

    private class Context implements BucketedContext, SingletonContext {
        private final ToLongFunctor toLongCast;
        private final ToLongFunctor prevToLongCast;

        private Context(int size) {
            toLongCast = ToLongCast.makeToLongCast(chunkType, size, 0);
            prevToLongCast = ToLongCast.makeToLongCast(chunkType, size, 0);
        }

        @Override
        public void close() {
            toLongCast.close();
            prevToLongCast.close();
        }
    }

    @Override
    public BucketedContext makeBucketedContext(int size) {
        return new Context(size);
    }

    @Override
    public SingletonContext makeSingletonContext(int size) {
        return new Context(size);
    }
}
