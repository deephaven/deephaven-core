package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.NullSafeAddition;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.utils.cast.ToDoubleCast;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.Collections;
import java.util.Map;

class ChunkedWeightedAverageOperator implements IterativeChunkedAggregationOperator {
    private final DoubleWeightRecordingInternalOperator weightOperator;
    private final String resultName;
    private final ChunkType chunkType;

    private long tableSize;
    private final LongArraySource normalCount;
    private LongArraySource nanCount;
    private final DoubleArraySource sumOfWeights;
    private final DoubleArraySource weightedSum;
    private final DoubleArraySource resultColumn;

    ChunkedWeightedAverageOperator(ChunkType chunkType, DoubleWeightRecordingInternalOperator weightOperator,
            String name) {
        this.chunkType = chunkType;
        this.weightOperator = weightOperator;
        this.resultName = name;

        tableSize = 0;
        normalCount = new LongArraySource();
        weightedSum = new DoubleArraySource();
        sumOfWeights = new DoubleArraySource();
        resultColumn = new DoubleArraySource();
    }

    @Override
    public void addChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final Context context = (Context) bucketedContext;
        final DoubleChunk<? extends Values> doubleValues = context.toDoubleCast.cast(values);
        final DoubleChunk<? extends Values> weightValues = weightOperator.getAddedWeights();
        Assert.neqNull(weightValues, "weightValues");
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            stateModified.set(ii, addChunk(doubleValues, weightValues, startPosition, length.get(ii),
                    destinations.get(startPosition)));
        }
    }

    @Override
    public void removeChunk(BucketedContext bucketedContext, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final Context context = (Context) bucketedContext;
        final DoubleChunk<? extends Values> doubleValues = context.prevToDoubleCast.cast(values);
        final DoubleChunk<? extends Values> weightValues = weightOperator.getRemovedWeights();
        Assert.neqNull(weightValues, "weightValues");
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            stateModified.set(ii, removeChunk(doubleValues, weightValues, startPosition, length.get(ii),
                    destinations.get(startPosition)));
        }
    }

    @Override
    public void modifyChunk(BucketedContext bucketedContext, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices,
            IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final Context context = (Context) bucketedContext;
        final DoubleChunk<? extends Values> prevDoubleValues = context.prevToDoubleCast.cast(previousValues);
        final DoubleChunk<? extends Values> prevWeightValues = weightOperator.getRemovedWeights();
        final DoubleChunk<? extends Values> newDoubleValues = context.toDoubleCast.cast(newValues);
        final DoubleChunk<? extends Values> newWeightValues = weightOperator.getAddedWeights();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            stateModified.set(ii, modifyChunk(prevDoubleValues, prevWeightValues, newDoubleValues, newWeightValues,
                    startPosition, length.get(ii), destinations.get(startPosition)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final Context context = (Context) singletonContext;
        final DoubleChunk<? extends Values> doubleValues = context.toDoubleCast.cast(values);
        final DoubleChunk<? extends Values> weightValues = weightOperator.getAddedWeights();
        return addChunk(doubleValues, weightValues, 0, values.size(), destination);
    }

    @Override
    public boolean removeChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends KeyIndices> inputIndices, long destination) {
        final Context context = (Context) singletonContext;
        final DoubleChunk<? extends Values> doubleValues = context.prevToDoubleCast.cast(values);
        final DoubleChunk<? extends Values> weightValues = weightOperator.getRemovedWeights();
        return removeChunk(doubleValues, weightValues, 0, values.size(), destination);
    }

    @Override
    public boolean modifyChunk(SingletonContext singletonContext, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        final Context context = (Context) singletonContext;
        final DoubleChunk<? extends Values> newDoubleValues = context.toDoubleCast.cast(newValues);
        final DoubleChunk<? extends Values> newWeightValues = weightOperator.getAddedWeights();

        final DoubleChunk<? extends Values> prevDoubleValues = context.prevToDoubleCast.cast(previousValues);
        final DoubleChunk<? extends Values> prevWeightValues = weightOperator.getRemovedWeights();

        return modifyChunk(prevDoubleValues, prevWeightValues, newDoubleValues, newWeightValues, 0,
                newDoubleValues.size(), destination);
    }

    private static void sumChunks(DoubleChunk<? extends Values> doubleValues,
            DoubleChunk<? extends Values> weightValues,
            int start,
            int length,
            MutableInt nansOut,
            MutableInt normalOut,
            MutableDouble sumOfWeightsOut,
            MutableDouble weightedSumOut) {
        long nans = 0;
        long normal = 0;
        double sumOfWeights = 0.0;
        double weightedSum = 0.0;

        for (int ii = 0; ii < length; ++ii) {
            final double weight = weightValues.get(start + ii);
            final double component = doubleValues.get(start + ii);

            if (Double.isNaN(weight) || Double.isNaN(component)) {
                nans++;
                continue;
            }

            if (Double.isInfinite(weight) || Double.isInfinite(component)) {
                nans++;
                continue;
            }

            if (weight == QueryConstants.NULL_DOUBLE || component == QueryConstants.NULL_DOUBLE) {
                continue;
            }

            normal++;
            sumOfWeights += weight;
            weightedSum += weight * component;
        }

        nansOut.setValue(nans);
        normalOut.setValue(normal);
        sumOfWeightsOut.setValue(sumOfWeights);
        weightedSumOut.setValue(weightedSum);
    }

    private boolean addChunk(DoubleChunk<? extends Values> doubleValues, DoubleChunk<? extends Values> weightValues,
            int start, int length, long destination) {
        final MutableInt nanOut = new MutableInt();
        final MutableInt normalOut = new MutableInt();
        final MutableDouble sumOfWeightsOut = new MutableDouble();
        final MutableDouble weightedSumOut = new MutableDouble();

        sumChunks(doubleValues, weightValues, start, length, nanOut, normalOut, sumOfWeightsOut, weightedSumOut);

        final long newNans = nanOut.intValue();
        final long newNormal = normalOut.intValue();
        final double newSumOfWeights = sumOfWeightsOut.doubleValue();
        final double newWeightedSum = weightedSumOut.doubleValue();

        final long totalNans;
        if (nanCount == null && newNans > 0) {
            totalNans = allocateNans(destination, newNans);
        } else if (nanCount != null) {
            final long oldNans = nanCount.getUnsafe(destination);
            totalNans = NullSafeAddition.plusLong(oldNans, newNans);
            if (newNans > 0) {
                nanCount.set(destination, totalNans);
            }
        } else {
            totalNans = 0;
        }

        final long totalNormal;
        final long existingNormal = normalCount.getUnsafe(destination);
        totalNormal = NullSafeAddition.plusLong(existingNormal, newNormal);
        Assert.geq(totalNormal, "totalNormal", newNormal, "newNormal");
        if (newNormal > 0) {
            normalCount.set(destination, totalNormal);
        }

        if (totalNormal > 0) {
            final double existingSumOfWeights = sumOfWeights.getUnsafe(destination);
            final double existingWeightedSum = weightedSum.getUnsafe(destination);

            final double totalWeightedSum = NullSafeAddition.plusDouble(existingWeightedSum, newWeightedSum);
            final double totalSumOfWeights = NullSafeAddition.plusDouble(existingSumOfWeights, newSumOfWeights);

            if (newNormal > 0) {
                weightedSum.set(destination, totalWeightedSum);
                sumOfWeights.set(destination, totalSumOfWeights);
            }

            if (totalNans > 0) {
                if (newNans == totalNans) {
                    resultColumn.set(destination, Double.NaN);
                    return true;
                }
                return false;
            } else {
                final double newResult = totalWeightedSum / totalSumOfWeights;
                final double existingResult = resultColumn.getAndSetUnsafe(destination, newResult);
                return newResult != existingResult;
            }
        } else {
            if (totalNans > 0 && totalNans == newNans) {
                resultColumn.set(destination, Double.NaN);
                return true;
            }
            return false;
        }
    }

    private long allocateNans(long destination, long newNans) {
        nanCount = new LongArraySource();
        nanCount.ensureCapacity(tableSize);
        nanCount.set(destination, newNans);
        return newNans;
    }

    private boolean removeChunk(DoubleChunk<? extends Values> doubleValues, DoubleChunk<? extends Values> weightValues,
            int start, int length, long destination) {
        final MutableInt nanOut = new MutableInt();
        final MutableInt normalOut = new MutableInt();
        final MutableDouble sumOfWeightsOut = new MutableDouble();
        final MutableDouble weightedSumOut = new MutableDouble();

        sumChunks(doubleValues, weightValues, start, length, nanOut, normalOut, sumOfWeightsOut, weightedSumOut);

        final int newNans = nanOut.intValue();
        final int newNormal = normalOut.intValue();
        final double newSumOfWeights = sumOfWeightsOut.doubleValue();
        final double newWeightedSum = weightedSumOut.doubleValue();

        final long totalNans;
        if (newNans > 0) {
            final long oldNans = nanCount.getUnsafe(destination);
            totalNans = NullSafeAddition.minusLong(oldNans, newNans);
            nanCount.set(destination, totalNans);
        } else if (nanCount != null) {
            totalNans = nanCount.getUnsafe(destination);
        } else {
            totalNans = 0;
        }

        final long totalNormal;
        final long existingNormal = normalCount.getUnsafe(destination);
        if (newNormal > 0) {
            totalNormal = NullSafeAddition.minusLong(existingNormal, newNormal);
            normalCount.set(destination, totalNormal);
        } else {
            totalNormal = NullSafeAddition.plusLong(existingNormal, 0);
        }
        Assert.geqZero(totalNormal, "totalNormal");

        final double totalWeightedSum;
        final double totalSumOfWeights;
        if (newNormal > 0) {
            if (totalNormal == 0) {
                weightedSum.set(destination, totalWeightedSum = 0.0);
                sumOfWeights.set(destination, totalSumOfWeights = 0.0);
            } else {
                final double existingSumOfWeights = sumOfWeights.getUnsafe(destination);
                final double existingWeightedSum = weightedSum.getUnsafe(destination);
                totalWeightedSum = existingWeightedSum - newWeightedSum;
                totalSumOfWeights = existingSumOfWeights - newSumOfWeights;
                weightedSum.set(destination, totalWeightedSum);
                sumOfWeights.set(destination, totalSumOfWeights);
            }
        } else {
            totalWeightedSum = weightedSum.getUnsafe(destination);
            totalSumOfWeights = sumOfWeights.getUnsafe(destination);
        }

        if (totalNans > 0) {
            // if we had nans before and removed some, but not all nothing could have changed
            return false;
        } else if (totalNormal == 0) {
            if (newNans > 0 || newNormal > 0) {
                resultColumn.set(destination, QueryConstants.NULL_DOUBLE);
                return true;
            }
            return false;
        } else {
            final double newResult = totalWeightedSum / totalSumOfWeights;
            final double existingResult = resultColumn.getAndSetUnsafe(destination, newResult);
            return newResult != existingResult;
        }
    }

    private boolean modifyChunk(DoubleChunk<? extends Values> prevDoubleValues,
            DoubleChunk<? extends Values> prevWeightValues, DoubleChunk<? extends Values> newDoubleValues,
            DoubleChunk<? extends Values> newWeightValues, int start, int length, long destination) {
        final MutableInt nanOut = new MutableInt();
        final MutableInt normalOut = new MutableInt();
        final MutableDouble sumOfWeightsOut = new MutableDouble();
        final MutableDouble weightedSumOut = new MutableDouble();

        sumChunks(prevDoubleValues, prevWeightValues, start, length, nanOut, normalOut, sumOfWeightsOut,
                weightedSumOut);

        final int prevNans = nanOut.intValue();
        final int prevNormal = normalOut.intValue();
        final double prevSumOfWeights = sumOfWeightsOut.doubleValue();
        final double prevWeightedSum = weightedSumOut.doubleValue();

        sumChunks(newDoubleValues, newWeightValues, start, length, nanOut, normalOut, sumOfWeightsOut, weightedSumOut);

        final int newNans = nanOut.intValue();
        final int newNormal = normalOut.intValue();
        final double newSumOfWeights = sumOfWeightsOut.doubleValue();
        final double newWeightedSum = weightedSumOut.doubleValue();


        final long totalNans;
        if (nanCount == null && newNans > 0) {
            totalNans = allocateNans(destination, newNans);
        } else if (nanCount != null) {
            final long oldNans = nanCount.getUnsafe(destination);
            totalNans = NullSafeAddition.plusLong(oldNans, newNans - prevNans);
            if (newNans != prevNans) {
                nanCount.set(destination, totalNans);
            }
        } else {
            totalNans = 0;
        }

        final long totalNormal;
        final long existingNormal = normalCount.getUnsafe(destination);
        totalNormal = NullSafeAddition.plusLong(existingNormal, newNormal - prevNormal);
        Assert.geq(totalNormal, "totalNormal", newNormal, "newNormal");
        if (newNormal != prevNormal) {
            normalCount.set(destination, totalNormal);
        }

        if (totalNormal > 0) {
            final double existingSumOfWeights = sumOfWeights.getUnsafe(destination);
            final double existingWeightedSum = weightedSum.getUnsafe(destination);

            final double totalWeightedSum =
                    NullSafeAddition.plusDouble(existingWeightedSum, newWeightedSum - prevWeightedSum);
            final double totalSumOfWeights =
                    NullSafeAddition.plusDouble(existingSumOfWeights, newSumOfWeights - prevSumOfWeights);

            if (totalWeightedSum != existingWeightedSum) {
                weightedSum.set(destination, totalWeightedSum);
            }
            if (totalSumOfWeights != existingWeightedSum) {
                sumOfWeights.set(destination, totalSumOfWeights);
            }

            if (totalNans > 0) {
                resultColumn.set(destination, Double.NaN);
                return prevNans == 0;
            } else {
                final double newResult = totalWeightedSum / totalSumOfWeights;
                final double existingResult = resultColumn.getAndSetUnsafe(destination, newResult);
                return newResult != existingResult;
            }
        } else {
            if (prevNormal > 0) {
                weightedSum.set(destination, 0.0);
                sumOfWeights.set(destination, 0.0);
            }
            if (totalNans == 0) {
                if (prevNans > 0 || prevNormal > 0) {
                    resultColumn.set(destination, QueryConstants.NULL_DOUBLE);
                    return true;
                }
                return false;
            } else {
                if (prevNans == 0) {
                    resultColumn.set(destination, Double.NaN);
                    return true;
                }
                return false;
            }
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        this.tableSize = tableSize;
        if (nanCount != null) {
            nanCount.ensureCapacity(tableSize);
        }
        normalCount.ensureCapacity(tableSize);
        weightedSum.ensureCapacity(tableSize);
        sumOfWeights.ensureCapacity(tableSize);
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
        private final ToDoubleCast toDoubleCast;
        private final ToDoubleCast prevToDoubleCast;

        private Context(int size) {
            toDoubleCast = ToDoubleCast.makeToDoubleCast(chunkType, size);
            prevToDoubleCast = ToDoubleCast.makeToDoubleCast(chunkType, size);
        }

        @Override
        public void close() {
            toDoubleCast.close();
            prevToDoubleCast.close();
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
