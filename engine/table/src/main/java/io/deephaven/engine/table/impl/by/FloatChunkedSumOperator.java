//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.compare.FloatComparisons;
import io.deephaven.engine.util.NullSafeAddition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.DoubleArraySource;
import io.deephaven.engine.table.impl.sources.FloatArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

/**
 * Incremental sum operator.
 */
final class FloatChunkedSumOperator extends FpChunkedNonNormalCounter
        implements IterativeChunkedAggregationOperator, ChunkSource<Values> {
    private final String name;
    // instead of using states, we maintain parallel arrays, which are going to be more efficient
    // memory wise than one state per output row
    private final FloatArraySource resultColumn = new FloatArraySource();
    /**
     * The running sum of each destination. An operator that receives removals and modifications sums only the normal
     * values here, and counts the NaN and infinite values separately.
     */
    private final DoubleArraySource runningSum;
    /**
     * The number of normal values in each destination, or null for an add-only operator.
     */
    private final NonNullCounter nonNullCount;

    private final boolean isAbsolute;
    /**
     * Whether the operator only receives additions. A NaN or infinite value then never leaves a destination, so the
     * result is the IEEE sum of every non-null value and no counts are kept.
     */
    private final boolean addOnly;

    FloatChunkedSumOperator(boolean absolute, boolean addOnly, String name) {
        isAbsolute = absolute;
        this.addOnly = addOnly;
        this.name = name;
        nonNullCount = addOnly ? null : new NonNullCounter();
        // region runningSum
        // an add-only float sum accumulates every non-null value here, in double precision
        runningSum = new DoubleArraySource();
        // endregion runningSum
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> asFloatChunk = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asFloatChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> asFloatChunk = values.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asFloatChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final FloatChunk<? extends Values> preAsFloatChunk = previousValues.asFloatChunk();
        final FloatChunk<? extends Values> postAsFloatChunk = newValues.asFloatChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii,
                    modifyChunk(preAsFloatChunk, postAsFloatChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asFloatChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asFloatChunk(), destination, 0, values.size());
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        return modifyChunk(previousValues.asFloatChunk(), newValues.asFloatChunk(), destination, 0, newValues.size());
    }

    private boolean addChunk(FloatChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNormalCount = new MutableInt(0);
        final MutableInt chunkNanCount = new MutableInt(0);
        final MutableInt chunkInfinityCount = new MutableInt(0);
        final MutableInt chunkMinusInfinityCount = new MutableInt(0);

        final double sum = doSum(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount, chunkInfinityCount,
                chunkMinusInfinityCount);

        if (addOnly) {
            addOnlySum(destination, sum, chunkNormalCount.get(), chunkNanCount.get(), chunkInfinityCount.get(),
                    chunkMinusInfinityCount.get());
            return true;
        }

        final long totalNanCount = updateNanCount(destination, chunkNanCount.get());
        final long totalPositiveInfinityCount = updatePositiveInfinityCount(destination, chunkInfinityCount.get());
        final long totalNegativeInfinityCount =
                updateNegativeInfinityCount(destination, chunkMinusInfinityCount.get());

        if (chunkNormalCount.get() > 0) {
            final long totalNormalCount = nonNullCount.addNonNullUnsafe(destination, chunkNormalCount.get());
            final double newSum = NullSafeAddition.plusDouble(runningSum.getUnsafe(destination), sum);
            runningSum.set(destination, newSum);
            resultColumn.set(destination, currentValueWithSum(totalNormalCount, totalNanCount,
                    totalPositiveInfinityCount, totalNegativeInfinityCount, newSum));
        } else if (chunkNanCount.get() > 0 || chunkInfinityCount.get() > 0
                || chunkMinusInfinityCount.get() > 0) {
            resultColumn.set(destination,
                    currentValueNoSum(totalNanCount, totalPositiveInfinityCount, totalNegativeInfinityCount));
        }

        return true;
    }

    private void addOnlySum(long destination, double normalSum, int normalCount, int nanCount,
            int positiveInfinityCount, int negativeInfinityCount) {
        final double chunkSum;
        if (nanCount > 0 || (positiveInfinityCount > 0 && negativeInfinityCount > 0)) {
            chunkSum = Double.NaN;
        } else if (positiveInfinityCount > 0) {
            chunkSum = Double.POSITIVE_INFINITY;
        } else if (negativeInfinityCount > 0) {
            chunkSum = Double.NEGATIVE_INFINITY;
        } else if (normalCount > 0) {
            chunkSum = normalSum;
        } else {
            return;
        }
        // region addOnlySum
        final double newSum = NullSafeAddition.plusDouble(runningSum.getUnsafe(destination), chunkSum);
        runningSum.set(destination, newSum);
        resultColumn.set(destination, (float) newSum);
        // endregion addOnlySum
    }

    private boolean removeChunk(FloatChunk<? extends Values> values, long destination, int chunkStart, int chunkSize) {
        requireNonNullCount();
        final MutableInt chunkNormalCount = new MutableInt(0);
        final MutableInt chunkNanCount = new MutableInt(0);
        final MutableInt chunkInfinityCount = new MutableInt(0);
        final MutableInt chunkMinusInfinityCount = new MutableInt(0);

        final double sum = doSum(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount, chunkInfinityCount,
                chunkMinusInfinityCount);

        final long totalNanCount = updateNanCount(destination, -chunkNanCount.get());
        final long totalPositiveInfinityCount =
                updatePositiveInfinityCount(destination, -chunkInfinityCount.get());
        final long totalNegativeInfinityCount =
                updateNegativeInfinityCount(destination, -chunkMinusInfinityCount.get());

        if (chunkNormalCount.get() > 0) {
            final long totalNormalCount = nonNullCount.addNonNullUnsafe(destination, -chunkNormalCount.get());
            final double newSum;
            if (totalNormalCount == 0) {
                newSum = 0;
                runningSum.set(destination, 0.0);
            } else {
                newSum = NullSafeAddition.plusDouble(runningSum.getUnsafe(destination), -sum);
                runningSum.set(destination, newSum);
            }
            resultColumn.set(destination, currentValueWithSum(totalNormalCount, totalNanCount,
                    totalPositiveInfinityCount, totalNegativeInfinityCount, newSum));
            return true;
        } else if (chunkNanCount.get() > 0 || chunkInfinityCount.get() > 0
                || chunkMinusInfinityCount.get() > 0) {
            // if we can still determine what our result is based just on the nans and infinities, use it
            final float possibleResult =
                    currentValueNoSum(totalNanCount, totalPositiveInfinityCount, totalNegativeInfinityCount);
            if (possibleResult != QueryConstants.NULL_FLOAT) {
                resultColumn.set(destination, possibleResult);
            } else {
                final long totalNormalCount = nonNullCount.getCountUnsafe(destination);
                if (totalNormalCount == 0) {
                    // no normal values left, so we must set to null float
                    resultColumn.set(destination, QueryConstants.NULL_FLOAT);
                } else {
                    // determine our running sum, and use that as the answer because we have no more non-normals
                    resultColumn.set(destination, (float) runningSum.getUnsafe(destination));
                }
            }
            return true;
        } else {
            return false;
        }
    }

    private boolean modifyChunk(FloatChunk<? extends Values> preValues, FloatChunk<? extends Values> postValues,
            long destination, int chunkStart, int chunkSize) {
        requireNonNullCount();
        final MutableInt preChunkNormalCount = new MutableInt(0);
        final MutableInt preChunkNanCount = new MutableInt(0);
        final MutableInt preChunkInfinityCount = new MutableInt(0);
        final MutableInt preChunkMinusInfinityCount = new MutableInt(0);

        final double preSum = doSum(preValues, chunkStart, chunkSize, preChunkNormalCount, preChunkNanCount,
                preChunkInfinityCount, preChunkMinusInfinityCount);

        final MutableInt postChunkNormalCount = new MutableInt(0);
        final MutableInt postChunkNanCount = new MutableInt(0);
        final MutableInt postChunkInfinityCount = new MutableInt(0);
        final MutableInt postChunkMinusInfinityCount = new MutableInt(0);

        final double postSum = doSum(postValues, chunkStart, chunkSize, postChunkNormalCount, postChunkNanCount,
                postChunkInfinityCount, postChunkMinusInfinityCount);

        final boolean normalCountChange = preChunkNormalCount.get() != postChunkNormalCount.get();
        final boolean nanChange = preChunkNanCount.get() != postChunkNanCount.get();
        final boolean posInfinityChange = preChunkInfinityCount.get() != postChunkInfinityCount.get();
        final boolean negInfinityChange =
                preChunkMinusInfinityCount.get() != postChunkMinusInfinityCount.get();

        final boolean unchangedResult = !nanChange && // The number of NaNs stayed the same
                !posInfinityChange && // The number of +inf stayed the same
                !negInfinityChange && // The number of -inf stayed the same
                (postSum == preSum); // The sum of the chunk being replaced == the sum of the new chunk

        final long preUpdateNonNullCount = nonNullCount.getCountUnsafe(destination);
        boolean changedToSingleValueOrEmpty = false;
        final long totalNormalCount;
        if (normalCountChange) {
            totalNormalCount = nonNullCount.addNonNullUnsafe(destination,
                    postChunkNormalCount.get() - preChunkNormalCount.get());

            if (totalNormalCount == 0 || preUpdateNonNullCount == 0) {
                changedToSingleValueOrEmpty = true;
            }
        } else {
            if (unchangedResult) {
                return false;
            }
            totalNormalCount = nonNullCount.getCountUnsafe(destination);
        }

        final long totalNanCount =
                updateNanCount(destination, preChunkNanCount.get(), postChunkNanCount.get());
        final long totalPositiveInfinityCount = updatePositiveInfinityCount(destination,
                preChunkInfinityCount.get(), postChunkInfinityCount.get());
        final long totalNegativeInfinityCount = updateNegativeInfinityCount(destination,
                preChunkMinusInfinityCount.get(), postChunkMinusInfinityCount.get());

        final float possibleResult =
                currentValueNoSum(totalNanCount, totalPositiveInfinityCount, totalNegativeInfinityCount);

        final double newSum;
        if ((preSum != postSum) || changedToSingleValueOrEmpty) {
            if (totalNormalCount == 0) {
                newSum = 0.0;
            } else if (preUpdateNonNullCount == 0) {
                newSum = postSum;
            } else {
                newSum = NullSafeAddition.plusDouble(runningSum.getUnsafe(destination), postSum - preSum);
            }
            runningSum.set(destination, newSum);
        } else if (possibleResult != QueryConstants.NULL_FLOAT) {
            newSum = Double.NaN; // it doesn't actually matter in this case
        } else {
            newSum = runningSum.getUnsafe(destination);
        }

        final float newValue;
        if (possibleResult != QueryConstants.NULL_FLOAT) {
            newValue = possibleResult;
        } else if (totalNormalCount == 0) {
            newValue = QueryConstants.NULL_FLOAT;
        } else {
            newValue = (float) newSum;
        }
        final float oldValue = resultColumn.getAndSetUnsafe(destination, newValue);
        return !FloatComparisons.eq(oldValue, newValue);
    }

    private float currentValueWithSum(long totalNormalCount, long totalNanCount, long totalPositiveInfinityCount,
            long totalNegativeInfinityCount, double newSum) {
        if (totalNanCount > 0 || (totalPositiveInfinityCount > 0 && totalNegativeInfinityCount > 0)) {
            return Float.NaN;
        }
        if (totalNegativeInfinityCount > 0) {
            return Float.NEGATIVE_INFINITY;
        }
        if (totalPositiveInfinityCount > 0) {
            return Float.POSITIVE_INFINITY;
        }
        if (totalNormalCount == 0) {
            return QueryConstants.NULL_FLOAT;
        }
        return (float) newSum;
    }

    private float currentValueNoSum(long totalNanCount, long totalPositiveInfinityCount,
            long totalNegativeInfinityCount) {
        if (totalNanCount > 0 || (totalPositiveInfinityCount > 0 && totalNegativeInfinityCount > 0)) {
            return Float.NaN;
        }
        if (totalNegativeInfinityCount > 0) {
            return Float.NEGATIVE_INFINITY;
        }
        if (totalPositiveInfinityCount > 0) {
            return Float.POSITIVE_INFINITY;
        }
        return QueryConstants.NULL_FLOAT;
    }

    private double doSum(FloatChunk<? extends Values> values, int chunkStart, int chunkSize,
            MutableInt chunkNormalCount, MutableInt chunkNanCount, MutableInt chunkInfinityCount,
            MutableInt chunkMinusInfinityCount) {
        final double sum;
        if (isAbsolute) {
            sum = SumFloatChunk.sumFloatChunkAbs(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount,
                    chunkInfinityCount);
        } else {
            sum = SumFloatChunk.sumFloatChunk(values, chunkStart, chunkSize, chunkNormalCount, chunkNanCount,
                    chunkInfinityCount, chunkMinusInfinityCount);
        }
        return sum;
    }

    private void requireNonNullCount() {
        if (nonNullCount == null) {
            throw new UnsupportedOperationException("Add-only sum operators do not support removals or modifications");
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        if (nonNullCount != null) {
            nonNullCount.ensureCapacity(tableSize);
        }
        if (runningSum != null) {
            runningSum.ensureCapacity(tableSize);
        }
        ensureNonNormalCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }

    final double getRunningSum(long destination) {
        return runningSum.getDouble(destination);
    }

    final float getResult(long destination) {
        return resultColumn.getFloat(destination);
    }

    @Override
    public ChunkType getChunkType() {
        return resultColumn.getChunkType();
    }

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return resultColumn.getChunk(context, rowSequence);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return resultColumn.getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination,
            @NotNull RowSequence rowSequence) {
        resultColumn.fillChunk(context, destination, rowSequence);
    }

    @Override
    public FillContext makeFillContext(int chunkCapacity, SharedContext sharedContext) {
        return resultColumn.makeFillContext(chunkCapacity, sharedContext);
    }

    @Override
    public GetContext makeGetContext(int chunkCapacity, SharedContext sharedContext) {
        return resultColumn.makeGetContext(chunkCapacity, sharedContext);
    }
}
