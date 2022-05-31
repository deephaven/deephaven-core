/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.SharedContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;

public class BigDecimalChunkedSumOperator implements IterativeChunkedAggregationOperator, ChunkSource<Values> {
    private final String name;
    private final ObjectArraySource<BigDecimal> resultColumn = new ObjectArraySource<>(BigDecimal.class);
    private final NonNullCounter nonNullCount = new NonNullCounter();
    private final boolean isAbsolute;

    BigDecimalChunkedSumOperator(boolean isAbsolute, String name) {
        this.isAbsolute = isAbsolute;
        this.name = name;
    }

    public static BigDecimal plus(BigDecimal a, BigDecimal b) {
        return a == null ? b : (b == null ? a : a.add(b));
    }

    public static BigDecimal minus(BigDecimal a, BigDecimal b) {
        return b == null ? a : a == null ? b.negate() : (a.subtract(b));
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigDecimal, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigDecimal, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigDecimal, ? extends Values> preAsObjectChunk = previousValues.asObjectChunk();
        final ObjectChunk<BigDecimal, ? extends Values> postAsObjectChunk = newValues.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii,
                    modifyChunk(preAsObjectChunk, postAsObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return addChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return removeChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
        return modifyChunk(previousValues.asObjectChunk(), newValues.asObjectChunk(), destination, 0,
                previousValues.size());
    }

    private boolean addChunk(ObjectChunk<BigDecimal, ? extends Values> values, long destination, int chunkStart,
            int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final BigDecimal partialSum = doSum(values, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.intValue() <= 0) {
            return false;
        }
        final BigDecimal oldValue = resultColumn.getUnsafe(destination);
        final boolean changed = oldValue == null || !partialSum.equals(BigDecimal.ZERO);
        if (changed) {
            resultColumn.set(destination, plus(oldValue, partialSum));
        }
        nonNullCount.addNonNullUnsafe(destination, chunkNonNull.intValue());
        return changed;
    }

    private boolean removeChunk(ObjectChunk<BigDecimal, ? extends Values> values, long destination, int chunkStart,
            int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final BigDecimal partialSum = doSum(values, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.intValue() <= 0) {
            return false;
        }
        if (nonNullCount.addNonNullUnsafe(destination, -chunkNonNull.intValue()) == 0) {
            resultColumn.set(destination, null);
        } else if (partialSum.equals(BigDecimal.ZERO)) {
            return false;
        } else {
            resultColumn.set(destination, minus(resultColumn.getUnsafe(destination), partialSum));
        }
        return true;
    }

    private boolean modifyChunk(ObjectChunk<BigDecimal, ? extends Values> preValues,
            ObjectChunk<BigDecimal, ? extends Values> postValues, long destination, int chunkStart, int chunkSize) {
        final MutableInt preChunkNonNull = new MutableInt(0);
        final MutableInt postChunkNonNull = new MutableInt(0);
        final BigDecimal prePartialSum = doSum(preValues, chunkStart, chunkSize, preChunkNonNull);
        final BigDecimal postPartialSum = doSum(postValues, chunkStart, chunkSize, postChunkNonNull);

        final int nullDifference = postChunkNonNull.intValue() - preChunkNonNull.intValue();

        if (nullDifference != 0) {
            final long newNonNull = nonNullCount.addNonNullUnsafe(destination, nullDifference);
            if (newNonNull == 0) {
                resultColumn.set(destination, null);
                return true;
            }
        }

        final BigDecimal difference = postPartialSum.subtract(prePartialSum);
        if (difference.equals(BigDecimal.ZERO)) {
            return false;
        }

        resultColumn.set(destination, plus(resultColumn.getUnsafe(destination), difference));
        return true;
    }

    private BigDecimal doSum(ObjectChunk<BigDecimal, ? extends Values> values, int chunkStart, int chunkSize,
            MutableInt chunkNonNull) {
        if (isAbsolute) {
            return SumBigDecimalChunk.sumBigDecimalChunkAbs(values, chunkStart, chunkSize, chunkNonNull);
        } else {
            return SumBigDecimalChunk.sumBigDecimalChunk(values, chunkStart, chunkSize, chunkNonNull);
        }
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultColumn.ensureCapacity(tableSize);
        nonNullCount.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(name, resultColumn);
    }

    @Override
    public void startTrackingPrevValues() {
        resultColumn.startTrackingPrevValues();
    }

    final BigDecimal getResult(long destination) {
        return resultColumn.get(destination);
    }

    @Override
    public ChunkType getChunkType() {
        return resultColumn.getChunkType();
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
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
