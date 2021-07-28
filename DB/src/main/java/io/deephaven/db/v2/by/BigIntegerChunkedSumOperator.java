/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.Collections;
import java.util.Map;

public class BigIntegerChunkedSumOperator implements IterativeChunkedAggregationOperator, ChunkSource<Values> {
    private final String name;
    private final boolean absolute;
    private final ObjectArraySource<BigInteger> resultColumn = new ObjectArraySource<>(BigInteger.class);
    private final NonNullCounter nonNullCount = new NonNullCounter();

    BigIntegerChunkedSumOperator(boolean absolute, String name) {
        this.absolute = absolute;
        this.name = name;
    }

    public static BigInteger plus(BigInteger a, BigInteger b){
        return a == null ? b: (b == null ? a : a.add(b));
    }

    public static BigInteger minus(BigInteger a, BigInteger b){
        return b == null ? a : a == null ? b.negate() : (a.subtract(b));
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigInteger, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, addChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigInteger, ? extends Values> asObjectChunk = values.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, removeChunk(asObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, IntChunk<KeyIndices> destinations, IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {
        final ObjectChunk<BigInteger, ? extends Values> preAsObjectChunk = previousValues.asObjectChunk();
        final ObjectChunk<BigInteger, ? extends Values> postAsObjectChunk = newValues.asObjectChunk();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final long destination = destinations.get(startPosition);
            stateModified.set(ii, modifyChunk(preAsObjectChunk, postAsObjectChunk, destination, startPosition, length.get(ii)));
        }
    }

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return addChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values, LongChunk<? extends KeyIndices> inputIndices, long destination) {
        return removeChunk(values.asObjectChunk(), destination, 0, values.size());
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues, LongChunk<? extends KeyIndices> postShiftIndices, long destination) {
        return modifyChunk(previousValues.asObjectChunk(), newValues.asObjectChunk(), destination, 0, previousValues.size());
    }

    private boolean addChunk(ObjectChunk<BigInteger, ? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final BigInteger partialSum = doSum(values, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.intValue() <= 0) {
            return false;
        }
        final BigInteger oldValue = resultColumn.getUnsafe(destination);
        final boolean changed = oldValue == null || !partialSum.equals(BigInteger.ZERO);
        if (changed) {
            resultColumn.set(destination, plus(oldValue, partialSum));
        }
        nonNullCount.addNonNullUnsafe(destination, chunkNonNull.intValue());
        return changed;
    }

    private BigInteger doSum(ObjectChunk<BigInteger, ? extends Values> values, int chunkStart, int chunkSize, MutableInt chunkNonNull) {
        if (absolute) {
            return SumBigIntegerChunk.sumBigIntegerChunkAbs(values, chunkStart, chunkSize, chunkNonNull);
        } else {
            return SumBigIntegerChunk.sumBigIntegerChunk(values, chunkStart, chunkSize, chunkNonNull);
        }
    }

    private boolean removeChunk(ObjectChunk<BigInteger, ? extends Values> values, long destination, int chunkStart, int chunkSize) {
        final MutableInt chunkNonNull = new MutableInt(0);
        final BigInteger partialSum = doSum(values, chunkStart, chunkSize, chunkNonNull);

        if (chunkNonNull.intValue() <= 0) {
            return false;
        }

        if (nonNullCount.addNonNullUnsafe(destination, -chunkNonNull.intValue()) == 0) {
            resultColumn.set(destination, null);
        } else if (partialSum.equals(BigInteger.ZERO)) {
            return false;
        } else {
            resultColumn.set(destination, minus(resultColumn.getUnsafe(destination), partialSum));
        }

        return true;
    }

    private boolean modifyChunk(ObjectChunk<BigInteger, ? extends Values> preValues, ObjectChunk<BigInteger, ? extends Values> postValues, long destination, int chunkStart, int chunkSize) {
        final MutableInt preChunkNonNull = new MutableInt(0);
        final MutableInt postChunkNonNull = new MutableInt(0);
        final BigInteger prePartialSum = doSum(preValues, chunkStart, chunkSize, preChunkNonNull);
        final BigInteger postPartialSum = doSum(postValues, chunkStart, chunkSize, postChunkNonNull);

        final int nullDifference = postChunkNonNull.intValue() - preChunkNonNull.intValue();

        if (nullDifference != 0) {
            final long newNonNull = nonNullCount.addNonNullUnsafe(destination, nullDifference);
            if (newNonNull == 0) {
                resultColumn.set(destination, null);
                return true;
            }
        }

        final BigInteger difference = postPartialSum.subtract(prePartialSum);
        if (difference.equals(BigInteger.ZERO)) {
            return false;
        }

        resultColumn.set(destination, plus(resultColumn.getUnsafe(destination), difference));
        return true;
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

    public BigInteger getResult(long destination) {
        return resultColumn.get(destination);
    }

    @Override
    public ChunkType getChunkType() {
        return resultColumn.getChunkType();
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        return resultColumn.getChunk(context, orderedKeys);
    }

    @Override
    public Chunk<? extends Values> getChunk(@NotNull GetContext context, long firstKey, long lastKey) {
        return resultColumn.getChunk(context, firstKey, lastKey);
    }

    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> destination, @NotNull OrderedKeys orderedKeys) {
        resultColumn.fillChunk(context, destination, orderedKeys);
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