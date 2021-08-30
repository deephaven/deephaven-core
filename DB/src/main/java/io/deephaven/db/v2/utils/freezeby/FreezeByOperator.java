/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils.freezeby;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.by.IterativeChunkedAggregationOperator;
import io.deephaven.db.v2.sources.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

public class FreezeByOperator implements IterativeChunkedAggregationOperator {
    private final WritableSource<?> resultSource;
    private final String name;
    private final FreezeByHelper helper;

    public FreezeByOperator(Class<?> type, String resultName,
        FreezeByCountOperator freezeByCountOperator) {
        resultSource = ArrayBackedColumnSource.getMemoryColumnSource(0, type);
        name = resultName;
        helper = makeHelper(resultSource, freezeByCountOperator);
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Attributes.Values> values,
        LongChunk<? extends Attributes.KeyIndices> inputIndices,
        IntChunk<Attributes.KeyIndices> destinations,
        IntChunk<Attributes.ChunkPositions> startPositions,
        IntChunk<Attributes.ChunkLengths> length,
        WritableBooleanChunk<Attributes.Values> stateModified) {
        helper.addChunk(values, startPositions, destinations, length);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Attributes.Values> values,
        LongChunk<? extends Attributes.KeyIndices> inputIndices,
        IntChunk<Attributes.KeyIndices> destinations,
        IntChunk<Attributes.ChunkPositions> startPositions,
        IntChunk<Attributes.ChunkLengths> length,
        WritableBooleanChunk<Attributes.Values> stateModified) {}

    @Override
    public void modifyChunk(BucketedContext context,
        Chunk<? extends Attributes.Values> previousValues,
        Chunk<? extends Attributes.Values> newValues,
        LongChunk<? extends Attributes.KeyIndices> postShiftIndices,
        IntChunk<Attributes.KeyIndices> destinations,
        IntChunk<Attributes.ChunkPositions> startPositions,
        IntChunk<Attributes.ChunkLengths> length,
        WritableBooleanChunk<Attributes.Values> stateModified) {}

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize,
        Chunk<? extends Attributes.Values> values,
        LongChunk<? extends Attributes.KeyIndices> inputIndices, long destination) {
        helper.addChunk(values, destination);
        return false;
    }


    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize,
        Chunk<? extends Attributes.Values> values,
        LongChunk<? extends Attributes.KeyIndices> inputIndices, long destination) {
        return false;
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize,
        Chunk<? extends Attributes.Values> previousValues,
        Chunk<? extends Attributes.Values> newValues,
        LongChunk<? extends Attributes.KeyIndices> postShiftIndices, long destination) {
        return false;
    }

    @Override
    public void ensureCapacity(long tableSize) {
        resultSource.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(name, (ColumnSource<?>) resultSource);
    }

    @Override
    public void startTrackingPrevValues() {
        resultSource.startTrackingPrevValues();
    }

    @Override
    public void propagateUpdates(@NotNull ShiftAwareListener.Update downstream,
        @NotNull ReadOnlyIndex newDestinations) {
        if (downstream.removed.nonempty()) {
            helper.clearIndex(downstream.removed);
        }
    }

    private static FreezeByHelper makeHelper(WritableSource source,
        FreezeByCountOperator rowCount) {
        switch (source.getChunkType()) {
            default:
            case Boolean:
                throw new IllegalStateException();
            case Char:
                return new CharFreezeByHelper(source, rowCount);
            case Byte:
                return new ByteFreezeByHelper(source, rowCount);
            case Short:
                return new ShortFreezeByHelper(source, rowCount);
            case Int:
                return new IntFreezeByHelper(source, rowCount);
            case Long:
                return new LongFreezeByHelper(source, rowCount);
            case Float:
                return new FloatFreezeByHelper(source, rowCount);
            case Double:
                return new DoubleFreezeByHelper(source, rowCount);
            case Object:
                if (source.getType() == DBDateTime.class) {
                    return new LongFreezeByHelper(source, rowCount);
                } else if (source.getType() == Boolean.class) {
                    return new BooleanFreezeByHelper(source, rowCount);
                } else {
                    return new ObjectFreezeByHelper(source, rowCount);
                }
        }
    }

    interface FreezeByHelper {
        void addChunk(Chunk<? extends Attributes.Values> values,
            IntChunk<Attributes.ChunkPositions> startPositions,
            IntChunk<Attributes.KeyIndices> destinations, IntChunk<Attributes.ChunkLengths> length);

        void addChunk(Chunk<? extends Attributes.Values> values, long destination);

        void clearIndex(OrderedKeys removed);
    }
}
