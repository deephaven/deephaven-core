/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util.freezeby;

import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.by.IterativeChunkedAggregationOperator;
import io.deephaven.engine.table.impl.sources.*;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.Map;

public class FreezeByOperator implements IterativeChunkedAggregationOperator {
    private final WritableColumnSource<?> resultSource;
    private final String name;
    private final FreezeByHelper helper;

    public FreezeByOperator(Class<?> type, String resultName, FreezeByCountOperator freezeByCountOperator) {
        resultSource = ArrayBackedColumnSource.getMemoryColumnSource(0, type);
        name = resultName;
        helper = makeHelper(resultSource, freezeByCountOperator);
    }

    @Override
    public void addChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {
        helper.addChunk(values, startPositions, destinations, length);
    }

    @Override
    public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
            IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
            WritableBooleanChunk<Values> stateModified) {}

    @Override
    public void modifyChunk(BucketedContext context, Chunk<? extends Values> previousValues,
            Chunk<? extends Values> newValues, LongChunk<? extends RowKeys> postShiftRowKeys,
            IntChunk<RowKeys> destinations, IntChunk<ChunkPositions> startPositions,
            IntChunk<ChunkLengths> length, WritableBooleanChunk<Values> stateModified) {}

    @Override
    public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        helper.addChunk(values, destination);
        return false;
    }


    @Override
    public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
            LongChunk<? extends RowKeys> inputRowKeys, long destination) {
        return false;
    }

    @Override
    public boolean modifyChunk(SingletonContext context, int chunkSize,
            Chunk<? extends Values> previousValues, Chunk<? extends Values> newValues,
            LongChunk<? extends RowKeys> postShiftRowKeys, long destination) {
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
    public void propagateUpdates(@NotNull TableUpdate downstream,
            @NotNull RowSet newDestinations) {
        if (downstream.removed().isNonempty()) {
            helper.clearIndex(downstream.removed());
        }
    }

    private static FreezeByHelper makeHelper(WritableColumnSource source, FreezeByCountOperator rowCount) {
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
                if (source.getType() == DateTime.class) {
                    return new LongFreezeByHelper(source, rowCount);
                } else if (source.getType() == Boolean.class) {
                    return new BooleanFreezeByHelper(source, rowCount);
                } else {
                    return new ObjectFreezeByHelper(source, rowCount);
                }
        }
    }

    interface FreezeByHelper {
        void addChunk(Chunk<? extends Values> values, IntChunk<ChunkPositions> startPositions,
                IntChunk<RowKeys> destinations, IntChunk<ChunkLengths> length);

        void addChunk(Chunk<? extends Values> values, long destination);

        void clearIndex(RowSequence removed);
    }
}
