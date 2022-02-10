/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.Nullable;

public abstract class IncrementalChunkedOperatorAggregationStateManagerTypedBase
        extends OperatorAggregationStateManagerTypedBase implements IncrementalOperatorAggregationStateManager {
    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();
    private final LongArraySource rowCountSource = new LongArraySource();
    private final WritableRowRedirection resultIndexToHashSlot =
            new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);

    // state variables that exist as part of the update
    private MutableInt outputPosition;
    private WritableIntChunk<RowKeys> outputPositions;
    @Nullable
    WritableIntChunk<RowKeys> reincarnatedPositions;

    protected IncrementalChunkedOperatorAggregationStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            int tableSize, double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    @Override
    public void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        outputPositions.setSize(rowSequence.intSize());
        buildTable((BuildContext) bc, rowSequence, sources);
        reset();
    }

    @Override
    public void doMainInsert(int tableLocation, int chunkPosition) {
        int position = outputPosition.getAndIncrement();
        outputPositions.set(chunkPosition, position);
        stateSource.set(tableLocation, position);
        outputPositionToHashSlot.set(position, tableLocation);
        rowCountSource.set(position, 1L);
    }

    @Override
    public void moveMain(int oldTableLocation, int newTableLocation) {
        final int position = stateSource.getUnsafe(newTableLocation);
        outputPositionToHashSlot.set(position, newTableLocation);
    }

    @Override
    public void promoteOverflow(int overflowLocation, int mainInsertLocation) {
        outputPositionToHashSlot.set(stateSource.getUnsafe(mainInsertLocation), mainInsertLocation);
    }

    @Override
    public void nextChunk(int size) {
        outputPositionToHashSlot.ensureCapacity(outputPosition.intValue() + size);
        rowCountSource.ensureCapacity(outputPosition.intValue() + size);
    }

    @Override
    public void doMainFound(int tableLocation, int chunkPosition) {
        final int foundPosition = stateSource.getUnsafe(tableLocation);
        outputPositions.set(chunkPosition, foundPosition);

        final long oldRowCount = rowCountSource.getUnsafe(foundPosition);
        Assert.geqZero(oldRowCount, "oldRowCount");
        if (reincarnatedPositions != null && oldRowCount == 0) {
            reincarnatedPositions.add(foundPosition);
        }
        rowCountSource.set(foundPosition, oldRowCount + 1);
    }

    @Override
    public void doOverflowFound(int overflowLocation, int chunkPosition) {
        final int position = overflowStateSource.getUnsafe(overflowLocation);
        outputPositions.set(chunkPosition, position);

        final long oldRowCount = rowCountSource.getUnsafe(position);
        Assert.geqZero(oldRowCount, "oldRowCount");
        if (reincarnatedPositions != null && oldRowCount == 0) {
            reincarnatedPositions.add(position);
        }
        rowCountSource.set(position, oldRowCount + 1);
    }

    @Override
    public void doOverflowInsert(int overflowLocation, int chunkPosition) {
        final int position = outputPosition.getAndIncrement();
        overflowStateSource.set(overflowLocation, position);
        outputPositions.set(chunkPosition, position);
        outputPositionToHashSlot.set(position, HashTableColumnSource.overflowLocationToHashLocation(overflowLocation));
        rowCountSource.set(position, 1L);
    }

    @Override
    public void doMissing(int chunkPosition) {
        throw new IllegalStateException("Failed to find aggregation slot for key!");
//        throw new IllegalStateException("Failed to find aggregation slot for key " + ChunkUtils.extractKeyStringFromChunks(keyChunkTypes, sourceKeyChunks, chunkPosition));
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final WritableRowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[keySources.length];
        for (int kci = 0; kci < keySources.length; ++kci) {
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot,
                    new HashTableColumnSource(keySources[kci], overflowKeySources[kci]));
        }
        return keyHashTableSources;
    }

    @Override
    public void startTrackingPrevValues() {
        resultIndexToHashSlot.startTrackingPrevValues();
    }

    @Override
    public void setRowSize(int outputPosition, long size) {
        rowCountSource.set(outputPosition, size);
    }

    @Override
    public SafeCloseable makeProbeContext(ColumnSource<?>[] probeSources, long maxSize) {
        return null;
    }

    @Override
    public void addForUpdate(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions,
            WritableIntChunk<RowKeys> reincarnatedPositions) {
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        this.reincarnatedPositions = reincarnatedPositions;
        outputPositions.setSize(rowSequence.intSize());
        buildTable((BuildContext) bc, rowSequence, sources);
        reset();
    }

    @Override
    public void remove(final SafeCloseable pc, RowSequence indexToRemove, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> emptiedPositions) {
        reset();
        throw new UnsupportedOperationException();
    }

    @Override
    public void findModifications(final SafeCloseable pc, RowSequence modifiedIndex, ColumnSource<?>[] leftSources,
            WritableIntChunk<RowKeys> outputPositions) {
        reset();
        throw new UnsupportedOperationException();
    }

    private void reset() {
        this.outputPosition = null;
        this.outputPositions = null;
        this.reincarnatedPositions = null;
    }
}