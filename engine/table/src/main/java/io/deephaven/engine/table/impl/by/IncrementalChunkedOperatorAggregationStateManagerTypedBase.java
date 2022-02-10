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
        extends OperatorAggregationStateManagerTypedBase implements IncrementalOperatorAggregationStateManager, HashHandler {
    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();
    private final LongArraySource rowCountSource = new LongArraySource();
    private final WritableRowRedirection resultIndexToHashSlot =
            new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
    private final HashHandler removeHandler = new RemoveHandler();
    private final HashHandler modifyHandler = new ModifyHandler();

    // state variables that exist as part of the update
    @Nullable
    private MutableInt outputPosition;
    @Nullable
    private WritableIntChunk<RowKeys> outputPositions;
    @Nullable
    WritableIntChunk<RowKeys> reincarnatedPositions;
    @Nullable
    WritableIntChunk<RowKeys> emptiedPositions;

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
        buildTable(this, (BuildContext) bc, rowSequence, sources);
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
        final int outputPosition = stateSource.getUnsafe(tableLocation);
        outputPositions.set(chunkPosition, outputPosition);

        final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
        Assert.geqZero(oldRowCount, "oldRowCount");
        if (reincarnatedPositions != null && oldRowCount == 0) {
            reincarnatedPositions.add(outputPosition);
        }
        rowCountSource.set(outputPosition, oldRowCount + 1);
    }

    @Override
    public void doOverflowFound(int overflowLocation, int chunkPosition) {
        final int outputPosition = overflowStateSource.getUnsafe(overflowLocation);
        outputPositions.set(chunkPosition, outputPosition);

        final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
        Assert.geqZero(oldRowCount, "oldRowCount");
        if (reincarnatedPositions != null && oldRowCount == 0) {
            reincarnatedPositions.add(outputPosition);
        }
        rowCountSource.set(outputPosition, oldRowCount + 1);
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
    public void addForUpdate(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions,
            WritableIntChunk<RowKeys> reincarnatedPositions) {
        if (rowSequence.isEmpty()) {
            return;
        }
        this.outputPosition = nextOutputPosition;
        this.outputPositions = outputPositions;
        this.outputPositions.setSize(rowSequence.intSize());
        this.reincarnatedPositions = reincarnatedPositions;
        this.reincarnatedPositions.setSize(0);
        buildTable(this, (BuildContext) bc, rowSequence, sources);
        reset();
    }

    @Override
    public void remove(final SafeCloseable pc, RowSequence indexToRemove, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> emptiedPositions) {
        this.outputPositions = outputPositions;
        this.outputPositions.setSize(indexToRemove.intSize());
        this.emptiedPositions = emptiedPositions;
        this.emptiedPositions.setSize(0);
        probeTable(removeHandler, (ProbeContext)pc, indexToRemove, true, sources);
        reset();
    }

    @Override
    public void findModifications(final SafeCloseable pc, RowSequence modifiedIndex, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions) {
        reset();
        this.outputPositions = outputPositions;
        this.outputPositions.setSize(modifiedIndex.intSize());
        probeTable(modifyHandler, (ProbeContext)pc, modifiedIndex, false, sources);
        reset();
    }

    private void reset() {
        this.outputPosition = null;
        this.outputPositions = null;
        this.reincarnatedPositions = null;
        this.emptiedPositions = null;
    }

    class RemoveHandler extends HashHandler.ProbeHandler {
        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowStateSource.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);

            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.gtZero(oldRowCount, "oldRowCount");
            if (oldRowCount == 1) {
                emptiedPositions.add(outputPosition);
            }
            rowCountSource.set(outputPosition, oldRowCount - 1);
        }

        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            final int outputPosition = stateSource.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);

            // decrement the row count
            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.gtZero(oldRowCount, "oldRowCount");
            if (oldRowCount == 1) {
                emptiedPositions.add(outputPosition);
            }
            rowCountSource.set(outputPosition, oldRowCount - 1);
        }


        @Override
        public void nextChunk(int size) {
        }

        @Override
        public void doMissing(int chunkPosition) {
            throw new IllegalStateException();
        }
    }

    class ModifyHandler extends HashHandler.ProbeHandler {
        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowStateSource.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            final int outputPosition = stateSource.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void nextChunk(int size) {
        }

        @Override
        public void doMissing(int chunkPosition) {
            throw new IllegalStateException();
        }
    }
}