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

/**
 * Incremental aggregation state manager that is extended by code generated typed hashers.
 */
public abstract class IncrementalChunkedOperatorAggregationStateManagerTypedBase
        extends OperatorAggregationStateManagerTypedBase
        implements IncrementalOperatorAggregationStateManager {
    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected final IntegerArraySource mainOutputPosition = new IntegerArraySource();

    // the state value for an overflow entry, parallel with overflowKeySources (the state is an output row key for the
    // aggregation)
    protected final IntegerArraySource overflowOutputPosition = new IntegerArraySource();

    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();
    private final LongArraySource rowCountSource = new LongArraySource();
    private final WritableRowRedirection resultIndexToHashSlot =
            new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);

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
        outputPositions.setSize(rowSequence.intSize());
        buildTable(new AddInitialHandler(nextOutputPosition, outputPositions), (BuildContext) bc, rowSequence, sources);
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final WritableRowRedirection resultIndexToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[mainKeySources.length];
        for (int kci = 0; kci < mainKeySources.length; ++kci) {
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultIndexToHashSlot,
                    new HashTableColumnSource(mainKeySources[kci], overflowKeySources[kci]));
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
        outputPositions.setSize(rowSequence.intSize());
        buildTable(new AddUpdateHandler(nextOutputPosition, outputPositions, reincarnatedPositions), (BuildContext) bc,
                rowSequence, sources);
    }

    @Override
    public void remove(final SafeCloseable pc, RowSequence indexToRemove, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> emptiedPositions) {
        outputPositions.setSize(indexToRemove.intSize());
        probeTable(new RemoveHandler(outputPositions, emptiedPositions), (ProbeContext) pc, indexToRemove, true,
                sources);
    }

    @Override
    public void findModifications(final SafeCloseable pc, RowSequence modifiedIndex, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(modifiedIndex.intSize());
        probeTable(new ModifyHandler(outputPositions), (ProbeContext) pc, modifiedIndex, false, sources);
    }

    @Override
    protected void ensureCapacity(int tableSize) {
        mainOutputPosition.ensureCapacity(tableSize);
        super.ensureCapacity(tableSize);
    }

    @Override
    protected void ensureOverflowState(int newCapacity) {
        overflowOutputPosition.ensureCapacity(newCapacity);
    }

    private abstract class AddHandler extends HashHandler.BuildHandler {
        final MutableInt outputPosition;
        final WritableIntChunk<RowKeys> outputPositions;

        public AddHandler(MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
            this.outputPosition = nextOutputPosition;
            this.outputPositions = outputPositions;
        }

        @Override
        public void doMainInsert(int tableLocation, int chunkPosition) {
            int position = outputPosition.getAndIncrement();
            outputPositions.set(chunkPosition, position);
            mainOutputPosition.set(tableLocation, position);
            outputPositionToHashSlot.set(position, tableLocation);
            rowCountSource.set(position, 1L);
        }

        @Override
        public void doMoveMain(int oldTableLocation, int newTableLocation) {
            final int position = mainOutputPosition.getUnsafe(newTableLocation);
            outputPositionToHashSlot.set(position, newTableLocation);
        }

        @Override
        public void doPromoteOverflow(int overflowLocation, int mainInsertLocation) {
            outputPositionToHashSlot.set(mainOutputPosition.getUnsafe(mainInsertLocation), mainInsertLocation);
        }

        @Override
        public void onNextChunk(int size) {
            outputPositionToHashSlot.ensureCapacity(outputPosition.intValue() + size);
            rowCountSource.ensureCapacity(outputPosition.intValue() + size);
        }

        @Override
        public void doOverflowInsert(int overflowLocation, int chunkPosition) {
            final int position = outputPosition.getAndIncrement();
            overflowOutputPosition.set(overflowLocation, position);
            outputPositions.set(chunkPosition, position);
            outputPositionToHashSlot.set(position,
                    HashTableColumnSource.overflowLocationToHashLocation(overflowLocation));
            rowCountSource.set(position, 1L);
        }
    }

    class AddInitialHandler extends AddHandler {
        public AddInitialHandler(MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
            super(nextOutputPosition, outputPositions);
        }

        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);

            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.geqZero(oldRowCount, "oldRowCount");
            rowCountSource.set(outputPosition, oldRowCount + 1);
        }

        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);

            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.geqZero(oldRowCount, "oldRowCount");
            rowCountSource.set(outputPosition, oldRowCount + 1);
        }
    }

    class AddUpdateHandler extends AddHandler {
        private final WritableIntChunk<RowKeys> reincarnatedPositions;

        public AddUpdateHandler(MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions,
                WritableIntChunk<RowKeys> reincarnatedPositions) {
            super(nextOutputPosition, outputPositions);
            this.reincarnatedPositions = reincarnatedPositions;
            reincarnatedPositions.setSize(0);
        }

        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);

            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.geqZero(oldRowCount, "oldRowCount");
            if (oldRowCount == 0) {
                reincarnatedPositions.add(outputPosition);
            }
            rowCountSource.set(outputPosition, oldRowCount + 1);
        }

        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);

            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.geqZero(oldRowCount, "oldRowCount");
            if (oldRowCount == 0) {
                reincarnatedPositions.add(outputPosition);
            }
            rowCountSource.set(outputPosition, oldRowCount + 1);
        }
    }

    class RemoveHandler extends HashHandler.ProbeHandler {
        private final WritableIntChunk<RowKeys> outputPositions;
        private final WritableIntChunk<RowKeys> emptiedPositions;

        public RemoveHandler(WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> emptiedPositions) {
            this.outputPositions = outputPositions;
            this.emptiedPositions = emptiedPositions;
            this.emptiedPositions.setSize(0);
        }

        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
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
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
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
        public void onNextChunk(int size) {}

        @Override
        public void doMissing(int chunkPosition) {
            throw new IllegalStateException();
        }
    }

    class ModifyHandler extends HashHandler.ProbeHandler {
        private final WritableIntChunk<RowKeys> outputPositions;

        public ModifyHandler(WritableIntChunk<RowKeys> outputPositions) {
            this.outputPositions = outputPositions;
        }

        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void onNextChunk(int size) {}

        @Override
        public void doMissing(int chunkPosition) {
            throw new IllegalStateException();
        }
    }
}
