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
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Incremental aggregation state manager that is extended by code generated typed hashers.
 */
public abstract class IncrementalChunkedOperatorAggregationStateManagerTypedBase
        extends OperatorAggregationStateManagerTypedBase
        implements IncrementalOperatorAggregationStateManager {
    // our state value used when nothing is there
    protected static final int EMPTY_OUTPUT_POSITION = QueryConstants.NULL_INT;

    // the state value for the bucket, parallel to mainKeySources (the state is an output row key for the aggregation)
    protected final IntegerArraySource mainOutputPosition = new IntegerArraySource();

    // the state value for an overflow entry, parallel with overflowKeySources (the state is an output row key for the
    // aggregation)
    protected final IntegerArraySource overflowOutputPosition = new IntegerArraySource();

    // used as a row redirection for the output key sources
    private final IntegerArraySource outputPositionToHashSlot = new IntegerArraySource();

    // how many values are in each state, addressed by output row key
    private final LongArraySource rowCountSource = new LongArraySource();

    // handlers for use during updates
    private final AddInitialHandler addInitialHandler = new AddInitialHandler();
    private final AddUpdateHandler addUpdateHandler = new AddUpdateHandler();
    private final RemoveHandler removeHandler = new RemoveHandler();
    private final ModifyHandler modifyHandler = new ModifyHandler();

    protected IncrementalChunkedOperatorAggregationStateManagerTypedBase(ColumnSource<?>[] tableKeySources,
            int tableSize, double maximumLoadFactor, double targetLoadFactor) {
        super(tableKeySources, tableSize, maximumLoadFactor, targetLoadFactor);
        ensureCapacity(tableSize);
    }

    @Override
    public SafeCloseable makeAggregationStateBuildContext(ColumnSource<?>[] buildSources, long maxSize) {
        return makeBuildContext(buildSources, maxSize);
    }

    @Override
    public void add(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        addInitialHandler.reset(nextOutputPosition, outputPositions);
        buildTable(addInitialHandler, (BuildContext) bc, rowSequence, sources);
        addInitialHandler.reset();
    }

    @Override
    public ColumnSource[] getKeyHashTableSources() {
        final RowRedirection resultRowKeyToHashSlot =
                new IntColumnSourceWritableRowRedirection(outputPositionToHashSlot);
        final ColumnSource[] keyHashTableSources = new ColumnSource[mainKeySources.length];
        for (int kci = 0; kci < mainKeySources.length; ++kci) {
            // noinspection unchecked
            keyHashTableSources[kci] = new RedirectedColumnSource(resultRowKeyToHashSlot,
                    new HashTableColumnSource(mainKeySources[kci], overflowKeySources[kci]));
        }
        return keyHashTableSources;
    }

    @Override
    public void startTrackingPrevValues() {}

    @Override
    public void beginUpdateCycle() {}

    @Override
    public void setRowSize(int outputPosition, long size) {
        rowCountSource.set(outputPosition, size);
    }

    @Override
    public void addForUpdate(final SafeCloseable bc, RowSequence rowSequence, ColumnSource<?>[] sources,
            MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions,
            WritableIntChunk<RowKeys> reincarnatedPositions) {
        outputPositions.setSize(rowSequence.intSize());
        reincarnatedPositions.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        addUpdateHandler.reset(nextOutputPosition, outputPositions, reincarnatedPositions);
        buildTable(addUpdateHandler, (BuildContext) bc,
                rowSequence, sources);
        addUpdateHandler.reset();
    }

    @Override
    public void remove(final SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> emptiedPositions) {
        outputPositions.setSize(rowSequence.intSize());
        emptiedPositions.setSize(0);
        if (rowSequence.isEmpty()) {
            return;
        }
        removeHandler.reset(outputPositions, emptiedPositions);
        probeTable(removeHandler, (ProbeContext) pc, rowSequence, true,
                sources);
        removeHandler.reset();
    }

    @Override
    public void findModifications(final SafeCloseable pc, RowSequence rowSequence, ColumnSource<?>[] sources,
            WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        modifyHandler.reset(outputPositions);
        probeTable(modifyHandler, (ProbeContext) pc, rowSequence, false, sources);
        modifyHandler.reset();
    }

    @Override
    protected void ensureMainState(int tableSize) {
        mainOutputPosition.ensureCapacity(tableSize);
    }

    @Override
    protected void ensureOverflowState(int newCapacity) {
        overflowOutputPosition.ensureCapacity(newCapacity);
    }

    private abstract class AddHandler extends HashHandler.BuildHandler {
        MutableInt outputPosition;
        WritableIntChunk<RowKeys> outputPositions;

        void reset(MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions) {
            this.outputPosition = nextOutputPosition;
            this.outputPositions = outputPositions;
        }

        void reset() {
            outputPosition = null;
            outputPositions = null;
        }

        @Override
        public void doMainInsert(int tableLocation, int chunkPosition) {
            final int nextOutputPosition = outputPosition.getAndIncrement();
            outputPositions.set(chunkPosition, nextOutputPosition);
            mainOutputPosition.set(tableLocation, nextOutputPosition);
            outputPositionToHashSlot.set(nextOutputPosition, tableLocation);
            rowCountSource.set(nextOutputPosition, 1L);
        }

        @Override
        public void doMoveMain(int oldTableLocation, int newTableLocation) {
            final int outputPosition = mainOutputPosition.getUnsafe(newTableLocation);
            outputPositionToHashSlot.set(outputPosition, newTableLocation);
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
            final int nextOutputPosition = outputPosition.getAndIncrement();
            outputPositions.set(chunkPosition, nextOutputPosition);
            overflowOutputPosition.set(overflowLocation, nextOutputPosition);
            outputPositionToHashSlot.set(nextOutputPosition,
                    HashTableColumnSource.overflowLocationToHashLocation(overflowLocation));
            rowCountSource.set(nextOutputPosition, 1L);
        }
    }

    class AddInitialHandler extends AddHandler {
        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);

            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.gtZero(oldRowCount, "oldRowCount");
            rowCountSource.set(outputPosition, oldRowCount + 1);
        }

        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);

            final long oldRowCount = rowCountSource.getUnsafe(outputPosition);
            Assert.gtZero(oldRowCount, "oldRowCount");
            rowCountSource.set(outputPosition, oldRowCount + 1);
        }
    }

    class AddUpdateHandler extends AddHandler {
        private WritableIntChunk<RowKeys> reincarnatedPositions;

        public void reset(MutableInt nextOutputPosition, WritableIntChunk<RowKeys> outputPositions,
                WritableIntChunk<RowKeys> reincarnatedPositions) {
            super.reset(nextOutputPosition, outputPositions);
            this.reincarnatedPositions = reincarnatedPositions;
        }

        void reset() {
            super.reset();
            reincarnatedPositions = null;
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
        private WritableIntChunk<RowKeys> outputPositions;
        private WritableIntChunk<RowKeys> emptiedPositions;

        public void reset(WritableIntChunk<RowKeys> outputPositions, WritableIntChunk<RowKeys> emptiedPositions) {
            this.outputPositions = outputPositions;
            this.emptiedPositions = emptiedPositions;
        }

        public void reset() {
            this.outputPositions = null;
            this.emptiedPositions = null;
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
        public void onNextChunk(int size) {}

        @Override
        public void doMissing(int chunkPosition) {
            throw new IllegalStateException();
        }
    }

    class ModifyHandler extends HashHandler.ProbeHandler {
        private WritableIntChunk<RowKeys> outputPositions;

        public void reset(WritableIntChunk<RowKeys> outputPositions) {
            this.outputPositions = outputPositions;
        }

        public void reset() {
            outputPositions = null;
        }

        @Override
        public void doMainFound(int tableLocation, int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void doOverflowFound(int overflowLocation, int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
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
