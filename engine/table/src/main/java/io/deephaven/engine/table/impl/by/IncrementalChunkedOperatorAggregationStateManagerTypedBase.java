/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.IntColumnSourceWritableRowRedirection;
import io.deephaven.engine.table.impl.util.RowRedirection;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.BuildContext;
import io.deephaven.engine.table.impl.util.TypedHasherUtil.BuildOrProbeContext.ProbeContext;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

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

    // handlers for use during initialization and updates
    private final AddHandler addHandler = new AddHandler();
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
    public void add(
            @NotNull final SafeCloseable bc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final MutableInt nextOutputPosition,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        addHandler.reset(nextOutputPosition, outputPositions);
        buildTable(addHandler, (BuildContext) bc, rowSequence, sources);
        addHandler.reset();
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
    public void remove(
            @NotNull final SafeCloseable pc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        removeHandler.reset(outputPositions);
        probeTable(removeHandler, (ProbeContext) pc, rowSequence, true, sources);
        removeHandler.reset();
    }

    @Override
    public void findModifications(
            @NotNull final SafeCloseable pc,
            @NotNull final RowSequence rowSequence,
            @NotNull final ColumnSource<?>[] sources,
            @NotNull final WritableIntChunk<RowKeys> outputPositions) {
        outputPositions.setSize(rowSequence.intSize());
        if (rowSequence.isEmpty()) {
            return;
        }
        modifyHandler.reset(outputPositions);
        probeTable(modifyHandler, (ProbeContext) pc, rowSequence, false, sources);
        modifyHandler.reset();
    }

    @Override
    protected void ensureMainState(final int tableSize) {
        mainOutputPosition.ensureCapacity(tableSize);
    }

    @Override
    protected void ensureOverflowState(final int newCapacity) {
        overflowOutputPosition.ensureCapacity(newCapacity);
    }

    private class AddHandler extends HashHandler.BuildHandler {

        MutableInt outputPosition;
        WritableIntChunk<RowKeys> outputPositions;

        void reset(
                @NotNull final MutableInt nextOutputPosition,
                @NotNull final WritableIntChunk<RowKeys> outputPositions) {
            this.outputPosition = nextOutputPosition;
            this.outputPositions = outputPositions;
        }

        void reset() {
            outputPosition = null;
            outputPositions = null;
        }

        @Override
        public void doMainInsert(final int tableLocation, final int chunkPosition) {
            final int nextOutputPosition = outputPosition.getAndIncrement();
            outputPositions.set(chunkPosition, nextOutputPosition);
            mainOutputPosition.set(tableLocation, nextOutputPosition);
            outputPositionToHashSlot.set(nextOutputPosition, tableLocation);
        }

        @Override
        public void doMainFound(final int tableLocation, final int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void doMoveMain(final int oldTableLocation, final int newTableLocation) {
            final int outputPosition = mainOutputPosition.getUnsafe(newTableLocation);
            outputPositionToHashSlot.set(outputPosition, newTableLocation);
        }

        @Override
        public void doPromoteOverflow(final int overflowLocation, final int mainInsertLocation) {
            outputPositionToHashSlot.set(mainOutputPosition.getUnsafe(mainInsertLocation), mainInsertLocation);
        }

        @Override
        public void onNextChunk(final int size) {
            outputPositionToHashSlot.ensureCapacity(outputPosition.intValue() + size);
        }

        @Override
        public void doOverflowInsert(final int overflowLocation, final int chunkPosition) {
            final int nextOutputPosition = outputPosition.getAndIncrement();
            outputPositions.set(chunkPosition, nextOutputPosition);
            overflowOutputPosition.set(overflowLocation, nextOutputPosition);
            outputPositionToHashSlot.set(nextOutputPosition,
                    HashTableColumnSource.overflowLocationToHashLocation(overflowLocation));
        }

        @Override
        public void doOverflowFound(final int overflowLocation, final int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }
    }

    class RemoveHandler extends HashHandler.ProbeHandler {

        private WritableIntChunk<RowKeys> outputPositions;

        public void reset(@NotNull final WritableIntChunk<RowKeys> outputPositions) {
            this.outputPositions = outputPositions;
        }

        public void reset() {
            this.outputPositions = null;
        }

        @Override
        public void doMainFound(final int tableLocation, final int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void doOverflowFound(final int overflowLocation, final int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void onNextChunk(final int size) {}

        @Override
        public void doMissing(final int chunkPosition) {
            throw new IllegalStateException();
        }
    }

    class ModifyHandler extends HashHandler.ProbeHandler {

        private WritableIntChunk<RowKeys> outputPositions;

        public void reset(@NotNull final WritableIntChunk<RowKeys> outputPositions) {
            this.outputPositions = outputPositions;
        }

        public void reset() {
            outputPositions = null;
        }

        @Override
        public void doMainFound(final int tableLocation, final int chunkPosition) {
            final int outputPosition = mainOutputPosition.getUnsafe(tableLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void doOverflowFound(final int overflowLocation, final int chunkPosition) {
            final int outputPosition = overflowOutputPosition.getUnsafe(overflowLocation);
            outputPositions.set(chunkPosition, outputPosition);
        }

        @Override
        public void onNextChunk(final int size) {}

        @Override
        public void doMissing(final int chunkPosition) {
            throw new IllegalStateException();
        }
    }
}
