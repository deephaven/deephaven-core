//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSink.FillFromContext;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.ChunkSource.GetContext;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.SelectColumn;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource.BLOCK_SIZE;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of {@link Table#applyToAllBy}.
 */
class FormulaMultiColumnChunkedOperator implements IterativeChunkedAggregationOperator {

    private final QueryTable inputTable;

    private final GroupByChunkedOperator groupBy;
    private final boolean delegateToBy;
    private final SelectColumn selectColumn;
    private final WritableColumnSource<?> resultColumn;
    private final String[] inputKeyColumns;

    private ChunkSource<Values> formulaDataSource;

    /**
     * Converts the upstream MCS to a downstream MCS.
     */
    private UnaryOperator<ModifiedColumnSet> inputToResultModifiedColumnSetFactory;

    /**
     * Captures the upstream MCS in resetForStep to use in propagateUpdates.
     */
    private ModifiedColumnSet updateUpstreamModifiedColumnSet;

    /**
     * Construct an operator for applying a formula to slot-vectors over an aggregated table..
     *
     * @param groupBy The {@link GroupByChunkedOperator} to use for tracking indices
     * @param delegateToBy Whether this operator is responsible for passing methods through to {@code groupBy}. Should
     *        be false if {@code groupBy} is updated by the helper, or if this is not the first operator sharing
     *        {@code groupBy}.
     * @param selectColumn The formula column that will produce the results
     */
    FormulaMultiColumnChunkedOperator(
            @NotNull final QueryTable inputTable,
            @NotNull final GroupByChunkedOperator groupBy,
            final boolean delegateToBy,
            @NotNull final SelectColumn selectColumn,
            @NotNull final String[] inputKeyColumns) {
        this.inputTable = inputTable;
        this.groupBy = groupBy;
        this.delegateToBy = delegateToBy;
        this.selectColumn = selectColumn;
        this.inputKeyColumns = inputKeyColumns;

        resultColumn = ArrayBackedColumnSource.getMemoryColumnSource(
                0, selectColumn.getReturnedType(), selectColumn.getReturnedComponentType());
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            groupBy.addChunk(bucketedContext, values, inputRowKeys, destinations, startPositions, length,
                    stateModified);
        }
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            groupBy.removeChunk(bucketedContext, values, inputRowKeys, destinations, startPositions, length,
                    stateModified);
        }
    }

    @Override
    public void modifyChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            groupBy.modifyChunk(bucketedContext, previousValues, newValues, postShiftRowKeys, destinations,
                    startPositions,
                    length, stateModified);
        }
    }

    @Override
    public void shiftChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> preShiftRowKeys,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            @NotNull final IntChunk<RowKeys> destinations,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            groupBy.shiftChunk(bucketedContext, previousValues, newValues, preShiftRowKeys, postShiftRowKeys,
                    destinations,
                    startPositions, length, stateModified);
        }
    }

    @Override
    public void modifyRowKeys(final BucketedContext context,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            groupBy.modifyRowKeys(context, inputRowKeys, destinations, startPositions, length, stateModified);
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        if (delegateToBy) {
            return groupBy.addChunk(singletonContext, chunkSize, values, inputRowKeys, destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        if (delegateToBy) {
            return groupBy.removeChunk(singletonContext, chunkSize, values, inputRowKeys, destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean modifyChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            final long destination) {
        if (delegateToBy) {
            return groupBy.modifyChunk(singletonContext, chunkSize, previousValues, newValues, postShiftRowKeys,
                    destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean shiftChunk(final SingletonContext singletonContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> preShiftRowKeys,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            final long destination) {
        if (delegateToBy) {
            return groupBy.shiftChunk(singletonContext, previousValues, newValues, preShiftRowKeys, postShiftRowKeys,
                    destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean modifyRowKeys(final SingletonContext context,
            @NotNull final LongChunk<? extends RowKeys> rowKeys,
            final long destination) {
        if (delegateToBy) {
            return groupBy.modifyRowKeys(context, rowKeys, destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean requiresRowKeys() {
        return delegateToBy;
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        if (delegateToBy) {
            groupBy.ensureCapacity(tableSize);
        }
        resultColumn.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Map.of(selectColumn.getName(), resultColumn);
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        if (delegateToBy) {
            groupBy.propagateInitialState(resultTable, startingDestinationsCount);
        }

        final Map<String, ColumnSource<?>> sourceColumns;
        if (inputKeyColumns.length == 0) {
            // noinspection unchecked
            sourceColumns = (Map<String, ColumnSource<?>>) groupBy.getInputResultColumns();
        } else {
            final Map<String, ColumnSource<?>> columnSourceMap = resultTable.getColumnSourceMap();
            sourceColumns = new HashMap<>(groupBy.getInputResultColumns());
            Arrays.stream(inputKeyColumns).forEach(col -> sourceColumns.put(col, columnSourceMap.get(col)));
        }
        selectColumn.initInputs(resultTable.getRowSet(), sourceColumns);
        formulaDataSource = selectColumn.getDataView();

        try (final DataCopyContext dataCopyContext = new DataCopyContext()) {
            dataCopyContext.copyData(resultTable.getRowSet());
        }
    }

    @Override
    public void startTrackingPrevValues() {
        if (delegateToBy) {
            groupBy.startTrackingPrevValues();
        }
        resultColumn.startTrackingPrevValues();
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(@NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        if (delegateToBy) {
            // We cannot use the groupBy's result MCS factory, because the result column names are not
            // guaranteed to be the same.
            groupBy.initializeRefreshing(resultTable, aggregationUpdateListener);
        }

        // Note that we also use the factory in propagateUpdates to identify the set of modified columns to handle.
        if (selectColumn.getColumns().isEmpty()) {
            return inputToResultModifiedColumnSetFactory = input -> ModifiedColumnSet.EMPTY;
        }
        final ModifiedColumnSet resultMCS = resultTable.newModifiedColumnSet(selectColumn.getName());
        final String[] inputColumnNames = selectColumn.getColumns().toArray(String[]::new);
        final ModifiedColumnSet inputMCS = inputTable.newModifiedColumnSet(inputColumnNames);
        return inputToResultModifiedColumnSetFactory = input -> {
            if (groupBy.getSomeKeyHasAddsOrRemoves() ||
                    (groupBy.getSomeKeyHasModifies() && input.containsAny(inputMCS))) {
                return resultMCS;
            }
            return ModifiedColumnSet.EMPTY;
        };
    }

    @Override
    public void resetForStep(@NotNull final TableUpdate upstream, final int startingDestinationsCount) {
        if (delegateToBy) {
            groupBy.resetForStep(upstream, startingDestinationsCount);
        }
        updateUpstreamModifiedColumnSet =
                upstream.modified().isEmpty() ? ModifiedColumnSet.EMPTY : upstream.modifiedColumnSet();
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream,
            @NotNull final RowSet newDestinations) {
        if (delegateToBy) {
            groupBy.propagateUpdates(downstream, newDestinations);
        }
        final ModifiedColumnSet resultModifiedColumnSet =
                inputToResultModifiedColumnSetFactory.apply(updateUpstreamModifiedColumnSet);
        updateUpstreamModifiedColumnSet = null;

        final boolean addsToProcess = downstream.added().isNonempty();
        final boolean modifiesToProcess = downstream.modified().isNonempty() && resultModifiedColumnSet.nonempty();
        final boolean removesToProcess = downstream.removed().isNonempty();

        if (!addsToProcess && !modifiesToProcess && !removesToProcess) {
            // Nothing to do.
            return;
        }

        if (!addsToProcess && !modifiesToProcess) {
            // Only removes to handle, clear the removed objects.
            if (!resultColumn.getType().isPrimitive()) {
                try (final DataFillerContext dataFillerContext = new DataFillerContext()) {
                    dataFillerContext.clearObjectColumnData(downstream.removed());
                }
            }
            return;
        }

        try (final DataCopyContext dataCopyContext = new DataCopyContext()) {
            // Clear removed objects from the result column.
            if (removesToProcess && !resultColumn.getType().isPrimitive()) {
                dataCopyContext.clearObjectColumnData(downstream.removed());
            }
            if (modifiesToProcess) {
                dataCopyContext.copyData(downstream.modified());
            }
            if (addsToProcess) {
                dataCopyContext.copyData(downstream.added());
            }
        }
    }

    @Override
    public void propagateFailure(@NotNull final Throwable originalException,
            @NotNull final TableListener.Entry sourceEntry) {
        if (delegateToBy) {
            groupBy.propagateFailure(originalException, sourceEntry);
        }
    }

    @Override
    public BucketedContext makeBucketedContext(final int size) {
        return delegateToBy ? groupBy.makeBucketedContext(size) : null;
    }

    @Override
    public SingletonContext makeSingletonContext(final int size) {
        return delegateToBy ? groupBy.makeSingletonContext(size) : null;
    }

    /**
     * Helper class to efficiently clear object data from the result columns.
     */
    private class DataFillerContext implements SafeCloseable {

        final FillFromContext fillFromContext;

        private DataFillerContext() {
            fillFromContext = resultColumn.makeFillFromContext(BLOCK_SIZE);
        }

        void clearObjectColumnData(@NotNull final RowSequence rowSequence) {
            try (final RowSequence.Iterator rowSequenceIterator = rowSequence.getRowSequenceIterator();
                    final WritableObjectChunk<?, Values> nullValueChunk =
                            WritableObjectChunk.makeWritableChunk(BLOCK_SIZE)) {
                nullValueChunk.fillWithNullValue(0, BLOCK_SIZE);
                while (rowSequenceIterator.hasMore()) {
                    final RowSequence rowSequenceSlice = rowSequenceIterator.getNextRowSequenceThrough(
                            calculateContainingBlockLastKey(rowSequenceIterator.peekNextKey()));
                    nullValueChunk.setSize(rowSequenceSlice.intSize());
                    resultColumn.fillFromChunk(fillFromContext, nullValueChunk, rowSequenceSlice);
                }
            }
        }

        @Override
        public void close() {
            fillFromContext.close();
        }
    }

    /**
     * Helper class to efficiently copy data from the formula data source to the result column.
     */
    private class DataCopyContext extends DataFillerContext {

        private final SharedContext sharedContext;
        private final GetContext getContext;

        private DataCopyContext() {
            sharedContext = SharedContext.makeSharedContext();
            getContext = formulaDataSource.makeGetContext(BLOCK_SIZE, sharedContext);
        }

        private void copyData(@NotNull final RowSequence rowSequence) {
            try (final RowSequence.Iterator rowSequenceIterator = rowSequence.getRowSequenceIterator()) {
                while (rowSequenceIterator.hasMore()) {
                    final RowSequence rowSequenceSlice = rowSequenceIterator.getNextRowSequenceThrough(
                            calculateContainingBlockLastKey(rowSequenceIterator.peekNextKey()));
                    resultColumn.fillFromChunk(fillFromContext,
                            formulaDataSource.getChunk(getContext, rowSequenceSlice), rowSequenceSlice);
                    sharedContext.reset();
                }
            }
        }

        @Override
        public void close() {
            super.close();
            sharedContext.close();
            getContext.close();
        }
    }

    private static long calculateContainingBlockLastKey(final long firstKey) {
        return (firstKey / BLOCK_SIZE) * BLOCK_SIZE + BLOCK_SIZE - 1;
    }
}
