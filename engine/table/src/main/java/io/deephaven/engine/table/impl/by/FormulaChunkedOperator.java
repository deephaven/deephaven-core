/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.ChunkSource.GetContext;
import io.deephaven.engine.table.impl.select.FormulaUtil;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.select.DhFormulaColumn;
import io.deephaven.engine.table.impl.select.FormulaColumn;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.ChunkSink.FillFromContext;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource.BLOCK_SIZE;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of {@link Table#applyToAllBy}.
 */
class FormulaChunkedOperator implements IterativeChunkedAggregationOperator {

    private final GroupByChunkedOperator groupBy;
    private final boolean delegateToBy;
    private final String[] inputColumnNames;
    private final String[] resultColumnNames;

    private final FormulaColumn[] formulaColumns;
    private final ChunkSource<Values>[] formulaDataSources;
    private final WritableColumnSource<?>[] resultColumns;
    private final ModifiedColumnSet[] resultColumnModifiedColumnSets;

    /**
     * Converts the upstream MCS to a downstream MCS.
     */
    private UnaryOperator<ModifiedColumnSet> inputToResultModifiedColumnSetFactory;

    /**
     * Captures the upstream MCS in resetForStep to use in propagateUpdates.
     */
    private ModifiedColumnSet updateUpstreamModifiedColumnSet;

    /**
     * Construct an operator for applying a formula to a set of aggregation result columns.
     *
     * @param groupBy The {@link GroupByChunkedOperator} to use for tracking indices
     * @param delegateToBy Whether this operator is responsible for passing methods through to {@code groupBy}. Should
     *        be false if {@code groupBy} is updated by the helper (and {@code groupBy} must come before this operator
     *        if so), or if this is not the first operator sharing {@code groupBy}.
     * @param formula The formula, before any column name substitutions
     * @param columnParamName The token to substitute column names for
     * @param resultColumnPairs The names for formula input and result columns
     */
    FormulaChunkedOperator(@NotNull final GroupByChunkedOperator groupBy,
            final boolean delegateToBy,
            @NotNull final String formula,
            @NotNull final String columnParamName,
            @NotNull final MatchPair... resultColumnPairs) {
        this.groupBy = groupBy;
        this.delegateToBy = delegateToBy;
        this.inputColumnNames = MatchPair.getRightColumns(resultColumnPairs);
        this.resultColumnNames = MatchPair.getLeftColumns(resultColumnPairs);

        formulaColumns = new DhFormulaColumn[resultColumnPairs.length];
        // noinspection unchecked
        formulaDataSources = new ChunkSource[resultColumnPairs.length]; // Not populated until propagateInitialState
        resultColumns = new WritableColumnSource[resultColumnPairs.length];
        resultColumnModifiedColumnSets = new ModifiedColumnSet[resultColumnPairs.length]; // Not populated until
                                                                                          // initializeRefreshing
        final Map<String, ? extends ColumnSource<?>> byResultColumns = groupBy.getResultColumns();
        for (int ci = 0; ci < resultColumnPairs.length; ++ci) {
            final String inputColumnName = inputColumnNames[ci];
            final String outputColumnName = resultColumnNames[ci];
            final FormulaColumn formulaColumn = formulaColumns[ci] = FormulaColumn.createFormulaColumn(outputColumnName,
                    FormulaUtil.replaceFormulaTokens(formula, columnParamName, inputColumnName));
            final ColumnSource<?> inputColumnSource = byResultColumns.get(inputColumnName);
            final ColumnDefinition<?> inputColumnDefinition = ColumnDefinition
                    .fromGenericType(inputColumnName, inputColumnSource.getType(),
                            inputColumnSource.getComponentType());
            formulaColumn.initDef(Collections.singletonMap(inputColumnName, inputColumnDefinition));
            // noinspection unchecked
            resultColumns[ci] = ArrayBackedColumnSource.getMemoryColumnSource(0, formulaColumn.getReturnedType());
        }
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
        for (@NotNull
        final WritableColumnSource<?> resultColumn : resultColumns) {
            resultColumn.ensureCapacity(tableSize);
        }
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        final Map<String, WritableColumnSource<?>> resultColumnsMap = new LinkedHashMap<>();
        for (int ci = 0; ci < resultColumnNames.length; ++ci) {
            resultColumnsMap.put(resultColumnNames[ci], resultColumns[ci]);
        }
        return resultColumnsMap;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        if (delegateToBy) {
            groupBy.propagateInitialState(resultTable, startingDestinationsCount);
        }

        final Map<String, ? extends ColumnSource<?>> byResultColumns = groupBy.getResultColumns();
        for (int ci = 0; ci < inputColumnNames.length; ++ci) {
            final String inputColumnName = inputColumnNames[ci];
            final FormulaColumn formulaColumn = formulaColumns[ci];
            final ColumnSource<?> inputColumnSource = byResultColumns.get(inputColumnName);
            formulaColumn.initInputs(resultTable.getRowSet(),
                    Collections.singletonMap(inputColumnName, inputColumnSource));
            // noinspection unchecked
            formulaDataSources[ci] = formulaColumn.getDataView();
        }

        final boolean[] allColumnsMask = makeAllColumnsMask();
        try (final DataCopyContext dataCopyContext = new DataCopyContext(allColumnsMask, allColumnsMask)) {
            dataCopyContext.copyData(resultTable.getRowSet());
        }
    }

    @Override
    public void startTrackingPrevValues() {
        if (delegateToBy) {
            groupBy.startTrackingPrevValues();
        }
        for (@NotNull
        final WritableColumnSource<?> resultColumn : resultColumns) {
            resultColumn.startTrackingPrevValues();
        }
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(@NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        for (int ci = 0; ci < resultColumnNames.length; ++ci) {
            resultColumnModifiedColumnSets[ci] = resultTable.newModifiedColumnSet(resultColumnNames[ci]);
        }
        if (delegateToBy) {
            // We cannot use the groupBy's result MCS factory, because the result column names are not guaranteed to be
            // the
            // same.
            groupBy.initializeRefreshing(resultTable, aggregationUpdateListener);
        }
        // Note that we also use the factory in propagateUpdates to identify the set of modified columns to handle.
        return inputToResultModifiedColumnSetFactory =
                groupBy.makeInputToResultModifiedColumnSetFactory(resultTable, resultColumnNames);
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
            return;
        }

        // Now we know we have some removes.
        if (!addsToProcess && !modifiesToProcess) {
            try (final DataFillerContext dataFillerContext = new DataFillerContext(makeObjectColumnsMask())) {
                dataFillerContext.clearObjectColumnData(downstream.removed());
            }
            return;
        }

        // Now we know we have some adds or modifies.
        final boolean[] modifiedColumnsMask =
                modifiesToProcess ? makeModifiedColumnsMask(resultModifiedColumnSet) : null;
        final boolean[] columnsToFillMask = addsToProcess ? makeAllColumnsMask()
                : removesToProcess ? makeObjectOrModifiedColumnsMask(resultModifiedColumnSet) : modifiedColumnsMask;
        final boolean[] columnsToGetMask = addsToProcess ? columnsToFillMask : modifiedColumnsMask;

        try (final DataCopyContext dataCopyContext = new DataCopyContext(columnsToFillMask, columnsToGetMask)) {
            if (removesToProcess) {
                dataCopyContext.clearObjectColumnData(downstream.removed());
            }
            if (modifiesToProcess) {
                dataCopyContext.copyData(downstream.modified(), modifiedColumnsMask);
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

    private class DataFillerContext implements SafeCloseable {

        private final boolean[] columnsToFillMask;
        final FillFromContext[] fillFromContexts;

        private DataFillerContext(@NotNull final boolean[] columnsToFillMask) {
            this.columnsToFillMask = columnsToFillMask;
            fillFromContexts = new FillFromContext[resultColumnNames.length];
            for (int ci = 0; ci < resultColumnNames.length; ++ci) {
                if (columnsToFillMask[ci]) {
                    fillFromContexts[ci] = resultColumns[ci].makeFillFromContext(BLOCK_SIZE);
                }
            }
        }

        void clearObjectColumnData(@NotNull final RowSequence rowSequence) {
            try (final RowSequence.Iterator RowSequenceIterator = rowSequence.getRowSequenceIterator();
                    final WritableObjectChunk<?, Values> nullValueChunk =
                            WritableObjectChunk.makeWritableChunk(BLOCK_SIZE)) {
                nullValueChunk.fillWithNullValue(0, BLOCK_SIZE);
                while (RowSequenceIterator.hasMore()) {
                    final RowSequence rowSequenceSlice = RowSequenceIterator.getNextRowSequenceThrough(
                            calculateContainingBlockLastKey(RowSequenceIterator.peekNextKey()));
                    nullValueChunk.setSize(rowSequenceSlice.intSize());
                    for (int ci = 0; ci < columnsToFillMask.length; ++ci) {
                        final WritableColumnSource<?> resultColumn = resultColumns[ci];
                        if (columnsToFillMask[ci] && !resultColumn.getType().isPrimitive()) {
                            resultColumn.fillFromChunk(fillFromContexts[ci], nullValueChunk, rowSequenceSlice);
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            SafeCloseable.closeAll(fillFromContexts);
        }
    }

    private class DataCopyContext extends DataFillerContext {

        private final boolean[] columnsToGetMask;
        private final SharedContext sharedContext;
        private final GetContext[] getContexts;

        private DataCopyContext(@NotNull final boolean[] columnsToFillMask, @NotNull final boolean[] columnsToGetMask) {
            super(columnsToFillMask);
            this.columnsToGetMask = columnsToGetMask;
            sharedContext = SharedContext.makeSharedContext();
            getContexts = new GetContext[resultColumnNames.length];
            for (int ci = 0; ci < resultColumnNames.length; ++ci) {
                if (columnsToGetMask[ci]) {
                    getContexts[ci] = formulaDataSources[ci].makeGetContext(BLOCK_SIZE, sharedContext);
                }
            }
        }

        private void copyData(@NotNull final RowSequence rowSequence) {
            copyData(rowSequence, columnsToGetMask);
        }

        private void copyData(@NotNull final RowSequence rowSequence, @NotNull final boolean[] columnsMask) {
            try (final RowSequence.Iterator RowSequenceIterator = rowSequence.getRowSequenceIterator()) {
                while (RowSequenceIterator.hasMore()) {
                    final RowSequence rowSequenceSlice = RowSequenceIterator.getNextRowSequenceThrough(
                            calculateContainingBlockLastKey(RowSequenceIterator.peekNextKey()));
                    for (int ci = 0; ci < columnsToGetMask.length; ++ci) {
                        if (columnsMask[ci]) {
                            resultColumns[ci].fillFromChunk(fillFromContexts[ci],
                                    formulaDataSources[ci].getChunk(getContexts[ci], rowSequenceSlice),
                                    rowSequenceSlice);
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            super.close();
            sharedContext.close();
            SafeCloseable.closeAll(getContexts);
        }
    }

    private static long calculateContainingBlockLastKey(final long firstKey) {
        return (firstKey / BLOCK_SIZE) * BLOCK_SIZE + BLOCK_SIZE - 1;
    }

    private boolean[] makeAllColumnsMask() {
        final boolean[] columnsMask = new boolean[resultColumnNames.length];
        Arrays.fill(columnsMask, true);
        return columnsMask;
    }

    private boolean[] makeObjectColumnsMask() {
        final boolean[] columnsMask = new boolean[resultColumns.length];
        for (int ci = 0; ci < resultColumns.length; ++ci) {
            columnsMask[ci] = !resultColumns[ci].getType().isPrimitive();
        }
        return columnsMask;
    }

    private boolean[] makeModifiedColumnsMask(@NotNull final ModifiedColumnSet resultModifiedColumnSet) {
        final boolean[] columnsMask = new boolean[resultColumnModifiedColumnSets.length];
        for (int ci = 0; ci < resultColumnModifiedColumnSets.length; ++ci) {
            columnsMask[ci] = resultModifiedColumnSet.containsAny(resultColumnModifiedColumnSets[ci]);
        }
        return columnsMask;
    }

    private boolean[] makeObjectOrModifiedColumnsMask(@NotNull final ModifiedColumnSet resultModifiedColumnSet) {
        final boolean[] columnsMask = new boolean[resultColumns.length];
        for (int ci = 0; ci < resultColumns.length; ++ci) {
            columnsMask[ci] = !resultColumns[ci].getType().isPrimitive()
                    || resultModifiedColumnSet.containsAny(resultColumnModifiedColumnSets[ci]);
        }
        return columnsMask;
    }
}
