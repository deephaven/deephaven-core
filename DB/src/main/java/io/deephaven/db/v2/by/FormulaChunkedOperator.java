package io.deephaven.db.v2.by;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.Utils;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.select.DhFormulaColumn;
import io.deephaven.db.v2.select.FormulaColumn;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.WritableChunkSink.FillFromContext;
import io.deephaven.db.v2.sources.WritableSource;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkLengths;
import io.deephaven.db.v2.sources.chunk.Attributes.ChunkPositions;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.ChunkSource.GetContext;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;

import static io.deephaven.db.v2.sources.ArrayBackedColumnSource.BLOCK_SIZE;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of
 * {@link io.deephaven.db.tables.Table#applyToAllBy}.
 */
class FormulaChunkedOperator implements IterativeChunkedAggregationOperator {

    private final ByChunkedOperator by;
    private final boolean delegateToBy;
    private final String[] inputColumnNames;
    private final String[] resultColumnNames;

    private final FormulaColumn[] formulaColumns;
    private final ChunkSource<Values>[] formulaDataSources;
    private final WritableSource<?>[] resultColumns;
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
     * @param by The {@link ByChunkedOperator} to use for tracking indices
     * @param delegateToBy Whether this operator is responsible for passing methods through to {@code by}. Should be
     *        false if {@code by} is updated by the helper (and {@code by} must come before this operator if so), or if
     *        this is not the first operator sharing {@code by}.
     * @param formula The formula, before any column name substitutions
     * @param columnParamName The token to substitute column names for
     * @param resultColumnPairs The names for formula input and result columns
     */
    FormulaChunkedOperator(@NotNull final ByChunkedOperator by,
            final boolean delegateToBy,
            @NotNull final String formula,
            @NotNull final String columnParamName,
            @NotNull final MatchPair... resultColumnPairs) {
        this.by = by;
        this.delegateToBy = delegateToBy;
        this.inputColumnNames = MatchPair.getRightColumns(resultColumnPairs);
        this.resultColumnNames = MatchPair.getLeftColumns(resultColumnPairs);

        formulaColumns = new DhFormulaColumn[resultColumnPairs.length];
        // noinspection unchecked
        formulaDataSources = new ChunkSource[resultColumnPairs.length]; // Not populated until propagateInitialState
        resultColumns = new WritableSource[resultColumnPairs.length];
        resultColumnModifiedColumnSets = new ModifiedColumnSet[resultColumnPairs.length]; // Not populated until
                                                                                          // initializeRefreshing
        final Map<String, ? extends ColumnSource<?>> byResultColumns = by.getResultColumns();
        for (int ci = 0; ci < resultColumnPairs.length; ++ci) {
            final String inputColumnName = inputColumnNames[ci];
            final String outputColumnName = resultColumnNames[ci];
            final FormulaColumn formulaColumn = formulaColumns[ci] = FormulaColumn.createFormulaColumn(outputColumnName,
                    Utils.replaceFormulaTokens(formula, columnParamName, inputColumnName));
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
            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            by.addChunk(bucketedContext, values, inputIndices, destinations, startPositions, length, stateModified);
        }
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            by.removeChunk(bucketedContext, values, inputIndices, destinations, startPositions, length, stateModified);
        }
    }

    @Override
    public void modifyChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            by.modifyChunk(bucketedContext, previousValues, newValues, postShiftIndices, destinations, startPositions,
                    length, stateModified);
        }
    }

    @Override
    public void shiftChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> preShiftIndices,
            @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            by.shiftChunk(bucketedContext, previousValues, newValues, preShiftIndices, postShiftIndices, destinations,
                    startPositions, length, stateModified);
        }
    }

    @Override
    public void modifyIndices(final BucketedContext context,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices,
            @NotNull final IntChunk<KeyIndices> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (delegateToBy) {
            by.modifyIndices(context, inputIndices, destinations, startPositions, length, stateModified);
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices, final long destination) {
        if (delegateToBy) {
            return by.addChunk(singletonContext, chunkSize, values, inputIndices, destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends KeyIndices> inputIndices, final long destination) {
        if (delegateToBy) {
            return by.removeChunk(singletonContext, chunkSize, values, inputIndices, destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean modifyChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
            final long destination) {
        if (delegateToBy) {
            return by.modifyChunk(singletonContext, chunkSize, previousValues, newValues, postShiftIndices,
                    destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean shiftChunk(final SingletonContext singletonContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends KeyIndices> preInputIndices,
            @NotNull final LongChunk<? extends KeyIndices> postInputIndices,
            final long destination) {
        if (delegateToBy) {
            return by.shiftChunk(singletonContext, previousValues, newValues, preInputIndices, postInputIndices,
                    destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean modifyIndices(final SingletonContext context, @NotNull final LongChunk<? extends KeyIndices> indices,
            final long destination) {
        if (delegateToBy) {
            return by.modifyIndices(context, indices, destination);
        } else {
            return false;
        }
    }

    @Override
    public boolean requiresIndices() {
        return delegateToBy;
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        if (delegateToBy) {
            by.ensureCapacity(tableSize);
        }
        for (@NotNull
        final WritableSource<?> resultColumn : resultColumns) {
            resultColumn.ensureCapacity(tableSize);
        }
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        final Map<String, WritableSource<?>> resultColumnsMap = new LinkedHashMap<>();
        for (int ci = 0; ci < resultColumnNames.length; ++ci) {
            resultColumnsMap.put(resultColumnNames[ci], resultColumns[ci]);
        }
        return resultColumnsMap;
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        if (delegateToBy) {
            by.propagateInitialState(resultTable);
        }

        final Map<String, ? extends ColumnSource<?>> byResultColumns = by.getResultColumns();
        for (int ci = 0; ci < inputColumnNames.length; ++ci) {
            final String inputColumnName = inputColumnNames[ci];
            final FormulaColumn formulaColumn = formulaColumns[ci];
            final ColumnSource<?> inputColumnSource = byResultColumns.get(inputColumnName);
            formulaColumn.initInputs(resultTable.getIndex(),
                    Collections.singletonMap(inputColumnName, inputColumnSource));
            // noinspection unchecked
            formulaDataSources[ci] = formulaColumn.getDataView();
        }

        final boolean[] allColumnsMask = makeAllColumnsMask();
        try (final DataCopyContext dataCopyContext = new DataCopyContext(allColumnsMask, allColumnsMask)) {
            dataCopyContext.copyData(resultTable.getIndex());
        }
    }

    @Override
    public void startTrackingPrevValues() {
        if (delegateToBy) {
            by.startTrackingPrevValues();
        }
        for (@NotNull
        final WritableSource<?> resultColumn : resultColumns) {
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
            // We cannot use the by's result MCS factory, because the result column names are not guaranteed to be the
            // same.
            by.initializeRefreshing(resultTable, aggregationUpdateListener);
        }
        // Note that we also use the factory in propagateUpdates to identify the set of modified columns to handle.
        return inputToResultModifiedColumnSetFactory =
                by.makeInputToResultModifiedColumnSetFactory(resultTable, resultColumnNames);
    }

    @Override
    public void resetForStep(@NotNull final ShiftAwareListener.Update upstream) {
        if (delegateToBy) {
            by.resetForStep(upstream);
        }
        updateUpstreamModifiedColumnSet =
                upstream.modified.empty() ? ModifiedColumnSet.EMPTY : upstream.modifiedColumnSet;
    }

    @Override
    public void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
            @NotNull final ReadOnlyIndex newDestinations) {
        if (delegateToBy) {
            by.propagateUpdates(downstream, newDestinations);
        }
        final ModifiedColumnSet resultModifiedColumnSet =
                inputToResultModifiedColumnSetFactory.apply(updateUpstreamModifiedColumnSet);
        updateUpstreamModifiedColumnSet = null;

        final boolean addsToProcess = downstream.added.nonempty();
        final boolean modifiesToProcess = downstream.modified.nonempty() && resultModifiedColumnSet.nonempty();
        final boolean removesToProcess = downstream.removed.nonempty();

        if (!addsToProcess && !modifiesToProcess && !removesToProcess) {
            return;
        }

        // Now we know we have some removes.
        if (!addsToProcess && !modifiesToProcess) {
            try (final DataFillerContext dataFillerContext = new DataFillerContext(makeObjectColumnsMask())) {
                dataFillerContext.clearObjectColumnData(downstream.removed);
            }
            return;
        }

        // Now we know we have some adds or modifies.
        final boolean[] modifiedColumnsMask =
                modifiesToProcess ? makeModifiedColumnsMask(resultModifiedColumnSet) : null;
        final boolean[] columnsToFillMask = addsToProcess ? makeAllColumnsMask()
                : removesToProcess ? makeObjectOrModifiedColumnsMask(resultModifiedColumnSet) : modifiedColumnsMask;
        final boolean[] columnsToGetMask = addsToProcess ? columnsToFillMask /*
                                                                              * This is the result of
                                                                              * makeAllColumnsMask() on the line above
                                                                              */ : modifiedColumnsMask;

        try (final DataCopyContext dataCopyContext = new DataCopyContext(columnsToFillMask, columnsToGetMask)) {
            if (removesToProcess) {
                dataCopyContext.clearObjectColumnData(downstream.removed);
            }
            if (modifiesToProcess) {
                dataCopyContext.copyData(downstream.modified, modifiedColumnsMask);
            }
            if (addsToProcess) {
                dataCopyContext.copyData(downstream.added);
            }
        }

    }

    @Override
    public void propagateFailure(@NotNull final Throwable originalException,
            @NotNull final UpdatePerformanceTracker.Entry sourceEntry) {
        if (delegateToBy) {
            by.propagateFailure(originalException, sourceEntry);
        }
    }

    @Override
    public BucketedContext makeBucketedContext(final int size) {
        return delegateToBy ? by.makeBucketedContext(size) : null;
    }

    @Override
    public SingletonContext makeSingletonContext(final int size) {
        return delegateToBy ? by.makeSingletonContext(size) : null;
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

        void clearObjectColumnData(@NotNull final OrderedKeys orderedKeys) {
            try (final OrderedKeys.Iterator orderedKeysIterator = orderedKeys.getOrderedKeysIterator();
                    final WritableObjectChunk<?, Values> nullValueChunk =
                            WritableObjectChunk.makeWritableChunk(BLOCK_SIZE)) {
                nullValueChunk.fillWithNullValue(0, BLOCK_SIZE);
                while (orderedKeysIterator.hasMore()) {
                    final OrderedKeys orderedKeysSlice = orderedKeysIterator.getNextOrderedKeysThrough(
                            calculateContainingBlockLastKey(orderedKeysIterator.peekNextKey()));
                    nullValueChunk.setSize(orderedKeysSlice.intSize());
                    for (int ci = 0; ci < columnsToFillMask.length; ++ci) {
                        final WritableSource<?> resultColumn = resultColumns[ci];
                        if (columnsToFillMask[ci] && !resultColumn.getType().isPrimitive()) {
                            resultColumn.fillFromChunk(fillFromContexts[ci], nullValueChunk, orderedKeysSlice);
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            SafeCloseable.closeArray(fillFromContexts);
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

        private void copyData(@NotNull final OrderedKeys orderedKeys) {
            copyData(orderedKeys, columnsToGetMask);
        }

        private void copyData(@NotNull final OrderedKeys orderedKeys, @NotNull final boolean[] columnsMask) {
            try (final OrderedKeys.Iterator orderedKeysIterator = orderedKeys.getOrderedKeysIterator()) {
                while (orderedKeysIterator.hasMore()) {
                    final OrderedKeys orderedKeysSlice = orderedKeysIterator.getNextOrderedKeysThrough(
                            calculateContainingBlockLastKey(orderedKeysIterator.peekNextKey()));
                    for (int ci = 0; ci < columnsToGetMask.length; ++ci) {
                        if (columnsMask[ci]) {
                            resultColumns[ci].fillFromChunk(fillFromContexts[ci],
                                    formulaDataSources[ci].getChunk(getContexts[ci], orderedKeysSlice),
                                    orderedKeysSlice);
                        }
                    }
                }
            }
        }

        @Override
        public void close() {
            super.close();
            sharedContext.close();
            SafeCloseable.closeArray(getContexts);
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
