/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource.BLOCK_SIZE;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of {@link Table#groupBy},
 * {@link io.deephaven.api.agg.spec.AggSpecGroup}, and {@link io.deephaven.api.agg.Aggregation#AggGroup(String...)}.
 */
public final class GroupByChunkedOperator
        extends BasicStateChangeRecorder
        implements IterativeChunkedAggregationOperator {

    private final QueryTable inputTable;
    private final boolean registeredWithHelper;
    private final boolean live;
    private final ObjectArraySource<WritableRowSet> rowSets;
    private final String[] inputColumnNames;
    private final Map<String, AggregateColumnSource<?, ?>> resultColumns;
    private final ModifiedColumnSet resultInputsModifiedColumnSet;

    private boolean stepValuesModified;
    private boolean someKeyHasAddsOrRemoves;
    private boolean someKeyHasModifies;

    GroupByChunkedOperator(@NotNull final QueryTable inputTable, final boolean registeredWithHelper,
            @NotNull final MatchPair... resultColumnPairs) {
        this.inputTable = inputTable;
        this.registeredWithHelper = registeredWithHelper;
        live = inputTable.isRefreshing();
        rowSets = new ObjectArraySource<>(WritableRowSet.class);
        resultColumns = Arrays.stream(resultColumnPairs).collect(Collectors.toMap(MatchPair::leftColumn,
                matchPair -> AggregateColumnSource
                        .make(inputTable.getColumnSource(matchPair.rightColumn()), rowSets),
                Assert::neverInvoked, LinkedHashMap::new));
        inputColumnNames = MatchPair.getRightColumns(resultColumnPairs);
        if (live) {
            resultInputsModifiedColumnSet = inputTable.newModifiedColumnSet(inputColumnNames);
        } else {
            resultInputsModifiedColumnSet = null;
        }
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= startPositions.size() > 0;
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputIndicesAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            addChunk(inputIndicesAsOrdered, startPosition, runLength, destination);
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= startPositions.size() > 0;
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputIndicesAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            removeChunk(inputIndicesAsOrdered, startPosition, runLength, destination);
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void modifyChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> preShiftRowKeys,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(previousValues, "previousValues");
        Assert.eqNull(newValues, "newValues");
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> preShiftIndicesAsOrdered = (LongChunk<OrderedRowKeys>) preShiftRowKeys;
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> postShiftIndicesAsOrdered = (LongChunk<OrderedRowKeys>) postShiftRowKeys;

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            doShift(preShiftIndicesAsOrdered, postShiftIndicesAsOrdered, startPosition, runLength, destination);
        }
    }

    @Override
    public void modifyRowKeys(final BucketedContext context,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (!stepValuesModified) {
            return;
        }
        someKeyHasModifies |= startPositions.size() > 0;
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= chunkSize > 0;
        // noinspection unchecked
        addChunk((LongChunk<OrderedRowKeys>) inputRowKeys, 0, chunkSize, destination);
        return true;
    }

    @Override
    public boolean addRowSet(SingletonContext context, RowSet rowSet, long destination) {
        someKeyHasAddsOrRemoves |= rowSet.isNonempty();
        addRowsToSlot(rowSet, destination);
        return true;
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= chunkSize > 0;
        // noinspection unchecked
        removeChunk((LongChunk<OrderedRowKeys>) inputRowKeys, 0, chunkSize, destination);
        return true;
    }

    @Override
    public boolean modifyChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            final long destination) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(final SingletonContext singletonContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> preShiftRowKeys,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            final long destination) {
        Assert.eqNull(previousValues, "previousValues");
        Assert.eqNull(newValues, "newValues");
        // noinspection unchecked
        doShift((LongChunk<OrderedRowKeys>) preShiftRowKeys, (LongChunk<OrderedRowKeys>) postShiftRowKeys, 0,
                preShiftRowKeys.size(), destination);
        return false;
    }

    @Override
    public boolean modifyRowKeys(final SingletonContext context, @NotNull final LongChunk<? extends RowKeys> rowKeys,
            final long destination) {
        if (!stepValuesModified) {
            return false;
        }
        someKeyHasModifies |= rowKeys.size() > 0;
        return rowKeys.size() != 0;
    }

    private void addChunk(@NotNull final LongChunk<OrderedRowKeys> indices, final int start, final int length,
            final long destination) {
        final WritableRowSet rowSet = rowSetForSlot(destination);
        final boolean wasEmpty = rowSet.isEmpty();
        rowSet.insert(indices, start, length);
        if (wasEmpty && rowSet.isNonempty()) {
            onReincarnated(destination);
        }
    }

    private void addRowsToSlot(@NotNull final RowSet addRowSet, final long destination) {
        final WritableRowSet rowSet = rowSetForSlot(destination);
        final boolean wasEmpty = rowSet.isEmpty();
        rowSet.insert(addRowSet);
        if (wasEmpty && rowSet.isNonempty()) {
            onReincarnated(destination);
        }
    }

    private void removeChunk(@NotNull final LongChunk<OrderedRowKeys> indices, final int start, final int length,
            final long destination) {
        final WritableRowSet rowSet = rowSetForSlot(destination);
        final boolean wasNonEmpty = rowSet.isNonempty();
        rowSet.remove(indices, start, length);
        if (wasNonEmpty && rowSet.isEmpty()) {
            onEmptied(destination);
        }
    }

    private void doShift(@NotNull final LongChunk<OrderedRowKeys> preShiftIndices,
            @NotNull final LongChunk<OrderedRowKeys> postShiftIndices,
            final int startPosition, final int runLength, final long destination) {
        final WritableRowSet rowSet = rowSetForSlot(destination);
        rowSet.remove(preShiftIndices, startPosition, runLength);
        rowSet.insert(postShiftIndices, startPosition, runLength);
    }

    private WritableRowSet rowSetForSlot(final long destination) {
        WritableRowSet rowSet = rowSets.getUnsafe(destination);
        if (rowSet == null) {
            final WritableRowSet empty = RowSetFactory.empty();
            rowSets.set(destination, rowSet = live ? empty.toTracking() : empty);
        }
        return rowSet;
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        rowSets.ensureCapacity(tableSize);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return resultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        // NB: We don't need previous tracking on the indices ColumnSource. It's in destination space, and we never move
        // anything. Nothing should be asking for previous values if they didn't exist previously.
        // indices.startTrackingPrevValues();
        // NB: These are usually (always, as of now) instances of AggregateColumnSource, meaning
        // startTrackingPrevValues() is a no-op.
        resultColumns.values().forEach(ColumnSource::startTrackingPrevValues);
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(@NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        initializeNewIndexPreviousValues(resultTable.getRowSet());
        return registeredWithHelper
                ? new InputToResultModifiedColumnSetFactory(resultTable,
                        resultColumns.keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY))
                : null;
    }

    /**
     * Make a factory that reads an upstream {@link ModifiedColumnSet} and produces a result {@link ModifiedColumnSet}.
     *
     * @param resultTable The result {@link QueryTable}
     * @param resultColumnNames The result column names, which must be parallel to this operator's input column names
     * @return The factory
     */
    UnaryOperator<ModifiedColumnSet> makeInputToResultModifiedColumnSetFactory(@NotNull final QueryTable resultTable,
            @NotNull final String[] resultColumnNames) {
        return new InputToResultModifiedColumnSetFactory(resultTable, resultColumnNames);
    }

    private class InputToResultModifiedColumnSetFactory implements UnaryOperator<ModifiedColumnSet> {

        private final ModifiedColumnSet updateModifiedColumnSet;
        private final ModifiedColumnSet allAggregatedColumns;
        private final ModifiedColumnSet.Transformer aggregatedColumnsTransformer;

        private InputToResultModifiedColumnSetFactory(@NotNull final QueryTable resultTable,
                @NotNull final String[] resultColumnNames) {
            updateModifiedColumnSet = new ModifiedColumnSet(resultTable.getModifiedColumnSetForUpdates());
            allAggregatedColumns = resultTable.newModifiedColumnSet(resultColumnNames);
            aggregatedColumnsTransformer = inputTable.newModifiedColumnSetTransformer(
                    inputColumnNames,
                    Arrays.stream(resultColumnNames).map(resultTable::newModifiedColumnSet)
                            .toArray(ModifiedColumnSet[]::new));
        }

        @Override
        public ModifiedColumnSet apply(@NotNull final ModifiedColumnSet upstreamModifiedColumnSet) {
            if (someKeyHasAddsOrRemoves) {
                return allAggregatedColumns;
            }
            if (someKeyHasModifies) {
                aggregatedColumnsTransformer.clearAndTransform(upstreamModifiedColumnSet, updateModifiedColumnSet);
                return updateModifiedColumnSet;
            }
            return ModifiedColumnSet.EMPTY;
        }
    }

    @Override
    public void resetForStep(@NotNull final TableUpdate upstream) {
        stepValuesModified = upstream.modified().isNonempty() && upstream.modifiedColumnSet().nonempty()
                && upstream.modifiedColumnSet().containsAny(resultInputsModifiedColumnSet);
        someKeyHasAddsOrRemoves = false;
        someKeyHasModifies = false;
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream,
            @NotNull final RowSet newDestinations) {
        initializeNewIndexPreviousValues(newDestinations);
    }

    private void initializeNewIndexPreviousValues(@NotNull final RowSequence newDestinations) {
        if (newDestinations.isEmpty()) {
            return;
        }
        try (final ChunkSource.GetContext indicesGetContext = rowSets.makeGetContext(BLOCK_SIZE);
                final RowSequence.Iterator newDestinationsIterator = newDestinations.getRowSequenceIterator()) {
            while (newDestinationsIterator.hasMore()) {
                final long nextDestination = newDestinationsIterator.peekNextKey();
                final long nextBlockEnd = (nextDestination / BLOCK_SIZE) * BLOCK_SIZE + BLOCK_SIZE - 1;
                // This RowSequence slice should be exactly aligned to a slice of a single data block in indices (since
                // it is an ArrayBackedColumnSource), allowing getChunk to skip a copy.
                final RowSequence newDestinationsSlice =
                        newDestinationsIterator.getNextRowSequenceThrough(nextBlockEnd);
                final ObjectChunk<TrackingWritableRowSet, Values> indicesChunk =
                        rowSets.getChunk(indicesGetContext, newDestinationsSlice).asObjectChunk();
                final int indicesChunkSize = indicesChunk.size();
                for (int ii = 0; ii < indicesChunkSize; ++ii) {
                    indicesChunk.get(ii).initializePreviousValue();
                }
            }
        }
    }

    @Override
    public boolean requiresRowKeys() {
        return true;
    }

    @Override
    public boolean unchunkedRowSet() {
        return true;
    }
}
