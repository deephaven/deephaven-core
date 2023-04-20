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
import org.jetbrains.annotations.Nullable;

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
public final class GroupByChunkedOperator implements IterativeChunkedAggregationOperator {

    private final QueryTable inputTable;
    private final boolean registeredWithHelper;
    private final String exposeRowSetsAs;

    private final boolean live;
    private final ObjectArraySource<WritableRowSet> rowSets;
    private final ObjectArraySource<Object> addedBuilders;
    private final ObjectArraySource<Object> removedBuilders;

    private final String[] inputColumnNames;
    private final Map<String, AggregateColumnSource<?, ?>> resultAggregatedColumns;
    private final ModifiedColumnSet aggregationInputsModifiedColumnSet;

    private RowSetBuilderRandom stepDestinationsModified;

    private boolean stepValuesModified;
    private boolean someKeyHasAddsOrRemoves;
    private boolean someKeyHasModifies;
    private boolean initialized;

    public GroupByChunkedOperator(
            @NotNull final QueryTable inputTable,
            final boolean registeredWithHelper,
            @Nullable final String exposeRowSetsAs,
            @NotNull final MatchPair... aggregatedColumnPairs) {
        this.inputTable = inputTable;
        this.registeredWithHelper = registeredWithHelper;
        this.exposeRowSetsAs = exposeRowSetsAs;

        live = inputTable.isRefreshing();
        rowSets = new ObjectArraySource<>(WritableRowSet.class);
        addedBuilders = new ObjectArraySource<>(Object.class);
        resultAggregatedColumns = Arrays.stream(aggregatedColumnPairs).collect(Collectors.toMap(MatchPair::leftColumn,
                matchPair -> AggregateColumnSource
                        .make(inputTable.getColumnSource(matchPair.rightColumn()), rowSets),
                Assert::neverInvoked, LinkedHashMap::new));
        inputColumnNames = MatchPair.getRightColumns(aggregatedColumnPairs);
        if (live) {
            aggregationInputsModifiedColumnSet = inputTable.newModifiedColumnSet(inputColumnNames);
            removedBuilders = new ObjectArraySource<>(Object.class);
        } else {
            aggregationInputsModifiedColumnSet = null;
            removedBuilders = null;
        }
        initialized = false;
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        someKeyHasAddsOrRemoves |= startPositions.size() > 0;
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            addChunk(inputRowKeysAsOrdered, startPosition, runLength, destination);
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
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            removeChunk(inputRowKeysAsOrdered, startPosition, runLength, destination);
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
        final LongChunk<OrderedRowKeys> preShiftRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) preShiftRowKeys;
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> postShiftRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) postShiftRowKeys;

        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            doShift(preShiftRowKeysAsOrdered, postShiftRowKeysAsOrdered, startPosition, runLength, destination);
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

    private void addChunk(@NotNull final LongChunk<OrderedRowKeys> rowKeys, final int start, final int length,
            final long destination) {
        if (length == 0) {
            return;
        }
        if (!initialized) {
            // during initialization, all rows are guaranteed to be in-order
            accumulateToBuilderSequential(addedBuilders, rowKeys, start, length, destination);
        } else {
            accumulateToBuilderRandom(addedBuilders, rowKeys, start, length, destination);
        }
        if (stepDestinationsModified != null) {
            stepDestinationsModified.addKey(destination);
        }
    }

    private void addRowsToSlot(@NotNull final RowSet addRowSet, final long destination) {
        if (addRowSet.isEmpty()) {
            return;
        }
        if (!initialized) {
            // during initialization, all rows are guaranteed to be in-order
            accumulateToBuilderSequential(addedBuilders, addRowSet, destination);
        } else {
            accumulateToBuilderRandom(addedBuilders, addRowSet, destination);
        }
    }

    private void removeChunk(@NotNull final LongChunk<OrderedRowKeys> rowKeys, final int start, final int length,
            final long destination) {
        if (length == 0) {
            return;
        }
        accumulateToBuilderRandom(removedBuilders, rowKeys, start, length, destination);
        stepDestinationsModified.addKey(destination);
    }

    private void doShift(@NotNull final LongChunk<OrderedRowKeys> preShiftRowKeys,
            @NotNull final LongChunk<OrderedRowKeys> postShiftRowKeys,
            final int startPosition, final int runLength, final long destination) {
        // treat shift as remove + add
        removeChunk(preShiftRowKeys, startPosition, runLength, destination);
        addChunk(postShiftRowKeys, startPosition, runLength, destination);
    }

    private static void accumulateToBuilderSequential(
            @NotNull final ObjectArraySource<Object> builderColumn,
            @NotNull final LongChunk<OrderedRowKeys> rowKeysToAdd,
            final int start, final int length, final long destination) {
        final RowSetBuilderSequential builder = (RowSetBuilderSequential) builderColumn.getUnsafe(destination);
        if (builder == null) {
            // create (and store) a new builder, fill with these keys
            final RowSetBuilderSequential newBuilder = RowSetFactory.builderSequential();
            newBuilder.appendOrderedRowKeysChunk(rowKeysToAdd, start, length);
            builderColumn.set(destination, newBuilder);
            return;
        }
        // add the keys to the stored builder
        builder.appendOrderedRowKeysChunk(rowKeysToAdd, start, length);
    }

    private static void accumulateToBuilderSequential(
            @NotNull final ObjectArraySource<Object> builderColumn,
            @NotNull final RowSet rowSetToAdd, final long destination) {
        final RowSetBuilderSequential builder = (RowSetBuilderSequential) builderColumn.getUnsafe(destination);
        if (builder == null) {
            // create (and store) a new builder, fill with this rowset
            final RowSetBuilderSequential newBuilder = RowSetFactory.builderSequential();
            newBuilder.appendRowSequence(rowSetToAdd);
            builderColumn.set(destination, newBuilder);
            return;
        }
        // add the rowset to the stored builder
        builder.appendRowSequence(rowSetToAdd);
    }


    private static void accumulateToBuilderRandom(@NotNull final ObjectArraySource<Object> builderColumn,
            @NotNull final LongChunk<OrderedRowKeys> rowKeysToAdd,
            final int start, final int length, final long destination) {
        final RowSetBuilderRandom builder = (RowSetBuilderRandom) builderColumn.getUnsafe(destination);
        if (builder == null) {
            // create (and store) a new builder, fill with these keys
            final RowSetBuilderRandom newBuilder = RowSetFactory.builderRandom();
            newBuilder.addOrderedRowKeysChunk(rowKeysToAdd, start, length);
            builderColumn.set(destination, newBuilder);
            return;
        }
        // add the keys to the stored builder
        builder.addOrderedRowKeysChunk(rowKeysToAdd, start, length);
    }

    private static void accumulateToBuilderRandom(@NotNull final ObjectArraySource<Object> builderColumn,
            @NotNull final RowSet rowSetToAdd, final long destination) {
        final RowSetBuilderRandom builder = (RowSetBuilderRandom) builderColumn.getUnsafe(destination);
        if (builder == null) {
            // create (and store) a new builder, fill with this rowset
            final RowSetBuilderRandom newBuilder = RowSetFactory.builderRandom();
            newBuilder.addRowSet(rowSetToAdd);
            builderColumn.set(destination, newBuilder);
            return;
        }
        // add the rowset to the stored builder
        builder.addRowSet(rowSetToAdd);
    }

    private static WritableRowSet extractAndClearBuilderRandom(
            @NotNull final WritableObjectChunk<RowSetBuilderRandom, Values> builderChunk,
            final int offset) {
        final RowSetBuilderRandom builder = builderChunk.get(offset);
        if (builder != null) {
            final WritableRowSet rowSet = builder.build();
            builderChunk.set(offset, null);
            return rowSet;
        }
        return null;
    }

    private static WritableRowSet extractAndClearBuilderSequential(
            @NotNull final WritableObjectChunk<RowSetBuilderSequential, Values> builderChunk,
            final int offset) {
        final RowSetBuilderSequential builder = builderChunk.get(offset);
        if (builder != null) {
            final WritableRowSet rowSet = builder.build();
            builderChunk.set(offset, null);
            return rowSet;
        }
        return null;
    }

    private static WritableRowSet nullToEmpty(@Nullable final WritableRowSet rowSet) {
        return rowSet == null ? RowSetFactory.empty() : rowSet;
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        rowSets.ensureCapacity(tableSize);
        addedBuilders.ensureCapacity(tableSize);
        if (live) {
            removedBuilders.ensureCapacity(tableSize);
        }
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        if (exposeRowSetsAs != null) {
            final Map<String, ColumnSource<?>> allResultColumns =
                    new LinkedHashMap<>(resultAggregatedColumns.size() + 1);
            allResultColumns.put(exposeRowSetsAs, rowSets);
            allResultColumns.putAll(resultAggregatedColumns);
            return allResultColumns;
        }
        return resultAggregatedColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        // NB: We don't need previous tracking on the rowSets ColumnSource, even if it's exposed. It's in destination
        // space, and we never move anything. Nothing should be asking for previous values if they didn't exist
        // previously.
        // NB: These are usually (always, as of now) instances of AggregateColumnSource, meaning
        // startTrackingPrevValues() is a no-op.
        resultAggregatedColumns.values().forEach(ColumnSource::startTrackingPrevValues);
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(
            @NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        initializeNewRowSetPreviousValues(resultTable.getRowSet());
        return registeredWithHelper
                ? new InputToResultModifiedColumnSetFactory(resultTable,
                        resultAggregatedColumns.keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY))
                : null;
    }

    /**
     * Make a factory that reads an upstream {@link ModifiedColumnSet} and produces a result {@link ModifiedColumnSet}.
     *
     * @param resultTable The result {@link QueryTable}
     * @param resultColumnNames The result column names, which must be parallel to this operator's input column names
     * @return The factory
     */
    UnaryOperator<ModifiedColumnSet> makeInputToResultModifiedColumnSetFactory(
            @NotNull final QueryTable resultTable,
            @NotNull final String[] resultColumnNames) {
        return new InputToResultModifiedColumnSetFactory(resultTable, resultColumnNames);
    }

    private class InputToResultModifiedColumnSetFactory implements UnaryOperator<ModifiedColumnSet> {

        private final ModifiedColumnSet updateModifiedColumnSet;
        private final ModifiedColumnSet allResultColumns;
        private final ModifiedColumnSet.Transformer aggregatedColumnsTransformer;

        private InputToResultModifiedColumnSetFactory(
                @NotNull final QueryTable resultTable,
                @NotNull final String[] resultAggregatedColumnNames) {
            updateModifiedColumnSet = new ModifiedColumnSet(resultTable.getModifiedColumnSetForUpdates());
            allResultColumns = resultTable.newModifiedColumnSet(resultAggregatedColumnNames);
            if (exposeRowSetsAs != null) {
                allResultColumns.setAll(exposeRowSetsAs);
            }
            aggregatedColumnsTransformer = inputTable.newModifiedColumnSetTransformer(
                    inputColumnNames,
                    Arrays.stream(resultAggregatedColumnNames).map(resultTable::newModifiedColumnSet)
                            .toArray(ModifiedColumnSet[]::new));
        }

        @Override
        public ModifiedColumnSet apply(@NotNull final ModifiedColumnSet upstreamModifiedColumnSet) {
            if (someKeyHasAddsOrRemoves) {
                return allResultColumns;
            }
            if (someKeyHasModifies) {
                aggregatedColumnsTransformer.clearAndTransform(upstreamModifiedColumnSet, updateModifiedColumnSet);
                return updateModifiedColumnSet;
            }
            return ModifiedColumnSet.EMPTY;
        }
    }

    @Override
    public void resetForStep(@NotNull final TableUpdate upstream, final int startingDestinationsCount) {
        stepValuesModified = upstream.modified().isNonempty() && upstream.modifiedColumnSet().nonempty()
                && upstream.modifiedColumnSet().containsAny(aggregationInputsModifiedColumnSet);
        someKeyHasAddsOrRemoves = false;
        someKeyHasModifies = false;
        stepDestinationsModified = new BitmapRandomBuilder(startingDestinationsCount);
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        Assert.neqTrue(initialized, "initialized");

        // use the builders to create the initial rowsets
        try (final RowSet initialDestinations = RowSetFactory.flat(startingDestinationsCount);
                final ResettableWritableObjectChunk<WritableRowSet, Values> rowSetResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderSequential, Values> addedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator destinationsIterator =
                        initialDestinations.getRowSequenceIterator()) {

            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> rowSetBackingChunk =
                    rowSetResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderSequential, Values> addedBuildersBackingChunk =
                    addedBuildersResettableChunk.asWritableObjectChunk();

            while (destinationsIterator.hasMore()) {
                final long firstSliceDestination = destinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        rowSets.resetWritableChunkToBackingStore(rowSetResettableChunk, firstSliceDestination);
                addedBuilders.resetWritableChunkToBackingStore(addedBuildersResettableChunk, firstSliceDestination);

                final long lastBackingChunkDestination =
                        firstBackingChunkDestination + rowSetBackingChunk.size() - 1;
                final RowSequence initialDestinationsSlice =
                        destinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                initialDestinationsSlice.forAllRowKeys((final long destination) -> {
                    final int backingChunkOffset =
                            Math.toIntExact(destination - firstBackingChunkDestination);
                    final WritableRowSet addRowSet = nullToEmpty(
                            extractAndClearBuilderSequential(addedBuildersBackingChunk, backingChunkOffset));
                    rowSetBackingChunk.set(backingChunkOffset, live ? addRowSet.toTracking() : addRowSet);
                });
            }
        }
        initialized = true;
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream, @NotNull final RowSet newDestinations) {
        // get the rowset for the updated items
        try (final WritableRowSet stepDestinations = stepDestinationsModified.build()) {
            // add the new destinations so a rowset will get created if it doesn't exist
            stepDestinations.insert(newDestinations);

            if (stepDestinations.isEmpty()) {
                return;
            }

            // use the builders to modify the rowsets
            try (final ResettableWritableObjectChunk<WritableRowSet, Values> rowSetResettableChunk =
                    ResettableWritableObjectChunk.makeResettableChunk();
                    final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersResettableChunk =
                            ResettableWritableObjectChunk.makeResettableChunk();
                    final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersResettableChunk =
                            ResettableWritableObjectChunk.makeResettableChunk();
                    final RowSequence.Iterator destinationsIterator =
                            stepDestinations.getRowSequenceIterator()) {

                // noinspection unchecked
                final WritableObjectChunk<WritableRowSet, Values> rowSetBackingChunk =
                        rowSetResettableChunk.asWritableObjectChunk();
                // noinspection unchecked
                final WritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersBackingChunk =
                        addedBuildersResettableChunk.asWritableObjectChunk();
                // noinspection unchecked
                final WritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersBackingChunk =
                        removedBuildersResettableChunk.asWritableObjectChunk();

                while (destinationsIterator.hasMore()) {
                    final long firstSliceDestination = destinationsIterator.peekNextKey();
                    final long firstBackingChunkDestination =
                            rowSets.resetWritableChunkToBackingStore(rowSetResettableChunk, firstSliceDestination);
                    addedBuilders.resetWritableChunkToBackingStore(addedBuildersResettableChunk,
                            firstSliceDestination);
                    removedBuilders.resetWritableChunkToBackingStore(removedBuildersResettableChunk,
                            firstSliceDestination);

                    final long lastBackingChunkDestination =
                            firstBackingChunkDestination + rowSetBackingChunk.size() - 1;
                    final RowSequence initialDestinationsSlice =
                            destinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                    initialDestinationsSlice.forAllRowKeys((final long destination) -> {
                        final int backingChunkOffset =
                                Math.toIntExact(destination - firstBackingChunkDestination);
                        final WritableRowSet workingRowSet = rowSetBackingChunk.get(backingChunkOffset);
                        if (workingRowSet == null) {
                            // use the addRowSet as the new rowset
                            final WritableRowSet addRowSet = nullToEmpty(
                                    extractAndClearBuilderRandom(addedBuildersBackingChunk, backingChunkOffset));
                            rowSetBackingChunk.set(backingChunkOffset, live ? addRowSet.toTracking() : addRowSet);
                        } else {
                            try (final WritableRowSet addRowSet =
                                    nullToEmpty(extractAndClearBuilderRandom(addedBuildersBackingChunk,
                                            backingChunkOffset));
                                    final WritableRowSet removeRowSet =
                                            nullToEmpty(extractAndClearBuilderRandom(removedBuildersBackingChunk,
                                                    backingChunkOffset))) {
                                workingRowSet.remove(removeRowSet);
                                workingRowSet.insert(addRowSet);
                            }
                        }
                    });
                }
            }
            stepDestinationsModified = null;
        }
        initializeNewRowSetPreviousValues(newDestinations);
    }

    private void initializeNewRowSetPreviousValues(@NotNull final RowSequence newDestinations) {
        if (newDestinations.isEmpty()) {
            return;
        }
        try (final ChunkSource.GetContext rowSetsGetContext = rowSets.makeGetContext(BLOCK_SIZE);
                final RowSequence.Iterator newDestinationsIterator = newDestinations.getRowSequenceIterator()) {
            while (newDestinationsIterator.hasMore()) {
                final long nextDestination = newDestinationsIterator.peekNextKey();
                final long nextBlockEnd = (nextDestination / BLOCK_SIZE) * BLOCK_SIZE + BLOCK_SIZE - 1;
                // This RowSequence slice should be exactly aligned to a slice of a single data block in rowsets (since
                // it is an ArrayBackedColumnSource), allowing getChunk to skip a copy.
                final RowSequence newDestinationsSlice =
                        newDestinationsIterator.getNextRowSequenceThrough(nextBlockEnd);
                final ObjectChunk<TrackingWritableRowSet, Values> rowSetsChunk =
                        rowSets.getChunk(rowSetsGetContext, newDestinationsSlice).asObjectChunk();
                final int rowSetsChunkSize = rowSetsChunk.size();
                for (int ii = 0; ii < rowSetsChunkSize; ++ii) {
                    rowSetsChunk.get(ii).initializePreviousValue();
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
