//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.MatchPair;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.UnaryOperator;

import static io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource.BLOCK_SIZE;

/**
 * An {@link IterativeChunkedAggregationOperator} used to re-aggregate the results of an
 * {@link io.deephaven.api.agg.Aggregation#AggGroup(String...) AggGroup} as part of a rollup.
 *
 * <p>
 * The operator is fundamentally different than the {@link GroupByChunkedOperator}. Rather than examining row keys, it
 * listens to the rollup's base (or intermediate) level and reads the exposed RowSet column. The relevant RowSets are
 * added to a random builder for each state while processing an update (or initialization). At the end of the update
 * cycle, it builds the rowsets and updates an internal ObjectArraySource of RowSets.
 * </p>
 *
 * <p>
 * The resulting column sources are once again {@link AggregateColumnSource}, which reuse the wrapped aggregated column
 * source from the source table (thus each level of the rollup uses the original table's sources as the input to the
 * AggregateColumnSources -- not the immediately prior level).
 * </p>
 */
public final class GroupByReaggregateOperator implements GroupByOperator {

    private final QueryTable inputTable;
    private final boolean registeredWithHelper;
    private final String exposeRowSetsAs;
    private final MatchPair[] aggregatedColumnPairs;
    private final List<String> hiddenResults;

    private final boolean live;
    private final ObjectArraySource<WritableRowSet> rowSets;
    private final ObjectArraySource<Object> addedBuilders;
    private final ObjectArraySource<Object> removedBuilders;

    private final String[] inputColumnNamesForResults;
    private final ModifiedColumnSet inputAggregatedColumnsModifiedColumnSet;

    private final Map<String, AggregateColumnSource<?, ?>> inputAggregatedColumns;
    private final Map<String, AggregateColumnSource<?, ?>> resultAggregatedColumns;

    private RowSetBuilderRandom stepDestinationsModified;
    private boolean rowsetsModified = false;

    private boolean initialized;

    public GroupByReaggregateOperator(
            @NotNull final QueryTable inputTable,
            final boolean registeredWithHelper,
            @Nullable final String exposeRowSetsAs,
            @Nullable List<String> hiddenResults,
            @NotNull final MatchPair... aggregatedColumnPairs) {
        this.inputTable = inputTable;
        this.registeredWithHelper = registeredWithHelper;
        this.exposeRowSetsAs = exposeRowSetsAs;
        this.hiddenResults = hiddenResults;
        this.aggregatedColumnPairs = aggregatedColumnPairs;

        if (exposeRowSetsAs == null) {
            throw new IllegalArgumentException("Must expose group RowSets for rollup.");
        }

        live = inputTable.isRefreshing();
        rowSets = new ObjectArraySource<>(WritableRowSet.class);
        addedBuilders = new ObjectArraySource<>(Object.class);

        inputAggregatedColumns = new LinkedHashMap<>(aggregatedColumnPairs.length);
        resultAggregatedColumns = new LinkedHashMap<>(aggregatedColumnPairs.length);
        final List<String> inputColumnNamesForResultsList = new ArrayList<>();
        Arrays.stream(aggregatedColumnPairs).forEach(pair -> {
            // we are reaggregating so have to use the left column for everything
            final ColumnSource<Object> source = inputTable.getColumnSource(pair.leftColumn());
            if (!(source instanceof AggregateColumnSource)) {
                throw new IllegalStateException("Expect to reaggregate AggregateColumnSources for a group operation.");
            }
            @SuppressWarnings("rawtypes")
            final ColumnSource<?> realSource = ((AggregateColumnSource) source).getAggregatedSource();
            final AggregateColumnSource<?, ?> aggregateColumnSource = AggregateColumnSource.make(realSource, rowSets);
            if (hiddenResults == null || !hiddenResults.contains(pair.output().name())) {
                resultAggregatedColumns.put(pair.output().name(), aggregateColumnSource);
                inputColumnNamesForResultsList.add(pair.input().name());
            }
            inputAggregatedColumns.put(pair.input().name(), aggregateColumnSource);
        });

        inputAggregatedColumnsModifiedColumnSet =
                inputTable.newModifiedColumnSet(inputAggregatedColumns.keySet().toArray(String[]::new));

        if (resultAggregatedColumns.containsKey(exposeRowSetsAs)) {
            throw new IllegalArgumentException(String.format(
                    "Exposing group RowSets as %s, but this conflicts with a requested grouped output column name",
                    exposeRowSetsAs));
        }
        inputColumnNamesForResults = inputColumnNamesForResultsList.toArray(String[]::new);
        removedBuilders = live ? new ObjectArraySource<>(Object.class) : null;
        initialized = false;
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length,
            @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            addChunk(values.asObjectChunk(), startPosition, runLength, destination);
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            removeChunk(values.asObjectChunk(), startPosition, runLength, destination);
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void modifyChunk(final BucketedContext bucketedContext,
            final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            final LongChunk<? extends RowKeys> postShiftRowKeys,
            @NotNull final IntChunk<RowKeys> destinations,
            @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);
            modifyChunk(previousValues.asObjectChunk(), newValues.asObjectChunk(), startPosition, runLength,
                    destination);
        }
        stateModified.fillWithValue(0, startPositions.size(), true);
    }

    @Override
    public void shiftChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> preShiftRowKeys,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        throw new IllegalStateException("GroupByReaggregateOperator should never be called with shiftChunk");
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            final long destination) {
        addChunk(values.asObjectChunk(), 0, chunkSize, destination);
        return true;
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        removeChunk(values.asObjectChunk(), 0, chunkSize, destination);
        return true;
    }

    @Override
    public boolean modifyChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
            final LongChunk<? extends RowKeys> postShiftRowKeys,
            final long destination) {
        modifyChunk(previousValues.asObjectChunk(), newValues.asObjectChunk(), 0, chunkSize, destination);
        return true;
    }

    @Override
    public boolean shiftChunk(final SingletonContext singletonContext, final Chunk<? extends Values> previousValues,
            final Chunk<? extends Values> newValues,
            @NotNull final LongChunk<? extends RowKeys> preShiftRowKeys,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            final long destination) {
        // we don't need to deal with these yet
        throw new IllegalStateException(
                "Reaggregations should not require shifts, as aggregations have fixed output slots.");
    }

    private void addChunk(@NotNull final ObjectChunk<RowSet, ? extends Values> rowSets, final int start,
            final int length,
            final long destination) {
        if (length == 0) {
            return;
        }
        accumulateToBuilderRandom(addedBuilders, rowSets, start, length, destination, false);
        if (stepDestinationsModified != null) {
            stepDestinationsModified.addKey(destination);
        }
    }

    private void removeChunk(@NotNull final ObjectChunk<RowSet, ? extends Values> rowSets, final int start,
            final int length,
            final long destination) {
        if (length == 0) {
            return;
        }
        accumulateToBuilderRandom(removedBuilders, rowSets, start, length, destination, false);
        stepDestinationsModified.addKey(destination);
    }

    private void modifyChunk(ObjectChunk<RowSet, ? extends Values> previousValues,
            ObjectChunk<RowSet, ? extends Values> newValues,
            int start,
            int length,
            long destination) {
        if (length == 0) {
            return;
        }

        accumulateToBuilderRandom(removedBuilders, previousValues, start, length, destination, true);
        accumulateToBuilderRandom(addedBuilders, newValues, start, length, destination, false);

        stepDestinationsModified.addKey(destination);
    }

    private static void accumulateToBuilderRandom(@NotNull final ObjectArraySource<Object> builderColumn,
            @NotNull final ObjectChunk<RowSet, ? extends Values> rowSetsToAdd,
            final int start, final int length, final long destination,
            final boolean previous) {
        RowSetBuilderRandom builder = (RowSetBuilderRandom) builderColumn.getUnsafe(destination);
        if (builder == null) {
            builderColumn.set(destination, builder = RowSetFactory.builderRandom());
        }
        // add the keys to the stored builder
        for (int ii = 0; ii < length; ++ii) {
            RowSet rowSet = rowSetsToAdd.get(start + ii);
            if (previous) {
                builder.addRowSet(rowSet.trackingCast().prev());
            } else {
                builder.addRowSet(rowSet);
            }
        }
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
        final Map<String, ColumnSource<?>> allResultColumns =
                new LinkedHashMap<>(resultAggregatedColumns.size() + 1);
        allResultColumns.put(exposeRowSetsAs, rowSets);
        allResultColumns.putAll(resultAggregatedColumns);
        return allResultColumns;
    }

    @Override
    public void startTrackingPrevValues() {
        // NB: We don't need previous tracking on the rowSets ColumnSource, even if it's exposed. It's in destination
        // space, and we never move anything. Nothing should be asking for previous values if they didn't exist
        // previously.
        // NB: These are usually (always, as of now) instances of AggregateColumnSource, meaning
        // startTrackingPrevValues() is a no-op.
        inputAggregatedColumns.values().forEach(ColumnSource::startTrackingPrevValues);
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(
            @NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        initializeNewRowSetPreviousValues(resultTable.getRowSet());
        return registeredWithHelper
                ? new InputToResultModifiedColumnSetFactory(resultTable,
                        inputColumnNamesForResults,
                        resultAggregatedColumns.keySet().toArray(String[]::new))
                : null;
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getInputResultColumns() {
        return inputAggregatedColumns;
    }

    @Override
    public boolean hasModifications(boolean columnsModified) {
        return columnsModified || rowsetsModified;
    }

    private class InputToResultModifiedColumnSetFactory implements UnaryOperator<ModifiedColumnSet> {

        private final ModifiedColumnSet updateModifiedColumnSet;
        private final ModifiedColumnSet.Transformer aggregatedColumnsTransformer;

        private InputToResultModifiedColumnSetFactory(
                @NotNull final QueryTable resultTable,
                @NotNull final String[] inputColumnNames,
                @NotNull final String[] resultAggregatedColumnNames) {
            updateModifiedColumnSet = new ModifiedColumnSet(resultTable.getModifiedColumnSetForUpdates());

            final String[] allInputs = Arrays.copyOf(inputColumnNames, inputColumnNames.length + 1);
            allInputs[allInputs.length - 1] = exposeRowSetsAs;
            final ModifiedColumnSet[] affectedColumns = new ModifiedColumnSet[allInputs.length];
            for (int ci = 0; ci < inputColumnNames.length; ++ci) {
                affectedColumns[ci] = resultTable.newModifiedColumnSet(resultAggregatedColumnNames[ci]);
            }
            affectedColumns[allInputs.length - 1] = resultTable.newModifiedColumnSet(allInputs);

            aggregatedColumnsTransformer = inputTable.newModifiedColumnSetTransformer(allInputs, affectedColumns);
        }

        @Override
        public ModifiedColumnSet apply(@NotNull final ModifiedColumnSet upstreamModifiedColumnSet) {
            aggregatedColumnsTransformer.clearAndTransform(upstreamModifiedColumnSet, updateModifiedColumnSet);
            return updateModifiedColumnSet;
        }
    }

    @Override
    public boolean resetForStep(@NotNull final TableUpdate upstream, final int startingDestinationsCount) {
        stepDestinationsModified = new BitmapRandomBuilder(startingDestinationsCount);
        rowsetsModified = false;
        return upstream.modifiedColumnSet().containsAny(inputAggregatedColumnsModifiedColumnSet);
    }

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        Assert.neqTrue(initialized, "initialized");

        // use the builders to create the initial rowsets
        try (final RowSet initialDestinations = RowSetFactory.flat(startingDestinationsCount);
                final ResettableWritableObjectChunk<WritableRowSet, Values> rowSetResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator destinationsIterator =
                        initialDestinations.getRowSequenceIterator()) {

            final WritableObjectChunk<WritableRowSet, Values> rowSetBackingChunk =
                    rowSetResettableChunk.asWritableObjectChunk();
            final WritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersBackingChunk =
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
                            extractAndClearBuilderRandom(addedBuildersBackingChunk, backingChunkOffset));
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

                final WritableObjectChunk<WritableRowSet, Values> rowSetBackingChunk =
                        rowSetResettableChunk.asWritableObjectChunk();
                final WritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersBackingChunk =
                        addedBuildersResettableChunk.asWritableObjectChunk();
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
                            if (!addRowSet.isEmpty()) {
                                rowsetsModified = true;
                            }
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
                                if (!addRowSet.isEmpty() || !removeRowSet.isEmpty()) {
                                    rowsetsModified = true;
                                }
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


    public String getExposedRowSetsAs() {
        return exposeRowSetsAs;
    }

    public MatchPair[] getAggregatedColumnPairs() {
        return aggregatedColumnPairs;
    }

    public List<String> getHiddenResults() {
        return hiddenResults;
    }

    private class ResultExtractor implements IterativeChunkedAggregationOperator {
        final Map<String, ? extends ColumnSource<?>> resultColumns;
        final String[] inputColumnNames;

        private ResultExtractor(Map<String, ? extends ColumnSource<?>> resultColumns, String[] inputColumnNames) {
            this.resultColumns = resultColumns;
            this.inputColumnNames = inputColumnNames;
        }

        @Override
        public Map<String, ? extends ColumnSource<?>> getResultColumns() {
            return resultColumns;
        }

        @Override
        public void addChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {}

        @Override
        public void removeChunk(BucketedContext context, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, IntChunk<RowKeys> destinations,
                IntChunk<ChunkPositions> startPositions, IntChunk<ChunkLengths> length,
                WritableBooleanChunk<Values> stateModified) {}

        @Override
        public boolean addChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            return false;
        }

        @Override
        public boolean removeChunk(SingletonContext context, int chunkSize, Chunk<? extends Values> values,
                LongChunk<? extends RowKeys> inputRowKeys, long destination) {
            return false;
        }

        @Override
        public void ensureCapacity(long tableSize) {}

        @Override
        public void startTrackingPrevValues() {}

        @Override
        public UnaryOperator<ModifiedColumnSet> initializeRefreshing(@NotNull QueryTable resultTable,
                @NotNull LivenessReferent aggregationUpdateListener) {
            return new InputToResultModifiedColumnSetFactory(resultTable,
                    inputColumnNames,
                    resultColumns.keySet().toArray(String[]::new));
        }
    }

    @NotNull
    public IterativeChunkedAggregationOperator resultExtractor(List<Pair> resultPairs) {
        final List<String> inputColumnNamesList = new ArrayList<>(resultPairs.size());
        final Map<String, ColumnSource<?>> resultColumns = new LinkedHashMap<>(resultPairs.size());
        for (final Pair pair : resultPairs) {
            final String inputName = pair.input().name();
            inputColumnNamesList.add(inputName);
            resultColumns.put(pair.output().name(), inputAggregatedColumns.get(inputName));
        }
        return new ResultExtractor(resultColumns, inputColumnNamesList.toArray(String[]::new));
    }
}
