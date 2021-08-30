package io.deephaven.db.v2.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.live.NotificationQueue;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.util.liveness.LivenessReferent;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.sources.ArrayBackedColumnSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.chunk.Attributes.*;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.tuples.SmartKeySource;
import io.deephaven.db.v2.utils.*;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of
 * {@link io.deephaven.db.tables.Table#byExternal}.
 */
public final class ByExternalChunkedOperator implements IterativeChunkedAggregationOperator {

    private static final Index NONEXISTENT_TABLE_INDEX = Index.FACTORY.getEmptyIndex();
    private static final IndexShiftData.SmartCoalescingBuilder NONEXISTENT_TABLE_SHIFT_BUILDER =
        new IndexShiftData.SmartCoalescingBuilder(NONEXISTENT_TABLE_INDEX.clone());
    private static final QueryTable NONEXISTENT_TABLE =
        new QueryTable(NONEXISTENT_TABLE_INDEX, Collections.emptyMap());

    private static final int WRITE_THROUGH_CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public interface AttributeCopier {
        void copyAttributes(@NotNull QueryTable parentTable, @NotNull QueryTable subTable);
    }

    private final QueryTable parentTable;
    private final AttributeCopier attributeCopier;
    private final List<Object> keysToPrepopulate;
    private final String[] keyColumnNames;

    private final LocalTableMap tableMap; // Consider making this optional, in which case we should
                                          // expose the tables column.
    private final String callSite;

    private final ObjectArraySource<QueryTable> tables;
    private final ObjectArraySource<Index> addedIndices;
    private final ObjectArraySource<Index> removedIndices;
    private final ObjectArraySource<Index> modifiedIndices;
    private final ObjectArraySource<IndexShiftData.SmartCoalescingBuilder> shiftDataBuilders;
    private final ModifiedColumnSet resultModifiedColumnSet;
    private final ModifiedColumnSet.Transformer upstreamToResultTransformer;

    private volatile ColumnSource<?> tableMapKeysSource;
    private volatile LivenessReferent aggregationUpdateListener;

    /**
     * <p>
     * Index to keep track of destinations with shifts.
     * <p>
     * This exists in each cycle between
     * {@link IterativeChunkedAggregationOperator#resetForStep(ShiftAwareListener.Update)} and
     * {@link IterativeChunkedAggregationOperator#propagateUpdates(ShiftAwareListener.Update, ReadOnlyIndex)}
     * <p>
     * If this ever becomes necessary in other operators, it could be moved out to the helper the
     * way modified destination tracking already is.
     * <p>
     * We should consider whether to instead use a random builder, but the current approach seemed
     * reasonable for now.
     */
    private Index stepShiftedDestinations;
    private boolean stepValuesModified;

    /**
     * Construct a new operator.
     *
     * @param unadjustedParentTable The parent table for all sub-tables, without any key-column
     *        dropping or similar already applied
     * @param parentTable The parent table for all sub-tables, with any key-column dropping or
     *        similar already applied
     * @param attributeCopier A procedure that copies attributes or similar from its first argument
     *        (the parent table) to its second (the sub-table)
     * @param keysToPrepopulate A list of keys to be pre-populated safely before the operation
     *        completes.
     * @param keyColumnNames The key columns
     */
    ByExternalChunkedOperator(@NotNull final QueryTable unadjustedParentTable,
        @NotNull final QueryTable parentTable,
        @NotNull final AttributeCopier attributeCopier,
        @NotNull final List<Object> keysToPrepopulate,
        @NotNull final String... keyColumnNames) {
        this.parentTable = parentTable;
        this.attributeCopier = attributeCopier;
        this.keysToPrepopulate = keysToPrepopulate;
        this.keyColumnNames = keyColumnNames;

        tableMap = new LocalTableMap(this::populate, parentTable.getDefinition());
        tableMap.setRefreshing(parentTable.isRefreshing());
        callSite = QueryPerformanceRecorder.getCallerLine();

        tables = new ObjectArraySource<>(QueryTable.class);
        addedIndices = new ObjectArraySource<>(Index.class);

        // Note: Sub-tables always share their ColumnSource map with the parent table, so they can
        // all use this result MCS.
        resultModifiedColumnSet =
            new ModifiedColumnSet(parentTable.getModifiedColumnSetForUpdates());

        if (parentTable.isRefreshing()) {
            removedIndices = new ObjectArraySource<>(Index.class);
            modifiedIndices = new ObjectArraySource<>(Index.class);
            shiftDataBuilders =
                new ObjectArraySource<>(IndexShiftData.SmartCoalescingBuilder.class);

            final Set<String> keyColumnNameSet =
                Arrays.stream(keyColumnNames).collect(Collectors.toSet());
            final Set<String> unadjustedParentColumnNameSet =
                new LinkedHashSet<>(unadjustedParentTable.getDefinition().getColumnNames());
            final String[] retainedResultColumnNames = parentTable.getDefinition().getColumnStream()
                .map(ColumnDefinition::getName)
                .filter(cn -> !keyColumnNameSet.contains(cn))
                .filter(unadjustedParentColumnNameSet::contains)
                .toArray(String[]::new);
            final ModifiedColumnSet[] retainedResultModifiedColumnSets =
                Arrays.stream(retainedResultColumnNames)
                    .map(parentTable::newModifiedColumnSet) // This is safe because we're not giving
                                                            // empty input
                    .toArray(ModifiedColumnSet[]::new);
            upstreamToResultTransformer =
                unadjustedParentTable.getModifiedColumnSetForUpdates().newTransformer(
                    retainedResultColumnNames,
                    retainedResultModifiedColumnSets);
        } else {
            removedIndices = null;
            modifiedIndices = null;
            shiftDataBuilders = null;
            upstreamToResultTransformer = null;
        }
    }

    LocalTableMap getTableMap() {
        return tableMap;
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext,
        final Chunk<? extends Values> values,
        @NotNull final LongChunk<? extends KeyIndices> inputIndices,
        @NotNull final IntChunk<KeyIndices> destinations,
        @NotNull final IntChunk<ChunkPositions> startPositions,
        @NotNull final IntChunk<ChunkLengths> length,
        @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered =
            (LongChunk<OrderedKeyIndices>) inputIndices;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, accumulateToIndex(addedIndices, inputIndicesAsOrdered,
                startPosition, runLength, destination));
        }
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext,
        final Chunk<? extends Values> values,
        @NotNull final LongChunk<? extends KeyIndices> inputIndices,
        @NotNull final IntChunk<KeyIndices> destinations,
        @NotNull final IntChunk<ChunkPositions> startPositions,
        @NotNull final IntChunk<ChunkLengths> length,
        @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered =
            (LongChunk<OrderedKeyIndices>) inputIndices;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, accumulateToIndex(removedIndices, inputIndicesAsOrdered,
                startPosition, runLength, destination));
        }
    }

    @Override
    public void modifyChunk(final BucketedContext bucketedContext,
        final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
        @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
        @NotNull final IntChunk<KeyIndices> destinations,
        @NotNull final IntChunk<ChunkPositions> startPositions,
        @NotNull final IntChunk<ChunkLengths> length,
        @NotNull final WritableBooleanChunk<Values> stateModified) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public void shiftChunk(final BucketedContext bucketedContext,
        final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
        @NotNull final LongChunk<? extends KeyIndices> preShiftIndices,
        @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
        @NotNull final IntChunk<KeyIndices> destinations,
        @NotNull final IntChunk<ChunkPositions> startPositions,
        @NotNull final IntChunk<ChunkLengths> length,
        @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(previousValues, "previousValues");
        Assert.eqNull(newValues, "newValues");
        final TreeIndexImplSequentialBuilder chunkDestinationBuilder =
            new TreeIndexImplSequentialBuilder(true);
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            if (appendShifts(preShiftIndices, postShiftIndices, startPosition, runLength,
                destination)) {
                chunkDestinationBuilder.appendKey(destination);
            }
        }
        try (final ReadOnlyIndex chunkDestinationsShifted =
            new CurrentOnlyIndex(chunkDestinationBuilder.getTreeIndexImpl())) {
            stepShiftedDestinations.insert(chunkDestinationsShifted);
        }
    }

    @Override
    public void modifyIndices(final BucketedContext context,
        @NotNull final LongChunk<? extends KeyIndices> inputIndices,
        @NotNull final IntChunk<KeyIndices> destinations,
        @NotNull final IntChunk<ChunkPositions> startPositions,
        @NotNull final IntChunk<ChunkLengths> length,
        @NotNull final WritableBooleanChunk<Values> stateModified) {
        if (!stepValuesModified) {
            return;
        }
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered =
            (LongChunk<OrderedKeyIndices>) inputIndices;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii, accumulateToIndex(modifiedIndices, inputIndicesAsOrdered,
                startPosition, runLength, destination));
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
        final Chunk<? extends Values> values,
        @NotNull final LongChunk<? extends KeyIndices> inputIndices, final long destination) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered =
            (LongChunk<OrderedKeyIndices>) inputIndices;
        return accumulateToIndex(addedIndices, inputIndicesAsOrdered, 0, chunkSize, destination);
    }

    @Override
    public boolean addIndex(SingletonContext context, Index index, long destination) {
        return accumulateToIndex(addedIndices, index, destination);
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
        final Chunk<? extends Values> values,
        @NotNull final LongChunk<? extends KeyIndices> inputIndices, final long destination) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> inputIndicesAsOrdered =
            (LongChunk<OrderedKeyIndices>) inputIndices;
        return accumulateToIndex(removedIndices, inputIndicesAsOrdered, 0, chunkSize, destination);
    }

    @Override
    public boolean modifyChunk(final SingletonContext singletonContext, final int chunkSize,
        final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
        @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
        final long destination) {
        // We have no inputs, so we should never get here.
        throw new IllegalStateException();
    }

    @Override
    public boolean shiftChunk(final SingletonContext singletonContext,
        final Chunk<? extends Values> previousValues, final Chunk<? extends Values> newValues,
        @NotNull final LongChunk<? extends KeyIndices> preInputIndices,
        @NotNull final LongChunk<? extends KeyIndices> postInputIndices,
        final long destination) {
        Assert.eqNull(previousValues, "previousValues");
        Assert.eqNull(newValues, "newValues");
        if (appendShifts(preInputIndices, postInputIndices, 0, preInputIndices.size(),
            destination)) {
            stepShiftedDestinations.insert(destination);
        }
        return false;
    }

    @Override
    public boolean modifyIndices(final SingletonContext context,
        @NotNull final LongChunk<? extends KeyIndices> indices, final long destination) {
        if (!stepValuesModified) {
            return false;
        }
        // noinspection unchecked
        final LongChunk<OrderedKeyIndices> indicesAsOrdered =
            (LongChunk<OrderedKeyIndices>) indices;
        return accumulateToIndex(modifiedIndices, indicesAsOrdered, 0, indices.size(), destination);
    }

    private static boolean accumulateToIndex(@NotNull final ObjectArraySource<Index> indexColumn,
        @NotNull final LongChunk<OrderedKeyIndices> indicesToAdd, final int start, final int length,
        final long destination) {
        final Index index = indexColumn.getUnsafe(destination);
        if (index == NONEXISTENT_TABLE_INDEX) {
            return false;
        }
        if (index == null) {
            indexColumn.set(destination,
                new CurrentOnlyIndex(TreeIndexImpl.fromChunk(indicesToAdd, start, length, false)));
        } else {
            index.insert(indicesToAdd, start, length);
        }
        return true;
    }

    private static boolean accumulateToIndex(@NotNull final ObjectArraySource<Index> indexColumn,
        @NotNull final Index indicesToAdd, final long destination) {
        final Index index = indexColumn.getUnsafe(destination);
        if (index == NONEXISTENT_TABLE_INDEX) {
            return false;
        }
        if (index == null) {
            final Index currentOnlyIndex = Index.CURRENT_FACTORY.getEmptyIndex();
            currentOnlyIndex.insert(indicesToAdd);
            indexColumn.set(destination, currentOnlyIndex);
        } else {
            index.insert(indicesToAdd);
        }
        return true;
    }

    private boolean appendShifts(@NotNull final LongChunk<? extends KeyIndices> preShiftIndices,
        @NotNull final LongChunk<? extends KeyIndices> postShiftIndices,
        final int startPosition, final int runLength, final long destination) {
        IndexShiftData.SmartCoalescingBuilder builder = shiftDataBuilders.getUnsafe(destination);
        if (builder == NONEXISTENT_TABLE_SHIFT_BUILDER) {
            return false;
        }
        if (builder == null) {
            final Index tableIndex = tables.getUnsafe(destination).getIndex();
            final Index removedIndex = removedIndices.getUnsafe(destination);
            final Index preShiftKeys;
            if (removedIndex == null) {
                preShiftKeys = tableIndex.clone();
            } else {
                preShiftKeys = tableIndex.minus(removedIndex);
            }
            shiftDataBuilders.set(destination,
                builder = new IndexShiftData.SmartCoalescingBuilder(preShiftKeys));
        }
        // the polarity must be the same for shifted index in our chunk, so we use the first one to
        // identify the proper polarity
        final boolean reversedPolarity = preShiftIndices.get(0) < postShiftIndices.get(0);
        if (reversedPolarity) {
            for (int ki = runLength - 1; ki >= 0; --ki) {
                final int keyOffset = ki + startPosition;
                final long preShiftKey = preShiftIndices.get(keyOffset);
                final long postShiftKey = postShiftIndices.get(keyOffset);
                final long delta = postShiftKey - preShiftKey;
                builder.shiftRange(preShiftKey, preShiftKey, delta);
            }
        } else {
            for (int ki = 0; ki < runLength; ++ki) {
                final int keyOffset = ki + startPosition;
                final long preShiftKey = preShiftIndices.get(keyOffset);
                final long postShiftKey = postShiftIndices.get(keyOffset);
                final long delta = postShiftKey - preShiftKey;
                builder.shiftRange(preShiftKey, preShiftKey, delta);
            }
        }
        return true;
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        tables.ensureCapacity(tableSize);
        addedIndices.ensureCapacity(tableSize);
        if (parentTable.isRefreshing()) {
            removedIndices.ensureCapacity(tableSize);
            modifiedIndices.ensureCapacity(tableSize);
            shiftDataBuilders.ensureCapacity(tableSize);
        }
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.emptyMap();
    }

    @Override
    public void startTrackingPrevValues() {}

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable) {
        tableMapKeysSource = keyColumnNames.length == 1
            ? resultTable.getColumnSource(keyColumnNames[0])
            : new SmartKeySource(Arrays.stream(keyColumnNames).map(resultTable::getColumnSource)
                .toArray(ColumnSource[]::new));

        final ReadOnlyIndex initialDestinations = resultTable.getIndex();
        if (initialDestinations.nonempty()) {
            // At this point, we cannot have had any tables pre-populated because the table map has
            // not been exposed
            // externally.
            // The table map is still managed by its creating scope, and so does not need extra
            // steps to ensure liveness.
            // There's also no aggregation update listener to retain yet.
            final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
            try (
                final ChunkSource.GetContext tableMapKeysGetContext =
                    tableMapKeysSource.makeGetContext(WRITE_THROUGH_CHUNK_SIZE);
                final ChunkBoxer.BoxerKernel tableMapKeysBoxer = ChunkBoxer
                    .getBoxer(tableMapKeysSource.getChunkType(), WRITE_THROUGH_CHUNK_SIZE);
                final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                    ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<Index, Values> addedIndicesResettableChunk =
                    ResettableWritableObjectChunk.makeResettableChunk();
                final OrderedKeys.Iterator initialDestinationsIterator =
                    initialDestinations.getOrderedKeysIterator()) {

                // noinspection unchecked
                final WritableObjectChunk<QueryTable, Values> tablesBackingChunk =
                    tablesResettableChunk.asWritableObjectChunk();
                // noinspection unchecked
                final WritableObjectChunk<Index, Values> addedIndicesBackingChunk =
                    addedIndicesResettableChunk.asWritableObjectChunk();

                while (initialDestinationsIterator.hasMore()) {
                    final long firstSliceDestination = initialDestinationsIterator.peekNextKey();
                    final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk,
                            firstSliceDestination);
                    addedIndices.resetWritableChunkToBackingStore(addedIndicesResettableChunk,
                        firstSliceDestination);
                    final long lastBackingChunkDestination =
                        firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                    final OrderedKeys initialDestinationsSlice = initialDestinationsIterator
                        .getNextOrderedKeysThrough(lastBackingChunkDestination);

                    final ObjectChunk<?, ? extends Values> tableMapKeyChunk =
                        tableMapKeysBoxer.box(tableMapKeysSource.getChunk(tableMapKeysGetContext,
                            initialDestinationsSlice));

                    final MutableInt tableMapKeyOffset = new MutableInt();
                    initialDestinationsSlice.forAllLongs((final long destinationToInitialize) -> {
                        final Object tableMapKey =
                            tableMapKeyChunk.get(tableMapKeyOffset.intValue());
                        tableMapKeyOffset.increment();

                        final int backingChunkOffset =
                            Math.toIntExact(destinationToInitialize - firstBackingChunkDestination);
                        final QueryTable unexpectedExistingTable =
                            tablesBackingChunk.get(backingChunkOffset);
                        if (unexpectedExistingTable != null) {
                            throw new IllegalStateException("Found unexpected existing table "
                                + unexpectedExistingTable + " in initial slot "
                                + destinationToInitialize + " for key " + tableMapKey);
                        }

                        final Index initialIndex =
                            extractAndClearIndex(addedIndicesBackingChunk, backingChunkOffset);
                        initialIndex.compact();
                        final QueryTable newTable = makeSubTable(initialIndex);
                        tablesBackingChunk.set(backingChunkOffset, newTable);
                        final Table unexpectedPrepopulatedTable =
                            tableMap.put(tableMapKey, newTable);
                        if (unexpectedPrepopulatedTable != null) {
                            throw new IllegalStateException("Found unexpected prepopulated table "
                                + unexpectedPrepopulatedTable + " after setting initial slot "
                                + destinationToInitialize + " for key " + tableMapKey);
                        }
                    });
                }
            } finally {
                if (setCallSite) {
                    QueryPerformanceRecorder.clearCallsite();
                }
            }
        }

        keysToPrepopulate.forEach(this::populateInternal);
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(
        @NotNull final QueryTable resultTable,
        @NotNull final LivenessReferent aggregationUpdateListener) {
        this.aggregationUpdateListener = aggregationUpdateListener;
        if (aggregationUpdateListener instanceof NotificationQueue.Dependency) {
            tableMap.setDependency((NotificationQueue.Dependency) aggregationUpdateListener);
        }
        tableMap.addParentReference(aggregationUpdateListener);
        tableMap.values()
            .forEach(st -> ((DynamicNode) st).addParentReference(aggregationUpdateListener));
        return IterativeChunkedAggregationOperator.super.initializeRefreshing(resultTable,
            aggregationUpdateListener);
    }

    @Override
    public void resetForStep(@NotNull final ShiftAwareListener.Update upstream) {
        stepShiftedDestinations = Index.CURRENT_FACTORY.getEmptyIndex();
        final boolean upstreamModified =
            upstream.modified.nonempty() && upstream.modifiedColumnSet.nonempty();
        if (upstreamModified) {
            // We re-use this for all sub-tables that have modifies.
            upstreamToResultTransformer.clearAndTransform(upstream.modifiedColumnSet,
                resultModifiedColumnSet);
            stepValuesModified = resultModifiedColumnSet.nonempty();
        } else {
            stepValuesModified = false;
        }
    }

    @Override
    public void propagateUpdates(@NotNull final ShiftAwareListener.Update downstream,
        @NotNull final ReadOnlyIndex newDestinations) {
        if (downstream.added.isEmpty() && downstream.removed.isEmpty()
            && downstream.modified.isEmpty() && stepShiftedDestinations.isEmpty()) {
            stepShiftedDestinations = null;
            return;
        }
        if (downstream.added.nonempty()) {
            try (final OrderedKeys resurrectedDestinations =
                downstream.added.minus(newDestinations)) {
                propagateResurrectedDestinations(resurrectedDestinations);
                propagateNewDestinations(newDestinations);
            }
        }
        propagateUpdatesToRemovedDestinations(downstream.removed);
        try (final OrderedKeys modifiedOrShiftedDestinations =
            downstream.modified.union(stepShiftedDestinations)) {
            stepShiftedDestinations = null;
            propagateUpdatesToModifiedDestinations(modifiedOrShiftedDestinations);
        }
    }

    private void propagateResurrectedDestinations(
        @NotNull final OrderedKeys resurrectedDestinations) {
        if (resurrectedDestinations.isEmpty()) {
            return;
        }
        try (
            final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> addedIndicesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final OrderedKeys.Iterator resurrectedDestinationsIterator =
                resurrectedDestinations.getOrderedKeysIterator()) {
            // Destinations that were added can't have any removals, modifications, or shifts.

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk =
                tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> addedIndicesBackingChunk =
                addedIndicesResettableChunk.asWritableObjectChunk();

            while (resurrectedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = resurrectedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination = tables
                    .resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                addedIndices.resetWritableChunkToBackingStore(addedIndicesResettableChunk,
                    firstSliceDestination);
                final long lastBackingChunkDestination =
                    firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final OrderedKeys resurrectedDestinationsSlice = resurrectedDestinationsIterator
                    .getNextOrderedKeysThrough(lastBackingChunkDestination);

                resurrectedDestinationsSlice.forAllLongs((final long resurrectedDestination) -> {
                    final int backingChunkOffset =
                        Math.toIntExact(resurrectedDestination - firstBackingChunkDestination);

                    final QueryTable resurrectedTable = tablesBackingChunk.get(backingChunkOffset);
                    if (resurrectedTable == NONEXISTENT_TABLE) {
                        return;
                    }
                    if (resurrectedTable == null) {
                        throw new IllegalStateException("Missing resurrected table in slot "
                            + resurrectedDestination + " for table map key "
                            + tableMapKeysSource.get(resurrectedDestination));
                    }

                    // This table existed already, and has been "resurrected" after becoming empty
                    // previously. We must notify.

                    final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();

                    downstream.added = nullToEmpty(
                        extractAndClearIndex(addedIndicesBackingChunk, backingChunkOffset));
                    downstream.removed = Index.CURRENT_FACTORY.getEmptyIndex();
                    downstream.modified = Index.CURRENT_FACTORY.getEmptyIndex();
                    downstream.shifted = IndexShiftData.EMPTY;
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                    resurrectedTable.getIndex().compact();

                    Assert.assertion(resurrectedTable.getIndex().isEmpty(),
                        "resurrectedTable.getIndex().isEmpty()");
                    resurrectedTable.getIndex().insert(downstream.added);
                    resurrectedTable.notifyListeners(downstream);
                });
            }
        }
    }

    private void propagateNewDestinations(@NotNull final OrderedKeys newDestinations) {
        if (newDestinations.isEmpty()) {
            return;
        }
        final boolean retainedTableMap = tableMap.tryRetainReference();
        final boolean retainedAggregationUpdateListener =
            aggregationUpdateListener.tryRetainReference();
        final boolean allowCreation = retainedTableMap && retainedAggregationUpdateListener;
        final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
        try (
            final ChunkSource.GetContext tableMapKeysGetContext =
                tableMapKeysSource.makeGetContext(WRITE_THROUGH_CHUNK_SIZE);
            final ChunkBoxer.BoxerKernel tableMapKeysBoxer =
                ChunkBoxer.getBoxer(tableMapKeysSource.getChunkType(), WRITE_THROUGH_CHUNK_SIZE);
            final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> addedIndicesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> removedIndicesResettableChunk =
                allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> modifiedIndicesResettableChunk =
                allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<IndexShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersResettableChunk =
                allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
            final OrderedKeys.Iterator newDestinationsIterator =
                newDestinations.getOrderedKeysIterator()) {

            // noinspection unchecked
            final WritableObjectChunk<QueryTable, Values> tablesBackingChunk =
                tablesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> addedIndicesBackingChunk =
                addedIndicesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> removedIndicesBackingChunk =
                allowCreation ? null : removedIndicesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> modifiedIndicesBackingChunk =
                allowCreation ? null : modifiedIndicesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<IndexShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersBackingChunk =
                allowCreation ? null : shiftDataBuildersResettableChunk.asWritableObjectChunk();

            while (newDestinationsIterator.hasMore()) {
                final long firstSliceDestination = newDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination = tables
                    .resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                addedIndices.resetWritableChunkToBackingStore(addedIndicesResettableChunk,
                    firstSliceDestination);
                if (!allowCreation) {
                    removedIndices.resetWritableChunkToBackingStore(removedIndicesResettableChunk,
                        firstSliceDestination);
                    modifiedIndices.resetWritableChunkToBackingStore(modifiedIndicesResettableChunk,
                        firstSliceDestination);
                    shiftDataBuilders.resetWritableChunkToBackingStore(
                        shiftDataBuildersResettableChunk, firstSliceDestination);
                }
                final long lastBackingChunkDestination =
                    firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final OrderedKeys newDestinationsSlice =
                    newDestinationsIterator.getNextOrderedKeysThrough(lastBackingChunkDestination);

                final ObjectChunk<?, ? extends Values> tableMapKeyChunk = tableMapKeysBoxer
                    .box(tableMapKeysSource.getChunk(tableMapKeysGetContext, newDestinationsSlice));

                final MutableInt tableMapKeyOffset = new MutableInt();
                newDestinationsSlice.forAllLongs((final long newDestination) -> {
                    final Object tableMapKey = tableMapKeyChunk.get(tableMapKeyOffset.intValue());
                    tableMapKeyOffset.increment();

                    final int backingChunkOffset =
                        Math.toIntExact(newDestination - firstBackingChunkDestination);
                    final QueryTable unexpectedExistingTable =
                        tablesBackingChunk.get(backingChunkOffset);
                    if (unexpectedExistingTable != null) {
                        throw new IllegalStateException(
                            "Found unexpected existing table " + unexpectedExistingTable
                                + " in new slot " + newDestination + " for key " + tableMapKey);
                    }

                    final QueryTable prepopulatedTable;

                    if (allowCreation) {
                        final MutableBoolean newTableAllocated = new MutableBoolean();
                        final QueryTable newOrPrepopulatedTable =
                            (QueryTable) tableMap.computeIfAbsent(tableMapKey, (unused) -> {
                                final Index newIndex = extractAndClearIndex(
                                    addedIndicesBackingChunk, backingChunkOffset);
                                newIndex.compact();
                                final QueryTable newTable = makeSubTable(newIndex);
                                tablesBackingChunk.set(backingChunkOffset, newTable);
                                newTableAllocated.setTrue();
                                return newTable;
                            });
                        prepopulatedTable =
                            newTableAllocated.booleanValue() ? null : newOrPrepopulatedTable;
                    } else {
                        prepopulatedTable = (QueryTable) tableMap.get(tableMapKey);
                    }
                    if (prepopulatedTable != null) {
                        tablesBackingChunk.set(backingChunkOffset, prepopulatedTable);

                        // "New" table already existed due to TableMap.populateKeys.
                        // We can ignore allowCreation; the table exists already, and must already
                        // retain appropriate referents.
                        // Additionally, we must notify of added rows.
                        final ShiftAwareListener.Update downstream =
                            new ShiftAwareListener.Update();

                        downstream.added = nullToEmpty(
                            extractAndClearIndex(addedIndicesBackingChunk, backingChunkOffset));
                        downstream.removed = Index.CURRENT_FACTORY.getEmptyIndex();
                        downstream.modified = Index.CURRENT_FACTORY.getEmptyIndex();
                        downstream.shifted = IndexShiftData.EMPTY;
                        downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                        prepopulatedTable.getIndex().insert(downstream.added);
                        prepopulatedTable.notifyListeners(downstream);
                    } else if (!allowCreation) {
                        // We will never try to create this table again, or accumulate further state
                        // for it.
                        tablesBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE);
                        addedIndicesBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_INDEX);
                        removedIndicesBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_INDEX);
                        modifiedIndicesBackingChunk.set(backingChunkOffset,
                            NONEXISTENT_TABLE_INDEX);
                        shiftDataBuildersBackingChunk.set(backingChunkOffset,
                            NONEXISTENT_TABLE_SHIFT_BUILDER);
                    }
                });
            }
        } finally {
            if (setCallSite) {
                QueryPerformanceRecorder.clearCallsite();
            }
            if (retainedAggregationUpdateListener) {
                aggregationUpdateListener.dropReference();
            }
            if (retainedTableMap) {
                tableMap.dropReference();
            }
        }
    }

    /**
     * Add an empty sub-table for the specified key if it's not currently found in the table map.
     *
     * @param key The key for the new sub-table
     */
    private void populate(final Object key) {
        LiveTableMonitor.DEFAULT.checkInitiateTableOperation();
        populateInternal(key);
    }

    private void populateInternal(final Object key) {
        // We don't bother with complicated retention or non-existent result handling, here.
        // If the user is calling TableMap.populateKeys (the only way to get here) they'd better be
        // sure of liveness
        // already, and they won't thank us for adding non-existent table tombstones rather than
        // blowing up.
        final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
        try {
            tableMap.computeIfAbsent(key, (unused) -> makeSubTable(null));
        } finally {
            if (setCallSite) {
                QueryPerformanceRecorder.clearCallsite();
            }
        }
    }

    private QueryTable makeSubTable(@Nullable final Index initialIndexToInsert) {
        // We don't start from initialIndexToInsert because it is expected to be a CurrentOnlyIndex.
        final QueryTable subTable =
            parentTable.getSubTable(Index.FACTORY.getEmptyIndex(), resultModifiedColumnSet);
        subTable.setRefreshing(parentTable.isRefreshing());
        if (aggregationUpdateListener != null) {
            subTable.addParentReference(aggregationUpdateListener);
        }
        attributeCopier.copyAttributes(parentTable, subTable);
        if (initialIndexToInsert != null) {
            subTable.getIndex().insert(initialIndexToInsert);
            subTable.getIndex().initializePreviousValue();
        }
        return subTable;
    }

    private void propagateUpdatesToRemovedDestinations(
        @NotNull final OrderedKeys removedDestinations) {
        if (removedDestinations.isEmpty()) {
            return;
        }
        try (
            final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> removedIndicesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final OrderedKeys.Iterator removedDestinationsIterator =
                removedDestinations.getOrderedKeysIterator()) {
            // Destinations that were completely removed can't have any additions, modifications, or
            // shifts.

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk =
                tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> removedIndicesBackingChunk =
                removedIndicesResettableChunk.asWritableObjectChunk();

            while (removedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = removedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination = tables
                    .resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                removedIndices.resetWritableChunkToBackingStore(removedIndicesResettableChunk,
                    firstSliceDestination);
                final long lastBackingChunkDestination =
                    firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final OrderedKeys removedDestinationsSlice = removedDestinationsIterator
                    .getNextOrderedKeysThrough(lastBackingChunkDestination);

                removedDestinationsSlice.forAllLongs((final long removedDestination) -> {
                    final int backingChunkOffset =
                        Math.toIntExact(removedDestination - firstBackingChunkDestination);

                    final QueryTable removedTable = tablesBackingChunk.get(backingChunkOffset);
                    if (removedTable == NONEXISTENT_TABLE) {
                        return;
                    }
                    if (removedTable == null) {
                        throw new IllegalStateException("Missing removed table in slot "
                            + removedDestination + " for table map key "
                            + tableMapKeysSource.get(removedDestination));
                    }

                    final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();

                    downstream.added = Index.CURRENT_FACTORY.getEmptyIndex();
                    downstream.removed = nullToEmpty(
                        extractAndClearIndex(removedIndicesBackingChunk, backingChunkOffset));
                    downstream.modified = Index.CURRENT_FACTORY.getEmptyIndex();
                    downstream.shifted = IndexShiftData.EMPTY;
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                    removedTable.getIndex().remove(downstream.removed);
                    removedTable.getIndex().compact();
                    Assert.assertion(removedTable.getIndex().isEmpty(),
                        "removedTable.getIndex().isEmpty()");
                    removedTable.notifyListeners(downstream);
                });
            }
        }
    }

    private void propagateUpdatesToModifiedDestinations(
        @NotNull final OrderedKeys modifiedDestinations) {
        if (modifiedDestinations.isEmpty()) {
            return;
        }
        try (
            final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> addedIndicesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> removedIndicesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<Index, Values> modifiedIndicesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final ResettableWritableObjectChunk<IndexShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
            final OrderedKeys.Iterator modifiedDestinationsIterator =
                modifiedDestinations.getOrderedKeysIterator()) {

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk =
                tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> addedIndicesBackingChunk =
                addedIndicesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> removedIndicesBackingChunk =
                removedIndicesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<Index, Values> modifiedIndicesBackingChunk =
                modifiedIndicesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<IndexShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersBackingChunk =
                shiftDataBuildersResettableChunk.asWritableObjectChunk();

            while (modifiedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = modifiedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination = tables
                    .resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                // The (valid) assumption is that the other write-through resets will address the
                // same range.
                addedIndices.resetWritableChunkToBackingStore(addedIndicesResettableChunk,
                    firstSliceDestination);
                removedIndices.resetWritableChunkToBackingStore(removedIndicesResettableChunk,
                    firstSliceDestination);
                modifiedIndices.resetWritableChunkToBackingStore(modifiedIndicesResettableChunk,
                    firstSliceDestination);
                shiftDataBuilders.resetWritableChunkToBackingStore(shiftDataBuildersResettableChunk,
                    firstSliceDestination);
                final long lastBackingChunkDestination =
                    firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final OrderedKeys modifiedDestinationsSlice = modifiedDestinationsIterator
                    .getNextOrderedKeysThrough(lastBackingChunkDestination);

                modifiedDestinationsSlice.forAllLongs((final long modifiedDestination) -> {
                    final int backingChunkOffset =
                        Math.toIntExact(modifiedDestination - firstBackingChunkDestination);

                    final QueryTable modifiedTable = tablesBackingChunk.get(backingChunkOffset);
                    if (modifiedTable == NONEXISTENT_TABLE) {
                        return;
                    }
                    if (modifiedTable == null) {
                        throw new IllegalStateException("Missing modified table in slot "
                            + modifiedDestination + " for table map key "
                            + tableMapKeysSource.get(modifiedDestination));
                    }

                    final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();

                    downstream.added = nullToEmpty(
                        extractAndClearIndex(addedIndicesBackingChunk, backingChunkOffset));
                    downstream.removed = nullToEmpty(
                        extractAndClearIndex(removedIndicesBackingChunk, backingChunkOffset));
                    downstream.modified = stepValuesModified
                        ? nullToEmpty(
                            extractAndClearIndex(modifiedIndicesBackingChunk, backingChunkOffset))
                        : Index.CURRENT_FACTORY.getEmptyIndex();
                    downstream.shifted = extractAndClearShiftDataBuilder(
                        shiftDataBuildersBackingChunk, backingChunkOffset);
                    downstream.modifiedColumnSet =
                        downstream.modified.empty() ? ModifiedColumnSet.EMPTY
                            : resultModifiedColumnSet;

                    if (downstream.removed.nonempty()) {
                        modifiedTable.getIndex().remove(downstream.removed);
                    }
                    if (downstream.shifted.nonempty()) {
                        downstream.shifted.apply(modifiedTable.getIndex());
                    }
                    if (downstream.added.nonempty()) {
                        modifiedTable.getIndex().insert(downstream.added);
                    }

                    modifiedTable.getIndex().compact();

                    modifiedTable.notifyListeners(downstream);
                });
            }
        }
    }

    private static Index extractAndClearIndex(
        @NotNull final WritableObjectChunk<Index, Values> indicesChunk, final int offset) {
        final Index index = indicesChunk.get(offset);
        Assert.neq(index, "index", NONEXISTENT_TABLE_INDEX, "NONEXISTENT_TABLE_INDEX");
        if (index != null) {
            indicesChunk.set(offset, null);
        }
        return index;
    }

    private static Index nullToEmpty(@Nullable final Index index) {
        return index == null ? Index.CURRENT_FACTORY.getEmptyIndex() : index;
    }

    private static IndexShiftData extractAndClearShiftDataBuilder(
        @NotNull final WritableObjectChunk<IndexShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersChunk,
        final int offset) {
        final IndexShiftData.SmartCoalescingBuilder shiftDataBuilder =
            shiftDataBuildersChunk.get(offset);
        Assert.neq(shiftDataBuilder, "shiftDataBuilder", NONEXISTENT_TABLE_SHIFT_BUILDER,
            "NONEXISTENT_TABLE_SHIFT_BUILDER");
        if (shiftDataBuilder == null) {
            return IndexShiftData.EMPTY;
        }
        shiftDataBuildersChunk.set(offset, null);
        return shiftDataBuilder.build();
    }

    @Override
    public void propagateFailure(@NotNull final Throwable originalException,
        @NotNull UpdatePerformanceTracker.Entry sourceEntry) {
        tableMap.values().forEach(
            st -> ((DynamicTable) st).notifyListenersOnError(originalException, sourceEntry));
    }

    @Override
    public boolean requiresIndices() {
        return true;
    }

    @Override
    public boolean unchunkedIndex() {
        return true;
    }

}
