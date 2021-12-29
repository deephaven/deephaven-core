package io.deephaven.engine.table.impl.by;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.impl.AdaptiveOrderedLongSetBuilderRandom;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.table.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.impl.OrderedLongSet;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.SmartKeySource;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of {@link Table#partitionBy}.
 */
public final class PartitionByChunkedOperator implements IterativeChunkedAggregationOperator {

    private static final WritableRowSet NONEXISTENT_TABLE_ROW_SET = RowSetFactory.empty();
    private static final RowSetShiftData.SmartCoalescingBuilder NONEXISTENT_TABLE_SHIFT_BUILDER =
            new RowSetShiftData.SmartCoalescingBuilder(NONEXISTENT_TABLE_ROW_SET.copy());
    private static final QueryTable NONEXISTENT_TABLE =
            new QueryTable(NONEXISTENT_TABLE_ROW_SET.toTracking(), Collections.emptyMap());

    private static final int WRITE_THROUGH_CHUNK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    public interface AttributeCopier {
        void copyAttributes(@NotNull QueryTable parentTable, @NotNull QueryTable subTable);
    }

    private final QueryTable parentTable;
    private final AttributeCopier attributeCopier;
    private final List<Object> keysToPrepopulate;
    private final String[] keyColumnNames;

    private final LocalTableMap tableMap; // Consider making this optional, in which case we should expose the tables
                                          // column.
    private final String callSite;

    private final ObjectArraySource<QueryTable> tables;
    private final ObjectArraySource<WritableRowSet> addedRowSets;
    private final ObjectArraySource<WritableRowSet> removedRowSets;
    private final ObjectArraySource<WritableRowSet> modifiedRowSets;
    private final ObjectArraySource<RowSetShiftData.SmartCoalescingBuilder> shiftDataBuilders;
    private final ModifiedColumnSet resultModifiedColumnSet;
    private final ModifiedColumnSet.Transformer upstreamToResultTransformer;

    private volatile ColumnSource<?> tableMapKeysSource;
    private volatile LivenessReferent aggregationUpdateListener;

    /**
     * <p>
     * RowSet to keep track of destinations with shifts.
     * <p>
     * This exists in each cycle between {@link IterativeChunkedAggregationOperator#resetForStep(TableUpdate)} and
     * {@link IterativeChunkedAggregationOperator#propagateUpdates(TableUpdate, RowSet)}
     * <p>
     * If this ever becomes necessary in other operators, it could be moved out to the helper the way modified
     * destination tracking already is.
     * <p>
     * We should consider whether to instead use a random builder, but the current approach seemed reasonable for now.
     */
    private WritableRowSet stepShiftedDestinations;
    private boolean stepValuesModified;

    /**
     * Construct a new operator.
     *
     * @param unadjustedParentTable The parent table for all sub-tables, without any key-column dropping or similar
     *        already applied
     * @param parentTable The parent table for all sub-tables, with any key-column dropping or similar already applied
     * @param attributeCopier A procedure that copies attributes or similar from its first argument (the parent table)
     *        to its second (the sub-table)
     * @param keysToPrepopulate A list of keys to be pre-populated safely before the operation completes.
     * @param keyColumnNames The key columns
     */
    PartitionByChunkedOperator(@NotNull final QueryTable unadjustedParentTable,
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
        addedRowSets = new ObjectArraySource<>(WritableRowSet.class);

        // Note: Sub-tables always share their ColumnSource map with the parent table, so they can all use this result
        // MCS.
        resultModifiedColumnSet = new ModifiedColumnSet(parentTable.getModifiedColumnSetForUpdates());

        if (parentTable.isRefreshing()) {
            removedRowSets = new ObjectArraySource<>(WritableRowSet.class);
            modifiedRowSets = new ObjectArraySource<>(WritableRowSet.class);
            shiftDataBuilders = new ObjectArraySource<>(RowSetShiftData.SmartCoalescingBuilder.class);

            final Set<String> keyColumnNameSet = Arrays.stream(keyColumnNames).collect(Collectors.toSet());
            final Set<String> unadjustedParentColumnNameSet =
                    new LinkedHashSet<>(unadjustedParentTable.getDefinition().getColumnNames());
            final String[] retainedResultColumnNames = parentTable.getDefinition().getColumnStream()
                    .map(ColumnDefinition::getName)
                    .filter(cn -> !keyColumnNameSet.contains(cn))
                    .filter(unadjustedParentColumnNameSet::contains)
                    .toArray(String[]::new);
            final ModifiedColumnSet[] retainedResultModifiedColumnSets = Arrays.stream(retainedResultColumnNames)
                    .map(parentTable::newModifiedColumnSet) // This is safe because we're not giving empty input
                    .toArray(ModifiedColumnSet[]::new);
            upstreamToResultTransformer = unadjustedParentTable.getModifiedColumnSetForUpdates().newTransformer(
                    retainedResultColumnNames,
                    retainedResultModifiedColumnSets);
        } else {
            removedRowSets = null;
            modifiedRowSets = null;
            shiftDataBuilders = null;
            upstreamToResultTransformer = null;
        }
    }

    LocalTableMap getTableMap() {
        return tableMap;
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii,
                    accumulateToIndex(addedRowSets, inputRowKeysAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii,
                    accumulateToIndex(removedRowSets, inputRowKeysAsOrdered, startPosition, runLength, destination));
        }
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
        final AdaptiveOrderedLongSetBuilderRandom chunkDestinationBuilder = new AdaptiveOrderedLongSetBuilderRandom();
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            if (appendShifts(preShiftRowKeys, postShiftRowKeys, startPosition, runLength, destination)) {
                chunkDestinationBuilder.addKey(destination);
            }
        }
        try (final RowSet chunkDestinationsShifted =
                new WritableRowSetImpl(chunkDestinationBuilder.getTreeIndexImpl())) {
            stepShiftedDestinations.insert(chunkDestinationsShifted);
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
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        for (int ii = 0; ii < startPositions.size(); ++ii) {
            final int startPosition = startPositions.get(ii);
            final int runLength = length.get(ii);
            final long destination = destinations.get(startPosition);

            stateModified.set(ii,
                    accumulateToIndex(modifiedRowSets, inputRowKeysAsOrdered, startPosition, runLength, destination));
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        return accumulateToIndex(addedRowSets, inputRowKeysAsOrdered, 0, chunkSize, destination);
    }

    @Override
    public boolean addRowSet(SingletonContext context, RowSet rowSet, long destination) {
        return accumulateToIndex(addedRowSets, rowSet, destination);
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        return accumulateToIndex(removedRowSets, inputRowKeysAsOrdered, 0, chunkSize, destination);
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
        if (appendShifts(preShiftRowKeys, postShiftRowKeys, 0, preShiftRowKeys.size(), destination)) {
            stepShiftedDestinations.insert(destination);
        }
        return false;
    }

    @Override
    public boolean modifyRowKeys(final SingletonContext context, @NotNull final LongChunk<? extends RowKeys> rowKeys,
            final long destination) {
        if (!stepValuesModified) {
            return false;
        }
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> rowKeysAsOrdered = (LongChunk<OrderedRowKeys>) rowKeys;
        return accumulateToIndex(modifiedRowSets, rowKeysAsOrdered, 0, rowKeys.size(), destination);
    }

    private static boolean accumulateToIndex(@NotNull final ObjectArraySource<WritableRowSet> indexColumn,
            @NotNull final LongChunk<OrderedRowKeys> rowKeysToAdd, final int start, final int length,
            final long destination) {
        final WritableRowSet rowSet = indexColumn.getUnsafe(destination);
        if (rowSet == NONEXISTENT_TABLE_ROW_SET) {
            return false;
        }
        if (rowSet == null) {
            indexColumn.set(destination,
                    new WritableRowSetImpl(OrderedLongSet.fromChunk(rowKeysToAdd, start, length, false)));
        } else {
            rowSet.insert(rowKeysToAdd, start, length);
        }
        return true;
    }

    private static boolean accumulateToIndex(@NotNull final ObjectArraySource<WritableRowSet> indexColumn,
            @NotNull final RowSet rowSetToAdd, final long destination) {
        final WritableRowSet rowSet = indexColumn.getUnsafe(destination);
        if (rowSet == NONEXISTENT_TABLE_ROW_SET) {
            return false;
        }
        if (rowSet == null) {
            final WritableRowSet currentRowSet = RowSetFactory.empty();
            currentRowSet.insert(rowSetToAdd);
            indexColumn.set(destination, currentRowSet);
        } else {
            rowSet.insert(rowSetToAdd);
        }
        return true;
    }

    private boolean appendShifts(@NotNull final LongChunk<? extends RowKeys> preShiftRowKeys,
            @NotNull final LongChunk<? extends RowKeys> postShiftRowKeys,
            final int startPosition, final int runLength, final long destination) {
        RowSetShiftData.SmartCoalescingBuilder builder = shiftDataBuilders.getUnsafe(destination);
        if (builder == NONEXISTENT_TABLE_SHIFT_BUILDER) {
            return false;
        }
        if (builder == null) {
            final RowSet tableRowSet = tables.getUnsafe(destination).getRowSet();
            final RowSet removedRowSet = removedRowSets.getUnsafe(destination);
            final RowSet preShiftKeys;
            if (removedRowSet == null) {
                preShiftKeys = tableRowSet.copy();
            } else {
                preShiftKeys = tableRowSet.minus(removedRowSet);
            }
            shiftDataBuilders.set(destination, builder = new RowSetShiftData.SmartCoalescingBuilder(preShiftKeys));
        }
        // the polarity must be the same for shifted RowSet in our chunk, so we use the first one to identify the proper
        // polarity
        final boolean reversedPolarity = preShiftRowKeys.get(0) < postShiftRowKeys.get(0);
        if (reversedPolarity) {
            for (int ki = runLength - 1; ki >= 0; --ki) {
                final int keyOffset = ki + startPosition;
                final long preShiftKey = preShiftRowKeys.get(keyOffset);
                final long postShiftKey = postShiftRowKeys.get(keyOffset);
                final long delta = postShiftKey - preShiftKey;
                builder.shiftRange(preShiftKey, preShiftKey, delta);
            }
        } else {
            for (int ki = 0; ki < runLength; ++ki) {
                final int keyOffset = ki + startPosition;
                final long preShiftKey = preShiftRowKeys.get(keyOffset);
                final long postShiftKey = postShiftRowKeys.get(keyOffset);
                final long delta = postShiftKey - preShiftKey;
                builder.shiftRange(preShiftKey, preShiftKey, delta);
            }
        }
        return true;
    }

    @Override
    public void ensureCapacity(final long tableSize) {
        tables.ensureCapacity(tableSize);
        addedRowSets.ensureCapacity(tableSize);
        if (parentTable.isRefreshing()) {
            removedRowSets.ensureCapacity(tableSize);
            modifiedRowSets.ensureCapacity(tableSize);
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
                : new SmartKeySource(
                        Arrays.stream(keyColumnNames).map(resultTable::getColumnSource).toArray(ColumnSource[]::new));

        final RowSet initialDestinations = resultTable.getRowSet();
        if (initialDestinations.isNonempty()) {
            // At this point, we cannot have had any tables pre-populated because the table map has not been exposed
            // externally.
            // The table map is still managed by its creating scope, and so does not need extra steps to ensure
            // liveness.
            // There's also no aggregation update listener to retain yet.
            final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
            try (final ChunkSource.GetContext tableMapKeysGetContext =
                    tableMapKeysSource.makeGetContext(WRITE_THROUGH_CHUNK_SIZE);
                    final ChunkBoxer.BoxerKernel tableMapKeysBoxer =
                            ChunkBoxer.getBoxer(tableMapKeysSource.getChunkType(), WRITE_THROUGH_CHUNK_SIZE);
                    final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                            ResettableWritableObjectChunk.makeResettableChunk();
                    final ResettableWritableObjectChunk<WritableRowSet, Values> addedRowSetsResettableChunk =
                            ResettableWritableObjectChunk.makeResettableChunk();
                    final RowSequence.Iterator initialDestinationsIterator =
                            initialDestinations.getRowSequenceIterator()) {

                // noinspection unchecked
                final WritableObjectChunk<QueryTable, Values> tablesBackingChunk =
                        tablesResettableChunk.asWritableObjectChunk();
                // noinspection unchecked
                final WritableObjectChunk<WritableRowSet, Values> addedRowSetsBackingChunk =
                        addedRowSetsResettableChunk.asWritableObjectChunk();

                while (initialDestinationsIterator.hasMore()) {
                    final long firstSliceDestination = initialDestinationsIterator.peekNextKey();
                    final long firstBackingChunkDestination =
                            tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                    addedRowSets.resetWritableChunkToBackingStore(addedRowSetsResettableChunk, firstSliceDestination);
                    final long lastBackingChunkDestination =
                            firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                    final RowSequence initialDestinationsSlice =
                            initialDestinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                    final ObjectChunk<?, ? extends Values> tableMapKeyChunk = tableMapKeysBoxer
                            .box(tableMapKeysSource.getChunk(tableMapKeysGetContext, initialDestinationsSlice));

                    final MutableInt tableMapKeyOffset = new MutableInt();
                    initialDestinationsSlice.forAllRowKeys((final long destinationToInitialize) -> {
                        final Object tableMapKey = tableMapKeyChunk.get(tableMapKeyOffset.intValue());
                        tableMapKeyOffset.increment();

                        final int backingChunkOffset =
                                Math.toIntExact(destinationToInitialize - firstBackingChunkDestination);
                        final QueryTable unexpectedExistingTable = tablesBackingChunk.get(backingChunkOffset);
                        if (unexpectedExistingTable != null) {
                            throw new IllegalStateException("Found unexpected existing table " + unexpectedExistingTable
                                    + " in initial slot " + destinationToInitialize + " for key " + tableMapKey);
                        }

                        final WritableRowSet initialRowSet =
                                extractAndClearIndex(addedRowSetsBackingChunk, backingChunkOffset).toTracking();
                        initialRowSet.compact();
                        final QueryTable newTable = makeSubTable(initialRowSet);
                        tablesBackingChunk.set(backingChunkOffset, newTable);
                        final Table unexpectedPrepopulatedTable = tableMap.put(tableMapKey, newTable);
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
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(@NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        this.aggregationUpdateListener = aggregationUpdateListener;
        if (aggregationUpdateListener instanceof NotificationQueue.Dependency) {
            tableMap.setDependency((NotificationQueue.Dependency) aggregationUpdateListener);
        }
        tableMap.addParentReference(aggregationUpdateListener);
        tableMap.values().forEach(st -> ((DynamicNode) st).addParentReference(aggregationUpdateListener));
        return IterativeChunkedAggregationOperator.super.initializeRefreshing(resultTable, aggregationUpdateListener);
    }

    @Override
    public void resetForStep(@NotNull final TableUpdate upstream) {
        stepShiftedDestinations = RowSetFactory.empty();
        final boolean upstreamModified = upstream.modified().isNonempty() && upstream.modifiedColumnSet().nonempty();
        if (upstreamModified) {
            // We re-use this for all sub-tables that have modifies.
            upstreamToResultTransformer.clearAndTransform(upstream.modifiedColumnSet(), resultModifiedColumnSet);
            stepValuesModified = resultModifiedColumnSet.nonempty();
        } else {
            stepValuesModified = false;
        }
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream,
            @NotNull final RowSet newDestinations) {
        if (downstream.added().isEmpty() && downstream.removed().isEmpty() && downstream.modified().isEmpty()
                && stepShiftedDestinations.isEmpty()) {
            stepShiftedDestinations = null;
            return;
        }
        if (downstream.added().isNonempty()) {
            try (final RowSequence resurrectedDestinations = downstream.added().minus(newDestinations)) {
                propagateResurrectedDestinations(resurrectedDestinations);
                propagateNewDestinations(newDestinations);
            }
        }
        propagateUpdatesToRemovedDestinations(downstream.removed());
        try (final RowSequence modifiedOrShiftedDestinations = downstream.modified().union(stepShiftedDestinations)) {
            stepShiftedDestinations = null;
            propagateUpdatesToModifiedDestinations(modifiedOrShiftedDestinations);
        }
    }

    private void propagateResurrectedDestinations(@NotNull final RowSequence resurrectedDestinations) {
        if (resurrectedDestinations.isEmpty()) {
            return;
        }
        try (final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> addedRowSetsResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator resurrectedDestinationsIterator =
                        resurrectedDestinations.getRowSequenceIterator()) {
            // Destinations that were added can't have any removals, modifications, or shifts.

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk = tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> addedRowSetsBackingChunk =
                    addedRowSetsResettableChunk.asWritableObjectChunk();

            while (resurrectedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = resurrectedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                addedRowSets.resetWritableChunkToBackingStore(addedRowSetsResettableChunk, firstSliceDestination);
                final long lastBackingChunkDestination = firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final RowSequence resurrectedDestinationsSlice =
                        resurrectedDestinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                resurrectedDestinationsSlice.forAllRowKeys((final long resurrectedDestination) -> {
                    final int backingChunkOffset =
                            Math.toIntExact(resurrectedDestination - firstBackingChunkDestination);

                    final QueryTable resurrectedTable = tablesBackingChunk.get(backingChunkOffset);
                    if (resurrectedTable == NONEXISTENT_TABLE) {
                        return;
                    }
                    if (resurrectedTable == null) {
                        throw new IllegalStateException("Missing resurrected table in slot " + resurrectedDestination
                                + " for table map key " + tableMapKeysSource.get(resurrectedDestination));
                    }

                    // This table existed already, and has been "resurrected" after becoming empty previously. We must
                    // notify.

                    final TableUpdateImpl downstream = new TableUpdateImpl();

                    downstream.added = nullToEmpty(extractAndClearIndex(addedRowSetsBackingChunk, backingChunkOffset));
                    downstream.removed = RowSetFactory.empty();
                    downstream.modified = RowSetFactory.empty();
                    downstream.shifted = RowSetShiftData.EMPTY;
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                    resurrectedTable.getRowSet().writableCast().compact();

                    Assert.assertion(resurrectedTable.getRowSet().isEmpty(), "resurrectedTable.build().isEmpty()");
                    resurrectedTable.getRowSet().writableCast().insert(downstream.added());
                    resurrectedTable.notifyListeners(downstream);
                });
            }
        }
    }

    private void propagateNewDestinations(@NotNull final RowSequence newDestinations) {
        if (newDestinations.isEmpty()) {
            return;
        }
        final boolean retainedTableMap = tableMap.tryRetainReference();
        final boolean retainedAggregationUpdateListener = aggregationUpdateListener.tryRetainReference();
        final boolean allowCreation = retainedTableMap && retainedAggregationUpdateListener;
        final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
        try (final ChunkSource.GetContext tableMapKeysGetContext =
                tableMapKeysSource.makeGetContext(WRITE_THROUGH_CHUNK_SIZE);
                final ChunkBoxer.BoxerKernel tableMapKeysBoxer =
                        ChunkBoxer.getBoxer(tableMapKeysSource.getChunkType(), WRITE_THROUGH_CHUNK_SIZE);
                final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> addedRowSetsResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> removedRowSetsResettableChunk =
                        allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> modifiedRowSetsResettableChunk =
                        allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersResettableChunk =
                        allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator newDestinationsIterator = newDestinations.getRowSequenceIterator()) {

            // noinspection unchecked
            final WritableObjectChunk<QueryTable, Values> tablesBackingChunk =
                    tablesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> addedRowSetsBackingChunk =
                    addedRowSetsResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> removedRowSetsBackingChunk =
                    allowCreation ? null : removedRowSetsResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> modifiedRowSetsBackingChunk =
                    allowCreation ? null : modifiedRowSetsResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersBackingChunk =
                    allowCreation ? null : shiftDataBuildersResettableChunk.asWritableObjectChunk();

            while (newDestinationsIterator.hasMore()) {
                final long firstSliceDestination = newDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                addedRowSets.resetWritableChunkToBackingStore(addedRowSetsResettableChunk, firstSliceDestination);
                if (!allowCreation) {
                    removedRowSets.resetWritableChunkToBackingStore(removedRowSetsResettableChunk,
                            firstSliceDestination);
                    modifiedRowSets.resetWritableChunkToBackingStore(modifiedRowSetsResettableChunk,
                            firstSliceDestination);
                    shiftDataBuilders.resetWritableChunkToBackingStore(shiftDataBuildersResettableChunk,
                            firstSliceDestination);
                }
                final long lastBackingChunkDestination = firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final RowSequence newDestinationsSlice =
                        newDestinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                final ObjectChunk<?, ? extends Values> tableMapKeyChunk = tableMapKeysBoxer
                        .box(tableMapKeysSource.getChunk(tableMapKeysGetContext, newDestinationsSlice));

                final MutableInt tableMapKeyOffset = new MutableInt();
                newDestinationsSlice.forAllRowKeys((final long newDestination) -> {
                    final Object tableMapKey = tableMapKeyChunk.get(tableMapKeyOffset.intValue());
                    tableMapKeyOffset.increment();

                    final int backingChunkOffset = Math.toIntExact(newDestination - firstBackingChunkDestination);
                    final QueryTable unexpectedExistingTable = tablesBackingChunk.get(backingChunkOffset);
                    if (unexpectedExistingTable != null) {
                        throw new IllegalStateException("Found unexpected existing table " + unexpectedExistingTable
                                + " in new slot " + newDestination + " for key " + tableMapKey);
                    }

                    final QueryTable prepopulatedTable;

                    if (allowCreation) {
                        final MutableBoolean newTableAllocated = new MutableBoolean();
                        final QueryTable newOrPrepopulatedTable =
                                (QueryTable) tableMap.computeIfAbsent(tableMapKey, (unused) -> {
                                    final WritableRowSet newRowSet = extractAndClearIndex(
                                            addedRowSetsBackingChunk, backingChunkOffset).toTracking();
                                    newRowSet.compact();
                                    final QueryTable newTable = makeSubTable(newRowSet);
                                    tablesBackingChunk.set(backingChunkOffset, newTable);
                                    newTableAllocated.setTrue();
                                    return newTable;
                                });
                        prepopulatedTable = newTableAllocated.booleanValue() ? null : newOrPrepopulatedTable;
                    } else {
                        prepopulatedTable = (QueryTable) tableMap.get(tableMapKey);
                    }
                    if (prepopulatedTable != null) {
                        tablesBackingChunk.set(backingChunkOffset, prepopulatedTable);

                        // "New" table already existed due to TableMap.populateKeys.
                        // We can ignore allowCreation; the table exists already, and must already retain appropriate
                        // referents.
                        // Additionally, we must notify of added rows.
                        final TableUpdateImpl downstream = new TableUpdateImpl();

                        downstream.added =
                                nullToEmpty(extractAndClearIndex(addedRowSetsBackingChunk, backingChunkOffset));
                        downstream.removed = RowSetFactory.empty();
                        downstream.modified = RowSetFactory.empty();
                        downstream.shifted = RowSetShiftData.EMPTY;
                        downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                        prepopulatedTable.getRowSet().writableCast().insert(downstream.added());
                        prepopulatedTable.notifyListeners(downstream);
                    } else if (!allowCreation) {
                        // We will never try to create this table again, or accumulate further state for it.
                        tablesBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE);
                        addedRowSetsBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_ROW_SET);
                        removedRowSetsBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_ROW_SET);
                        modifiedRowSetsBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_ROW_SET);
                        shiftDataBuildersBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_SHIFT_BUILDER);
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
        UpdateGraphProcessor.DEFAULT.checkInitiateTableOperation();
        populateInternal(key);
    }

    private void populateInternal(final Object key) {
        // We don't bother with complicated retention or non-existent result handling, here.
        // If the user is calling TableMap.populateKeys (the only way to get here) they'd better be sure of liveness
        // already, and they won't thank us for adding non-existent table tombstones rather than blowing up.
        final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
        try {
            tableMap.computeIfAbsent(key, (unused) -> makeSubTable(null));
        } finally {
            if (setCallSite) {
                QueryPerformanceRecorder.clearCallsite();
            }
        }
    }

    private QueryTable makeSubTable(@Nullable final RowSet initialRowSetToInsert) {
        // We don't start from initialRowSetToInsert because it is expected to be a WritableRowSetImpl.
        final QueryTable subTable =
                parentTable.getSubTable(RowSetFactory.empty().toTracking(), resultModifiedColumnSet);
        subTable.setRefreshing(parentTable.isRefreshing());
        if (aggregationUpdateListener != null) {
            subTable.addParentReference(aggregationUpdateListener);
        }
        attributeCopier.copyAttributes(parentTable, subTable);
        if (initialRowSetToInsert != null) {
            subTable.getRowSet().writableCast().insert(initialRowSetToInsert);
            ((TrackingWritableRowSet) subTable.getRowSet()).initializePreviousValue();
        }
        return subTable;
    }

    private void propagateUpdatesToRemovedDestinations(@NotNull final RowSequence removedDestinations) {
        if (removedDestinations.isEmpty()) {
            return;
        }
        try (final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> removedRowSetsResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator removedDestinationsIterator = removedDestinations.getRowSequenceIterator()) {
            // Destinations that were completely removed can't have any additions, modifications, or shifts.

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk = tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> removedRowSetsBackingChunk =
                    removedRowSetsResettableChunk.asWritableObjectChunk();

            while (removedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = removedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                removedRowSets.resetWritableChunkToBackingStore(removedRowSetsResettableChunk, firstSliceDestination);
                final long lastBackingChunkDestination = firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final RowSequence removedDestinationsSlice =
                        removedDestinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                removedDestinationsSlice.forAllRowKeys((final long removedDestination) -> {
                    final int backingChunkOffset = Math.toIntExact(removedDestination - firstBackingChunkDestination);

                    final QueryTable removedTable = tablesBackingChunk.get(backingChunkOffset);
                    if (removedTable == NONEXISTENT_TABLE) {
                        return;
                    }
                    if (removedTable == null) {
                        throw new IllegalStateException("Missing removed table in slot " + removedDestination
                                + " for table map key " + tableMapKeysSource.get(removedDestination));
                    }

                    final TableUpdateImpl downstream = new TableUpdateImpl();

                    downstream.added = RowSetFactory.empty();
                    downstream.removed =
                            nullToEmpty(extractAndClearIndex(removedRowSetsBackingChunk, backingChunkOffset));
                    downstream.modified = RowSetFactory.empty();
                    downstream.shifted = RowSetShiftData.EMPTY;
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                    removedTable.getRowSet().writableCast().remove(downstream.removed());
                    removedTable.getRowSet().writableCast().compact();
                    Assert.assertion(removedTable.getRowSet().isEmpty(), "removedTable.build().isEmpty()");
                    removedTable.notifyListeners(downstream);
                });
            }
        }
    }

    private void propagateUpdatesToModifiedDestinations(@NotNull final RowSequence modifiedDestinations) {
        if (modifiedDestinations.isEmpty()) {
            return;
        }
        try (final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> addedRowSetsResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> removedRowSetsResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<WritableRowSet, Values> modifiedRowSetsResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator modifiedDestinationsIterator =
                        modifiedDestinations.getRowSequenceIterator()) {

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk = tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> addedRowSetsBackingChunk =
                    addedRowSetsResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> removedRowSetsBackingChunk =
                    removedRowSetsResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<WritableRowSet, Values> modifiedRowSetsBackingChunk =
                    modifiedRowSetsResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersBackingChunk =
                    shiftDataBuildersResettableChunk.asWritableObjectChunk();

            while (modifiedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = modifiedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                // The (valid) assumption is that the other write-through resets will address the same range.
                addedRowSets.resetWritableChunkToBackingStore(addedRowSetsResettableChunk, firstSliceDestination);
                removedRowSets.resetWritableChunkToBackingStore(removedRowSetsResettableChunk, firstSliceDestination);
                modifiedRowSets.resetWritableChunkToBackingStore(modifiedRowSetsResettableChunk, firstSliceDestination);
                shiftDataBuilders.resetWritableChunkToBackingStore(shiftDataBuildersResettableChunk,
                        firstSliceDestination);
                final long lastBackingChunkDestination = firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final RowSequence modifiedDestinationsSlice =
                        modifiedDestinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                modifiedDestinationsSlice.forAllRowKeys((final long modifiedDestination) -> {
                    final int backingChunkOffset = Math.toIntExact(modifiedDestination - firstBackingChunkDestination);

                    final QueryTable modifiedTable = tablesBackingChunk.get(backingChunkOffset);
                    if (modifiedTable == NONEXISTENT_TABLE) {
                        return;
                    }
                    if (modifiedTable == null) {
                        throw new IllegalStateException("Missing modified table in slot " + modifiedDestination
                                + " for table map key " + tableMapKeysSource.get(modifiedDestination));
                    }

                    final TableUpdateImpl downstream = new TableUpdateImpl();

                    downstream.added = nullToEmpty(extractAndClearIndex(addedRowSetsBackingChunk, backingChunkOffset));
                    downstream.removed =
                            nullToEmpty(extractAndClearIndex(removedRowSetsBackingChunk, backingChunkOffset));
                    downstream.modified = stepValuesModified
                            ? nullToEmpty(extractAndClearIndex(modifiedRowSetsBackingChunk, backingChunkOffset))
                            : RowSetFactory.empty();
                    downstream.shifted =
                            extractAndClearShiftDataBuilder(shiftDataBuildersBackingChunk, backingChunkOffset);
                    downstream.modifiedColumnSet =
                            downstream.modified().isEmpty() ? ModifiedColumnSet.EMPTY : resultModifiedColumnSet;

                    if (downstream.removed().isNonempty()) {
                        modifiedTable.getRowSet().writableCast().remove(downstream.removed());
                    }
                    if (downstream.shifted().nonempty()) {
                        downstream.shifted().apply(((WritableRowSet) modifiedTable.getRowSet()));
                    }
                    if (downstream.added().isNonempty()) {
                        modifiedTable.getRowSet().writableCast().insert(downstream.added());
                    }

                    modifiedTable.getRowSet().writableCast().compact();

                    modifiedTable.notifyListeners(downstream);
                });
            }
        }
    }

    private static WritableRowSet extractAndClearIndex(
            @NotNull final WritableObjectChunk<WritableRowSet, Values> rowSetChunk,
            final int offset) {
        final WritableRowSet rowSet = rowSetChunk.get(offset);
        Assert.neq(rowSet, "rowSet", NONEXISTENT_TABLE_ROW_SET, "NONEXISTENT_TABLE_ROW_SET");
        if (rowSet != null) {
            rowSetChunk.set(offset, null);
        }
        return rowSet;
    }

    private static RowSet nullToEmpty(@Nullable final RowSet rowSet) {
        return rowSet == null ? RowSetFactory.empty() : rowSet;
    }

    private static RowSetShiftData extractAndClearShiftDataBuilder(
            @NotNull final WritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersChunk,
            final int offset) {
        final RowSetShiftData.SmartCoalescingBuilder shiftDataBuilder = shiftDataBuildersChunk.get(offset);
        Assert.neq(shiftDataBuilder, "shiftDataBuilder", NONEXISTENT_TABLE_SHIFT_BUILDER,
                "NONEXISTENT_TABLE_SHIFT_BUILDER");
        if (shiftDataBuilder == null) {
            return RowSetShiftData.EMPTY;
        }
        shiftDataBuildersChunk.set(offset, null);
        return shiftDataBuilder.build();
    }

    @Override
    public void propagateFailure(@NotNull final Throwable originalException, @NotNull TableListener.Entry sourceEntry) {
        tableMap.values().forEach(st -> ((BaseTable) st).notifyListenersOnError(originalException, sourceEntry));
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
