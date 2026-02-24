//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.Partition;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.ResettableWritableObjectChunk;
import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.rowset.impl.AdaptiveOrderedLongSetBuilderRandom;
import io.deephaven.engine.rowset.impl.WritableRowSetImpl;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableListener;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.ConstituentDependency;
import io.deephaven.engine.table.impl.PrecomputedRowSetBuilderRandom;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.iterators.ChunkedObjectColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * An {@link IterativeChunkedAggregationOperator} used in the implementation of {@link Table#partitionBy} and
 * {@link Partition}.
 */
public final class PartitionByChunkedOperator implements IterativeChunkedAggregationOperator {

    // region nonexistent table sentinels

    /**
     * Sentinel value for the row set builders belonging to a table that was never created because either the result
     * table was no longer live or the aggregation update listener was no longer live. Should be used for assignment and
     * reference equality tests, only.
     */
    private static final RowSetBuilderRandom NONEXISTENT_TABLE_ROW_SET_BUILDER = RowSetFactory.builderRandom();

    /**
     * Helper value for the following sentinel values but not used directly for comparison
     */
    private static final WritableRowSet NONEXISTENT_TABLE_ROW_SET = RowSetFactory.empty();

    /**
     * Sentinel value for the shift builder belonging to a table that was never created because either the result table
     * was no longer live or the aggregation update listener was no longer live. Should be used for assignment and
     * reference equality tests, only.
     */
    private static final RowSetShiftData.SmartCoalescingBuilder NONEXISTENT_TABLE_SHIFT_BUILDER =
            new RowSetShiftData.SmartCoalescingBuilder(NONEXISTENT_TABLE_ROW_SET);
    /**
     * Sentinel value for a table that was never created because either the result table was no longer live or the
     * aggregation update listener was no longer live. Should be used for assignment and reference equality tests, only.
     */
    private static final QueryTable NONEXISTENT_TABLE =
            new QueryTable(NONEXISTENT_TABLE_ROW_SET.toTracking(), Collections.emptyMap());

    // endregion nonexistent table sentinels

    public interface AttributeCopier {
        void copyAttributes(@NotNull QueryTable parentTable, @NotNull QueryTable subTable);
    }

    private final QueryTable parentTable;
    private final String resultName;
    private final Map<String, Object> subTableAttributes;

    private final String callSite;

    private final ObjectArraySource<QueryTable> tables;
    private final ObjectArraySource<Object> addedBuilders;
    private final ObjectArraySource<Object> removedBuilders;
    private final ObjectArraySource<Object> modifiedBuilders;
    private final ObjectArraySource<RowSetShiftData.SmartCoalescingBuilder> shiftDataBuilders;
    private final ModifiedColumnSet resultModifiedColumnSet;
    private final ModifiedColumnSet.Transformer upstreamToResultTransformer;

    private boolean initialized;

    private volatile Table resultTable;
    private volatile LivenessReferent aggregationUpdateListener;

    /**
     * <p>
     * RowSet to keep track of destinations whose constituent tables have been updated in any way (due to adds, removes,
     * modifies, or shifts). Due to the chunk-oriented nature of operator data handling, this may also include added and
     * removed destinations. This is recorded separately from the {@code stateModified} chunks used to produce
     * downstream modifies, because updates to constituents are <em>not</em> modifies to their rows in the aggregation
     * output table, but rather table updates to be propagated.
     * <p>
     * This exists in each cycle between {@link IterativeChunkedAggregationOperator#resetForStep(TableUpdate, int)} and
     * {@link IterativeChunkedAggregationOperator#propagateUpdates(TableUpdate, RowSet)}
     * <p>
     * If this ever becomes necessary in other operators, it could be moved out to the helper the way modified
     * destination tracking already is.
     * <p>
     * We should consider whether to instead use a random builder, but the current approach seemed reasonable for now.
     */
    private WritableRowSet stepUpdatedDestinations;
    /**
     * Whether any of our value columns were modified on this step, necessitating modification propagation.
     */
    private boolean stepValuesModified;

    /**
     * Construct a new operator.
     *
     * @param unadjustedParentTable The parent table for all sub-tables, without any key-column dropping or similar
     *        already applied
     * @param parentTable The parent table for all sub-tables, with any key-column dropping or similar already applied
     * @param attributeCopier A procedure that copies attributes or similar from its first argument (the parent table)
     *        to its second (the sub-table)
     * @param keyColumnNames The key columns
     */
    PartitionByChunkedOperator(@NotNull final QueryTable unadjustedParentTable,
            @NotNull final QueryTable parentTable,
            @NotNull final String resultName,
            @NotNull final AttributeCopier attributeCopier,
            @NotNull final String... keyColumnNames) {
        this.parentTable = parentTable;
        this.resultName = resultName;
        initialized = false;

        callSite = QueryPerformanceRecorder.getCallerLine();

        tables = new ObjectArraySource<>(QueryTable.class);
        addedBuilders = new ObjectArraySource<>(Object.class);

        // Note: Sub-tables always share their ColumnSource map with the parent table, so they can all use this result
        // MCS.
        resultModifiedColumnSet = new ModifiedColumnSet(parentTable.getModifiedColumnSetForUpdates());

        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            // noinspection resource
            final QueryTable attributeDestination = parentTable.getSubTable(RowSetFactory.empty().toTracking());
            // NB: Sub-table inherits "systemicness" from current thread at construction
            attributeCopier.copyAttributes(parentTable, attributeDestination);
            subTableAttributes = attributeDestination.getAttributes();
        }

        if (parentTable.isRefreshing()) {
            removedBuilders = new ObjectArraySource<>(Object.class);
            modifiedBuilders = new ObjectArraySource<>(Object.class);
            shiftDataBuilders = new ObjectArraySource<>(RowSetShiftData.SmartCoalescingBuilder.class);

            final Set<String> keyColumnNameSet = Arrays.stream(keyColumnNames).collect(Collectors.toSet());
            final Set<String> unadjustedParentColumnNameSet = unadjustedParentTable.getDefinition().getColumnNameSet();
            final String[] retainedResultColumnNames = parentTable.getDefinition().getColumnStream()
                    .map(ColumnDefinition::getName)
                    .filter(Predicate.not(keyColumnNameSet::contains))
                    .filter(unadjustedParentColumnNameSet::contains)
                    .toArray(String[]::new);
            final ModifiedColumnSet[] retainedResultModifiedColumnSets = Arrays.stream(retainedResultColumnNames)
                    .map(parentTable::newModifiedColumnSet) // This is safe because we're not giving empty input
                    .toArray(ModifiedColumnSet[]::new);
            upstreamToResultTransformer = unadjustedParentTable.getModifiedColumnSetForUpdates().newTransformer(
                    retainedResultColumnNames,
                    retainedResultModifiedColumnSets);
        } else {
            removedBuilders = null;
            modifiedBuilders = null;
            shiftDataBuilders = null;
            upstreamToResultTransformer = null;
        }
    }

    @Override
    public void addChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        final AdaptiveOrderedLongSetBuilderRandom chunkDestinationsBuilder =
                stepUpdatedDestinations == null ? null : new AdaptiveOrderedLongSetBuilderRandom();
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        final int numDestinations = startPositions.size();
        for (int di = 0; di < numDestinations; ++di) {
            final int startPosition = startPositions.get(di);
            final int runLength = length.get(di);
            final long destination = destinations.get(startPosition);
            if (!initialized) {
                // during initialization, all rows are guaranteed to be in-order
                accumulateToBuilderSequential(addedBuilders, inputRowKeysAsOrdered, startPosition,
                        runLength, destination);
            } else {
                accumulateToBuilderRandom(addedBuilders, inputRowKeysAsOrdered, startPosition, runLength,
                        destination);
            }
            if (chunkDestinationsBuilder != null) {
                chunkDestinationsBuilder.addKey(destination);
            }
        }
        if (chunkDestinationsBuilder != null) {
            try (final RowSet chunkUpdatedDestinations =
                    new WritableRowSetImpl(chunkDestinationsBuilder.getOrderedLongSet())) {
                stepUpdatedDestinations.insert(chunkUpdatedDestinations);
            }
        }
    }

    @Override
    public void removeChunk(final BucketedContext bucketedContext, final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys,
            @NotNull final IntChunk<RowKeys> destinations, @NotNull final IntChunk<ChunkPositions> startPositions,
            @NotNull final IntChunk<ChunkLengths> length, @NotNull final WritableBooleanChunk<Values> stateModified) {
        Assert.eqNull(values, "values");
        Assert.neqNull(stepUpdatedDestinations, "stepUpdatedDestinations");
        final AdaptiveOrderedLongSetBuilderRandom chunkDestinationsBuilder = new AdaptiveOrderedLongSetBuilderRandom();
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        final int numDestinations = startPositions.size();
        for (int di = 0; di < numDestinations; ++di) {
            final int startPosition = startPositions.get(di);
            final int runLength = length.get(di);
            final long destination = destinations.get(startPosition);
            accumulateToBuilderRandom(removedBuilders, inputRowKeysAsOrdered, startPosition, runLength,
                    destination);
            chunkDestinationsBuilder.addKey(destination);
        }
        try (final RowSet chunkUpdatedDestinations =
                new WritableRowSetImpl(chunkDestinationsBuilder.getOrderedLongSet())) {
            stepUpdatedDestinations.insert(chunkUpdatedDestinations);
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
        Assert.neqNull(stepUpdatedDestinations, "stepUpdatedDestinations");
        final AdaptiveOrderedLongSetBuilderRandom chunkDestinationsBuilder = new AdaptiveOrderedLongSetBuilderRandom();
        final int numDestinations = startPositions.size();
        for (int di = 0; di < numDestinations; ++di) {
            final int startPosition = startPositions.get(di);
            final int runLength = length.get(di);
            final long destination = destinations.get(startPosition);

            if (appendShifts(preShiftRowKeys, postShiftRowKeys, startPosition, runLength, destination)) {
                chunkDestinationsBuilder.addKey(destination);
            }
        }
        try (final RowSet chunkUpdatedDestinations =
                new WritableRowSetImpl(chunkDestinationsBuilder.getOrderedLongSet())) {
            stepUpdatedDestinations.insert(chunkUpdatedDestinations);
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
        Assert.neqNull(stepUpdatedDestinations, "stepUpdatedDestinations");
        final AdaptiveOrderedLongSetBuilderRandom chunkDestinationsBuilder = new AdaptiveOrderedLongSetBuilderRandom();
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        final int numDestinations = startPositions.size();
        for (int di = 0; di < numDestinations; ++di) {
            final int startPosition = startPositions.get(di);
            final int runLength = length.get(di);
            final long destination = destinations.get(startPosition);
            accumulateToBuilderRandom(modifiedBuilders, inputRowKeysAsOrdered, startPosition, runLength,
                    destination);
            chunkDestinationsBuilder.addKey(destination);
        }
        try (final RowSet chunkUpdatedDestinations =
                new WritableRowSetImpl(chunkDestinationsBuilder.getOrderedLongSet())) {
            stepUpdatedDestinations.insert(chunkUpdatedDestinations);
        }
    }

    @Override
    public boolean addChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        Assert.eqNull(values, "values");
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        if (!initialized) {
            accumulateToBuilderSequential(addedBuilders, inputRowKeysAsOrdered, 0, chunkSize, destination);
        } else {
            accumulateToBuilderRandom(addedBuilders, inputRowKeysAsOrdered, 0, chunkSize, destination);
        }
        if (stepUpdatedDestinations != null) {
            stepUpdatedDestinations.insert(destination);
        }
        return false;
    }

    @Override
    public boolean addRowSet(SingletonContext context, RowSet rowSet, long destination) {
        if (!initialized) {
            accumulateToBuilderSequential(addedBuilders, rowSet, destination);
        } else {
            accumulateToBuilderRandom(addedBuilders, rowSet, destination);
        }
        if (stepUpdatedDestinations != null) {
            stepUpdatedDestinations.insert(destination);
        }
        return false;
    }

    @Override
    public boolean removeChunk(final SingletonContext singletonContext, final int chunkSize,
            final Chunk<? extends Values> values,
            @NotNull final LongChunk<? extends RowKeys> inputRowKeys, final long destination) {
        Assert.eqNull(values, "values");
        Assert.neqNull(stepUpdatedDestinations, "stepUpdatedDestinations");
        // noinspection unchecked
        final LongChunk<OrderedRowKeys> inputRowKeysAsOrdered = (LongChunk<OrderedRowKeys>) inputRowKeys;
        accumulateToBuilderRandom(removedBuilders, inputRowKeysAsOrdered, 0, chunkSize, destination);
        stepUpdatedDestinations.insert(destination);
        return false;
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
        Assert.neqNull(stepUpdatedDestinations, "stepUpdatedDestinations");
        if (appendShifts(preShiftRowKeys, postShiftRowKeys, 0, preShiftRowKeys.size(), destination)) {
            stepUpdatedDestinations.insert(destination);
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
        accumulateToBuilderRandom(modifiedBuilders, rowKeysAsOrdered, 0, rowKeys.size(), destination);
        stepUpdatedDestinations.insert(destination);
        return false;
    }

    private static void accumulateToBuilderSequential(
            @NotNull final ObjectArraySource<Object> rowSetBuilderColumn,
            @NotNull final LongChunk<OrderedRowKeys> rowKeysToAdd,
            final int start, final int length, final long destination) {
        final RowSetBuilderSequential builder = (RowSetBuilderSequential) rowSetBuilderColumn.getUnsafe(destination);
        if (builder == null) {
            // create (and store) a new builder, fill with these keys
            final RowSetBuilderSequential newBuilder = RowSetFactory.builderSequential();
            newBuilder.appendOrderedRowKeysChunk(rowKeysToAdd, start, length);
            rowSetBuilderColumn.set(destination, newBuilder);
            return;
        }
        // add the keys to the stored builder
        builder.appendOrderedRowKeysChunk(rowKeysToAdd, start, length);
    }

    private static void accumulateToBuilderSequential(
            @NotNull final ObjectArraySource<Object> rowSetBuilderColumn,
            @NotNull final RowSet rowSetToAdd,
            final long destination) {
        final RowSetBuilderSequential builder = (RowSetBuilderSequential) rowSetBuilderColumn.getUnsafe(destination);
        if (builder == null) {
            // create (and store) a new builder, fill with this rowset
            final RowSetBuilderSequential newBuilder = RowSetFactory.builderSequential();
            newBuilder.appendRowSequence(rowSetToAdd);
            rowSetBuilderColumn.set(destination, newBuilder);
            return;
        }
        // add the rowset to the stored builder
        builder.appendRowSequence(rowSetToAdd);
    }

    private static void accumulateToBuilderRandom(
            @NotNull final ObjectArraySource<Object> rowSetBuilderColumn,
            @NotNull final LongChunk<OrderedRowKeys> rowKeysToAdd,
            final int start, final int length, final long destination) {
        final RowSetBuilderRandom builder = (RowSetBuilderRandom) rowSetBuilderColumn.getUnsafe(destination);
        if (builder == NONEXISTENT_TABLE_ROW_SET_BUILDER) {
            return;
        }
        if (builder == null) {
            // create (and store) a new builder, fill with these keys
            final RowSetBuilderRandom newBuilder = RowSetFactory.builderRandom();
            newBuilder.addOrderedRowKeysChunk(rowKeysToAdd, start, length);
            rowSetBuilderColumn.set(destination, newBuilder);
            return;
        }
        // add the keys to the stored builder
        builder.addOrderedRowKeysChunk(rowKeysToAdd, start, length);
    }

    private static void accumulateToBuilderRandom(
            @NotNull final ObjectArraySource<Object> rowSetBuilderColumn,
            @NotNull final RowSet rowSetToAdd,
            final long destination) {
        final RowSetBuilderRandom builder = (RowSetBuilderRandom) rowSetBuilderColumn.getUnsafe(destination);
        if (builder == NONEXISTENT_TABLE_ROW_SET_BUILDER) {
            return;
        }
        if (builder == null) {
            // create (and store) a new builder, fill with this rowset
            final RowSetBuilderRandom newBuilder = RowSetFactory.builderRandom();
            newBuilder.addRowSet(rowSetToAdd);
            rowSetBuilderColumn.set(destination, newBuilder);
            return;
        }
        // add the rowset to the stored builder
        builder.addRowSet(rowSetToAdd);
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
            final RowSetBuilderRandom removedRowSetBuilder =
                    (RowSetBuilderRandom) removedBuilders.getUnsafe(destination);
            final RowSet preShiftKeys;
            if (removedRowSetBuilder == null) {
                preShiftKeys = tableRowSet.copy();
            } else {
                final WritableRowSet rs = removedRowSetBuilder.build();
                preShiftKeys = tableRowSet.minus(rs);

                // no future modifications are expected, replace the exhausted builder with a dummy for compatibility
                RowSetBuilderRandom newBuilder = PrecomputedRowSetBuilderRandom.createFromRowSet(rs);
                removedBuilders.set(destination, newBuilder);
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
        addedBuilders.ensureCapacity(tableSize);
        if (parentTable.isRefreshing()) {
            removedBuilders.ensureCapacity(tableSize);
            modifiedBuilders.ensureCapacity(tableSize);
            shiftDataBuilders.ensureCapacity(tableSize);
        }
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getResultColumns() {
        return Collections.singletonMap(resultName, tables);
    }

    @Override
    public void startTrackingPrevValues() {}

    @Override
    public void propagateInitialState(@NotNull final QueryTable resultTable, int startingDestinationsCount) {
        Assert.neqTrue(initialized, "initialized");
        // NB: We must use a flat row set over all the starting destinations rather than the result table's row set,
        // because we need to ensure that destinations from an initial key table are built even when not preserving
        // empty states in the output row set.
        try (final RowSet initialDestinations = RowSetFactory.flat(startingDestinationsCount)) {
            if (initialDestinations.isNonempty()) {
                // This is before resultTable and aggregationUpdateListener are set (if they will be). We're still in
                // our initialization scope, and don't need to do anything special to ensure liveness.
                final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
                // @formatter:off
                try (final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                     final ResettableWritableObjectChunk<RowSetBuilderSequential, Values> addedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                     final RowSequence.Iterator initialDestinationsIterator =
                        initialDestinations.getRowSequenceIterator()) {
                    // @formatter:on

                    // noinspection unchecked
                    final WritableObjectChunk<QueryTable, Values> tablesBackingChunk =
                            tablesResettableChunk.asWritableObjectChunk();
                    // noinspection unchecked
                    final WritableObjectChunk<RowSetBuilderSequential, Values> addedBuildersBackingChunk =
                            addedBuildersResettableChunk.asWritableObjectChunk();

                    while (initialDestinationsIterator.hasMore()) {
                        final long firstSliceDestination = initialDestinationsIterator.peekNextKey();
                        final long firstBackingChunkDestination =
                                tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                        addedBuilders.resetWritableChunkToBackingStore(addedBuildersResettableChunk,
                                firstSliceDestination);
                        final long lastBackingChunkDestination =
                                firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                        final RowSequence initialDestinationsSlice =
                                initialDestinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                        initialDestinationsSlice.forAllRowKeys((final long destinationToInitialize) -> {
                            final int backingChunkOffset =
                                    Math.toIntExact(destinationToInitialize - firstBackingChunkDestination);
                            // we use sequential builders during initialization
                            final WritableRowSet initialRowSet =
                                    extractAndClearBuilderSequential(addedBuildersBackingChunk, backingChunkOffset);
                            final QueryTable newTable = makeSubTable(initialRowSet);
                            tablesBackingChunk.set(backingChunkOffset, newTable);
                        });
                    }
                } finally {
                    if (setCallSite) {
                        QueryPerformanceRecorder.clearCallsite();
                    }
                }
            }
        }
        initialized = true;
    }

    @Override
    public UnaryOperator<ModifiedColumnSet> initializeRefreshing(
            @NotNull final QueryTable resultTable,
            @NotNull final LivenessReferent aggregationUpdateListener) {
        this.resultTable = resultTable;
        this.aggregationUpdateListener = aggregationUpdateListener;

        // This is safe to do here since Partition is the only operation that creates a column of
        // NotificationQueue.Dependency (i.e. Table), and since we only allow one Partition operator per aggregation.
        // Otherwise, it would be better to do this in the helper.
        ConstituentDependency.install(resultTable, (NotificationQueue.Dependency) aggregationUpdateListener);

        // Link constituents
        new ChunkedObjectColumnIterator<Table>(tables, resultTable.getRowSet())
                .forEachRemaining(this::linkTableReferences);

        // This operator never reports modifications
        return ignored -> ModifiedColumnSet.EMPTY;
    }

    private void linkTableReferences(@NotNull final Table subTable) {
        // The sub-table will not continue to update unless the aggregation update listener remains reachable and live.
        subTable.addParentReference(aggregationUpdateListener);
        // We don't need to add a reference from the result to the sub-table, because the sub-table is reachable from
        // the constituent column for the (GC) life of the result, and because the ConstituentDependency will enforce
        // the necessary dependency from result to sub-table.
        // We do need to ensure that the sub-table remains live for the lifetime of the result.
        resultTable.manage(subTable);
    }

    @Override
    public boolean resetForStep(@NotNull final TableUpdate upstream, int startingDestinationsCount) {
        stepUpdatedDestinations = RowSetFactory.empty();
        final boolean upstreamModified = upstream.modified().isNonempty() && upstream.modifiedColumnSet().nonempty();
        if (upstreamModified) {
            // We re-use this for all sub-tables that have modifies.
            upstreamToResultTransformer.clearAndTransform(upstream.modifiedColumnSet(), resultModifiedColumnSet);
            stepValuesModified = resultModifiedColumnSet.nonempty();
        } else {
            stepValuesModified = false;
        }
        return false;
    }

    @Override
    public void propagateUpdates(@NotNull final TableUpdate downstream, @NotNull final RowSet newDestinations) {
        stepUpdatedDestinations.remove(downstream.added());
        stepUpdatedDestinations.remove(downstream.removed());
        if (downstream.added().isEmpty() && downstream.removed().isEmpty() && stepUpdatedDestinations.isEmpty()) {
            try (final SafeCloseable ignored = stepUpdatedDestinations) {
                stepUpdatedDestinations = null;
            }
            return;
        }
        if (downstream.added().isNonempty()) {
            try (final RowSequence resurrectedDestinations = downstream.added().minus(newDestinations)) {
                propagateResurrectedDestinations(resurrectedDestinations);
                propagateNewDestinations(newDestinations);
            }
        }
        propagateUpdatesToRemovedDestinations(downstream.removed());

        try (final RowSequence ignored = stepUpdatedDestinations) {
            propagateUpdatesToModifiedDestinations(stepUpdatedDestinations);
            stepUpdatedDestinations = null;
        }
    }

    private void propagateResurrectedDestinations(@NotNull final RowSequence resurrectedDestinations) {
        if (resurrectedDestinations.isEmpty()) {
            return;
        }
        try (final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator resurrectedDestinationsIterator =
                        resurrectedDestinations.getRowSequenceIterator()) {
            // Destinations that were added can't have any removals, modifications, or shifts.

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk = tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersBackingChunk =
                    addedBuildersResettableChunk.asWritableObjectChunk();

            while (resurrectedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = resurrectedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                addedBuilders.resetWritableChunkToBackingStore(addedBuildersResettableChunk,
                        firstSliceDestination);
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
                        throw new IllegalStateException("Missing resurrected table in slot " + resurrectedDestination);
                    }

                    // This table existed already, and has been "resurrected" after becoming empty previously. We must
                    // notify.

                    final TableUpdateImpl downstream = new TableUpdateImpl();

                    downstream.added =
                            nullToEmpty(extractAndClearBuilderRandom(addedBuildersBackingChunk, backingChunkOffset));
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
        final boolean retainedResultTable = resultTable.tryRetainReference();
        final boolean retainedAggregationUpdateListener = aggregationUpdateListener.tryRetainReference();
        final boolean allowCreation = retainedResultTable && retainedAggregationUpdateListener;
        final boolean setCallSite = QueryPerformanceRecorder.setCallsite(callSite);
        try (final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersResettableChunk =
                        allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> modifiedBuildersResettableChunk =
                        allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersResettableChunk =
                        allowCreation ? null : ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator newDestinationsIterator = newDestinations.getRowSequenceIterator()) {

            // noinspection unchecked
            final WritableObjectChunk<QueryTable, Values> tablesBackingChunk =
                    tablesResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersBackingChunk =
                    addedBuildersResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersBackingChunk =
                    allowCreation ? null : removedBuildersResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> modifiedBuildersBackingChunk =
                    allowCreation ? null : modifiedBuildersResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersBackingChunk =
                    allowCreation ? null : shiftDataBuildersResettableChunk.asWritableObjectChunk();

            while (newDestinationsIterator.hasMore()) {
                final long firstSliceDestination = newDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                addedBuilders.resetWritableChunkToBackingStore(addedBuildersResettableChunk,
                        firstSliceDestination);
                if (!allowCreation) {
                    removedBuilders.resetWritableChunkToBackingStore(removedBuildersResettableChunk,
                            firstSliceDestination);
                    modifiedBuilders.resetWritableChunkToBackingStore(modifiedBuildersResettableChunk,
                            firstSliceDestination);
                    shiftDataBuilders.resetWritableChunkToBackingStore(shiftDataBuildersResettableChunk,
                            firstSliceDestination);
                }
                final long lastBackingChunkDestination = firstBackingChunkDestination + tablesBackingChunk.size() - 1;
                final RowSequence newDestinationsSlice =
                        newDestinationsIterator.getNextRowSequenceThrough(lastBackingChunkDestination);

                newDestinationsSlice.forAllRowKeys((final long newDestination) -> {
                    final int backingChunkOffset = Math.toIntExact(newDestination - firstBackingChunkDestination);
                    if (allowCreation) {
                        final WritableRowSet newRowSet =
                                extractAndClearBuilderRandom(addedBuildersBackingChunk, backingChunkOffset);
                        final QueryTable newTable = makeSubTable(newRowSet);
                        linkTableReferences(newTable);
                        tablesBackingChunk.set(backingChunkOffset, newTable);
                    } else {
                        // We will never try to create this table again, or accumulate further state for it.
                        tablesBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE);
                        addedBuildersBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_ROW_SET_BUILDER);
                        removedBuildersBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_ROW_SET_BUILDER);
                        modifiedBuildersBackingChunk.set(backingChunkOffset, NONEXISTENT_TABLE_ROW_SET_BUILDER);
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
            if (retainedResultTable) {
                resultTable.dropReference();
            }
        }
    }

    private QueryTable makeSubTable(@NotNull final WritableRowSet initialRowSetToInsert) {
        initialRowSetToInsert.compact();
        final QueryTable subTable = parentTable.getSubTable(
                initialRowSetToInsert.toTracking(), resultModifiedColumnSet, subTableAttributes);
        subTable.setRefreshing(parentTable.isRefreshing());
        return subTable;
    }

    private void propagateUpdatesToRemovedDestinations(@NotNull final RowSequence removedDestinations) {
        if (removedDestinations.isEmpty()) {
            return;
        }
        try (final ResettableWritableObjectChunk<QueryTable, Values> tablesResettableChunk =
                ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator removedDestinationsIterator = removedDestinations.getRowSequenceIterator()) {
            // Destinations that were completely removed can't have any additions, modifications, or shifts.

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk = tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersBackingChunk =
                    removedBuildersResettableChunk.asWritableObjectChunk();

            while (removedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = removedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                removedBuilders.resetWritableChunkToBackingStore(removedBuildersResettableChunk,
                        firstSliceDestination);
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
                        throw new IllegalStateException("Missing removed table in slot " + removedDestination);
                    }

                    final TableUpdateImpl downstream = new TableUpdateImpl();

                    downstream.added = RowSetFactory.empty();
                    downstream.removed =
                            nullToEmpty(extractAndClearBuilderRandom(removedBuildersBackingChunk, backingChunkOffset));
                    downstream.modified = RowSetFactory.empty();
                    downstream.shifted = RowSetShiftData.EMPTY;
                    downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;

                    removedTable.getRowSet().writableCast().remove(downstream.removed());
                    removedTable.getRowSet().writableCast().compact();
                    Assert.assertion(removedTable.getRowSet().isEmpty(), "removedTable.getRowSet().isEmpty()");
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
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetBuilderRandom, Values> modifiedBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final ResettableWritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersResettableChunk =
                        ResettableWritableObjectChunk.makeResettableChunk();
                final RowSequence.Iterator modifiedDestinationsIterator =
                        modifiedDestinations.getRowSequenceIterator()) {

            // noinspection unchecked
            final ObjectChunk<QueryTable, Values> tablesBackingChunk = tablesResettableChunk.asObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> addedBuildersBackingChunk =
                    addedBuildersResettableChunk.asWritableObjectChunk();

            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> removedBuildersBackingChunk =
                    removedBuildersResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetBuilderRandom, Values> modifiedBuildersBackingChunk =
                    modifiedBuildersResettableChunk.asWritableObjectChunk();
            // noinspection unchecked
            final WritableObjectChunk<RowSetShiftData.SmartCoalescingBuilder, Values> shiftDataBuildersBackingChunk =
                    shiftDataBuildersResettableChunk.asWritableObjectChunk();

            while (modifiedDestinationsIterator.hasMore()) {
                final long firstSliceDestination = modifiedDestinationsIterator.peekNextKey();
                final long firstBackingChunkDestination =
                        tables.resetWritableChunkToBackingStore(tablesResettableChunk, firstSliceDestination);
                // The (valid) assumption is that the other write-through resets will address the same range.
                addedBuilders.resetWritableChunkToBackingStore(addedBuildersResettableChunk,
                        firstSliceDestination);
                removedBuilders.resetWritableChunkToBackingStore(removedBuildersResettableChunk,
                        firstSliceDestination);
                modifiedBuilders.resetWritableChunkToBackingStore(modifiedBuildersResettableChunk,
                        firstSliceDestination);
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
                        throw new IllegalStateException("Missing modified table in slot " + modifiedDestination);
                    }

                    final TableUpdateImpl downstream = new TableUpdateImpl();

                    downstream.added =
                            nullToEmpty(extractAndClearBuilderRandom(addedBuildersBackingChunk, backingChunkOffset));
                    downstream.removed =
                            nullToEmpty(extractAndClearBuilderRandom(removedBuildersBackingChunk, backingChunkOffset));
                    downstream.modified = stepValuesModified
                            ? nullToEmpty(
                                    extractAndClearBuilderRandom(modifiedBuildersBackingChunk, backingChunkOffset))
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

    private static WritableRowSet extractAndClearBuilderRandom(
            @NotNull final WritableObjectChunk<RowSetBuilderRandom, Values> builderChunk,
            final int offset) {
        final RowSetBuilderRandom builder = builderChunk.get(offset);
        Assert.neq(builder, "builder", NONEXISTENT_TABLE_ROW_SET_BUILDER,
                "NONEXISTENT_TABLE_ROW_SET_BUILDER");
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
        new ChunkedObjectColumnIterator<QueryTable>(tables, resultTable.getRowSet())
                .forEachRemaining(st -> st.notifyListenersOnError(originalException, sourceEntry));
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
