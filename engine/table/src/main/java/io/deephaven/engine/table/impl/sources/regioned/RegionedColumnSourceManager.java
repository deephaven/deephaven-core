//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.*;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.DelayedErrorNotifier;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSource.*;

/**
 * Manage column sources made up of regions in their own row key address space.
 */
public class RegionedColumnSourceManager
        implements ColumnSourceManager, DelegatingLivenessNode, PushdownPredicateManager {

    private static final Logger log = LoggerFactory.getLogger(RegionedColumnSourceManager.class);

    /**
     * How many locations to test for data index or other location-level metadata before we give up and assume the
     * location has no useful information for push-down purposes.
     */
    private static final int PUSHDOWN_LOCATION_SAMPLES = Configuration.getInstance()
            .getIntegerForClassWithDefault(RegionedColumnSourceManager.class, "pushdownLocationSamples", 5);

    /**
     * The liveness node to which this column source manager will delegate.
     */
    private final LivenessNode livenessNode;

    /**
     * Whether this column source manager is serving a refreshing dynamic table.
     */
    private final boolean isRefreshing;

    /**
     * The column definitions that define our column sources.
     */
    private final List<ColumnDefinition<?>> columnDefinitions;

    /**
     * The column sources that make up this table.
     */
    private final Map<String, RegionedColumnSource<?>> columnSources = new LinkedHashMap<>();

    /**
     * The column sources of this table as a map from column source to column name. This map should not be accessed
     * directly, but rather through {@link #columnSourceToName()}.
     */
    private volatile IdentityHashMap<ColumnSource<?>, String> columnSourceToName;

    /**
     * An unmodifiable view of columnSources.
     */
    private final Map<String, ? extends ColumnSource<?>> sharedColumnSources =
            Collections.unmodifiableMap(columnSources);

    /**
     * State for table locations that have been added, but have never been found to exist with non-zero size.
     */
    private final KeyedObjectHashMap<ImmutableTableLocationKey, EmptyTableLocationEntry> emptyTableLocations =
            new KeyedObjectHashMap<>(EMPTY_TABLE_LOCATION_ENTRY_KEY);

    /**
     * State for table locations that provide the regions backing our column sources.
     */
    private final KeyedObjectHashMap<ImmutableTableLocationKey, IncludedTableLocationEntry> includedTableLocations =
            new KeyedObjectHashMap<>(INCLUDED_TABLE_LOCATION_ENTRY_KEY);

    /**
     * Table locations that provide the regions backing our column sources, in insertion order.
     */
    private final List<IncludedTableLocationEntry> orderedIncludedTableLocations = new ArrayList<>();

    private static final String LOCATION_COLUMN_NAME = "__TableLocation";
    private static final ColumnDefinition<TableLocation> LOCATION_COLUMN_DEFINITION =
            ColumnDefinition.fromGenericType(LOCATION_COLUMN_NAME, TableLocation.class);
    private static final String ROWS_SET_COLUMN_NAME = "__RowSet";
    private static final ColumnDefinition<RowSet> ROWS_SET_COLUMN_DEFINITION =
            ColumnDefinition.fromGenericType(ROWS_SET_COLUMN_NAME, RowSet.class);
    private static final TableDefinition SIMPLE_LOCATION_TABLE_DEFINITION = TableDefinition.of(
            LOCATION_COLUMN_DEFINITION,
            ROWS_SET_COLUMN_DEFINITION);

    /**
     * Non-empty table locations stored in a table. Rows are keyed by {@link IncludedTableLocationEntry#regionIndex
     * region index}.
     */
    private final QueryTable includedLocationsTable;
    private final Map<String, WritableColumnSource<?>> partitioningColumnValueSources;
    private final ObjectArraySource<TableLocation> locationSource;
    private final ObjectArraySource<RowSet> rowSetSource;
    private final ModifiedColumnSet rowSetModifiedColumnSet;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @ReferentialIntegrity
    private final Collection<DataIndex> retainedDataIndexes = new ArrayList<>();

    /**
     * List of tracked location keys to release at the end of the cycle.
     */
    private final List<AbstractTableLocation> releasedLocations = new ArrayList<>();

    /**
     * A reference to a delayed error notifier for the {@link #includedLocationsTable}, if one is pending.
     */
    @ReferentialIntegrity
    private Runnable delayedErrorReference;

    /**
     * The next region index to assign to a location. We increment for each new location and will not reuse indices from
     * regions that were removed.
     */
    private int nextRegionIndex = 0;

    /**
     * List of locations that were removed this cycle. Will be swapped each cycle with {@code invalidatedLocations} and
     * cleared.
     */
    private List<IncludedTableLocationEntry> removedTableLocations = new ArrayList<>();

    /**
     * List of locations to invalidate at the end of the cycle. Swapped with {@code removedTableLocations} each cycle to
     * avoid reallocating.
     */
    private List<IncludedTableLocationEntry> invalidatedLocations = new ArrayList<>();

    /**
     * Will invalidate the locations at the end of the cycle after all downstream updates are complete.
     */
    final UpdateCommitter<?> invalidateCommitter;

    /**
     * Construct a column manager with the specified component factory and definitions.
     *
     * @param isRefreshing Whether the table using this column source manager is refreshing
     * @param removeAllowed Whether the table using this column source manager may remove locations
     * @param componentFactory The component factory
     * @param columnDefinitions The column definitions
     */
    RegionedColumnSourceManager(
            final boolean isRefreshing,
            final boolean removeAllowed,
            @NotNull final RegionedTableComponentFactory componentFactory,
            @NotNull final ColumnToCodecMappings codecMappings,
            @NotNull final List<ColumnDefinition<?>> columnDefinitions) {

        this.isRefreshing = isRefreshing;
        this.columnDefinitions = columnDefinitions;
        for (final ColumnDefinition<?> columnDefinition : columnDefinitions) {
            columnSources.put(
                    columnDefinition.getName(),
                    componentFactory.createRegionedColumnSource(this, columnDefinition, codecMappings));
        }

        // Create the table that will hold the location data
        partitioningColumnValueSources = columnDefinitions.stream()
                .filter(ColumnDefinition::isPartitioning)
                .collect(Collectors.toMap(
                        ColumnDefinition::getName,
                        cd -> ArrayBackedColumnSource.getMemoryColumnSource(cd.getDataType(), cd.getComponentType())));
        locationSource = new ObjectArraySource<>(TableLocation.class);
        rowSetSource = new ObjectArraySource<>(RowSet.class);
        final LinkedHashMap<String, ColumnSource<?>> columnSourceMap =
                new LinkedHashMap<>(partitioningColumnValueSources);
        columnSourceMap.put(LOCATION_COLUMN_NAME, locationSource);
        columnSourceMap.put(ROWS_SET_COLUMN_NAME, rowSetSource);
        final TableDefinition locationTableDefinition = partitioningColumnValueSources.isEmpty()
                ? SIMPLE_LOCATION_TABLE_DEFINITION
                : TableDefinition.inferFrom(columnSourceMap);

        if (isRefreshing) {
            livenessNode = new LivenessArtifact() {
                @Override
                protected void destroy() {
                    super.destroy();
                    // NB: we do not want to null out any subscriptionBuffers here, as they may still be in use by a
                    // notification delivery running currently with this destroy. We also do not want to clear the table
                    // location maps as these locations may still be useful for static tables.
                    for (final EmptyTableLocationEntry entry : emptyTableLocations.values()) {
                        if (entry.subscriptionBuffer != null) {
                            entry.subscriptionBuffer.reset();
                        }
                    }
                    for (final IncludedTableLocationEntry entry : includedTableLocations.values()) {
                        if (entry.subscriptionBuffer != null) {
                            entry.subscriptionBuffer.reset();
                        }
                    }
                }
            };
        } else {
            // This RCSM will be managing table locations to prevent them from being de-scoped but will not otherwise
            // participate in the liveness management process.
            livenessNode = new ReferenceCountedLivenessNode(false) {};
            livenessNode.retainReference();
        }

        try (final SafeCloseable ignored = isRefreshing ? LivenessScopeStack.open() : null) {
            includedLocationsTable = new QueryTable(
                    locationTableDefinition,
                    RowSetFactory.empty().toTracking(),
                    columnSourceMap,
                    null, // No need to pre-allocate a MCS
                    null // No attributes to provide (not add-only or append-only, because locations can grow)
            ) {
                {
                    if (!removeAllowed) {
                        setFlat();
                    }
                    setRefreshing(isRefreshing);
                }
            };
            if (isRefreshing) {
                livenessNode.manage(includedLocationsTable);
                rowSetModifiedColumnSet = includedLocationsTable.newModifiedColumnSet(ROWS_SET_COLUMN_NAME);
            } else {
                rowSetModifiedColumnSet = null;
            }
        }

        invalidateCommitter = new UpdateCommitter<>(this,
                ExecutionContext.getContext().getUpdateGraph(),
                RegionedColumnSourceManager::invalidateAndRelease);
    }

    /**
     * Activated by the invalidateCommitter to invalidate populated locations and to release all the managed locations
     * that we no longer care about.
     */
    private synchronized void invalidateAndRelease() {
        invalidatedLocations.forEach(IncludedTableLocationEntry::invalidate);
        invalidatedLocations.clear();
        if (!releasedLocations.isEmpty()) {
            unmanage(releasedLocations.stream());
            releasedLocations.clear();
        }
    }

    @Override
    public synchronized void addLocation(@NotNull final TableLocation tableLocation) {
        final IncludedTableLocationEntry includedLocation = includedTableLocations.get(tableLocation.getKey());
        final EmptyTableLocationEntry emptyLocation = emptyTableLocations.get(tableLocation.getKey());

        if (includedLocation == null && emptyLocation == null) {
            if (log.isDebugEnabled()) {
                log.debug().append("LOCATION_ADDED:").append(tableLocation.toString()).endl();
            }
            // Hold on to this table location.
            livenessNode.manage(tableLocation);
            emptyTableLocations.add(new EmptyTableLocationEntry(tableLocation));
        } else {
            // Duplicate location - not allowed
            final TableLocation duplicateLocation =
                    includedLocation != null ? includedLocation.location : emptyLocation.location;
            if (tableLocation != duplicateLocation) {
                // If it ever transpires that we need to compare the locations and not just detect a second add, then
                // we need to add plumbing to include access to the location provider
                throw new TableDataException(
                        "Data Routing Configuration error: TableDataService elements overlap at locations " +
                                tableLocation.toStringDetailed() + " and " + duplicateLocation.toStringDetailed());
            } else {
                // This is unexpected - we got the identical table location object twice
                // If we ever get this, some thought needs to go into why.
                throw new TableDataException("Unexpected: TableDataService returned the same location twice: " +
                        tableLocation.toStringDetailed());
            }
        }
    }

    @Override
    public synchronized void removeLocationKey(final @NotNull ImmutableTableLocationKey locationKey) {
        final IncludedTableLocationEntry includedLocation = includedTableLocations.remove(locationKey);
        final EmptyTableLocationEntry emptyLocation = emptyTableLocations.remove(locationKey);

        if (emptyLocation != null) {
            if (log.isDebugEnabled()) {
                log.debug().append("EMPTY_LOCATION_REMOVED:").append(locationKey.toString()).endl();
            }
            if (emptyLocation.location instanceof AbstractTableLocation) {
                releasedLocations.add((AbstractTableLocation) emptyLocation.location);
                invalidateCommitter.maybeActivate();
            }
        } else if (includedLocation != null) {
            orderedIncludedTableLocations.remove(includedLocation);
            removedTableLocations.add(includedLocation);
            if (includedLocation.location instanceof AbstractTableLocation) {
                releasedLocations.add((AbstractTableLocation) includedLocation.location);
            }
            invalidateCommitter.maybeActivate();
        }
    }

    @Override
    public synchronized TrackingWritableRowSet initialize() {
        Assert.assertion(includedLocationsTable.isEmpty(), "includedLocationsTable.isEmpty()");

        // Do our first pass over the locations to include as many as possible and build the initial row set
        final TableUpdateImpl update = update(true);
        // noinspection resource
        final TrackingWritableRowSet initialRowSet = update.added().writableCast().toTracking();
        update.added = null;
        update.release();

        // Add single-column data indexes for all partitioning columns, whether refreshing or not
        columnDefinitions.stream().filter(ColumnDefinition::isPartitioning).forEach(cd -> {
            try (final SafeCloseable ignored = isRefreshing ? LivenessScopeStack.open() : null) {
                final DataIndex partitioningIndex =
                        new PartitioningColumnDataIndex<>(cd.getName(), columnSources.get(cd.getName()), this);
                retainedDataIndexes.add(partitioningIndex);
                if (isRefreshing) {
                    livenessNode.manage(partitioningIndex);
                }
                DataIndexer.of(initialRowSet).addDataIndex(partitioningIndex);
            }
        });

        // If we're static, add all data indexes present in the included locations
        if (!isRefreshing && initialRowSet.isNonempty()) {
            // Use the first location as a proxy for the whole table; since data indexes must be complete over all
            // locations, this is a valid approach.
            final TableLocation firstLocation = includedTableLocations.iterator().next().location;
            for (final String[] keyColumnNames : firstLocation.getDataIndexColumns()) {
                // Skip adding additional indexes on partitioning columns
                if (keyColumnNames.length == 1 && partitioningColumnValueSources.containsKey(keyColumnNames[0])) {
                    continue;
                }
                // Here, we assume the data index is present on all included locations. MergedDataIndex.validate() will
                // be used to test this before attempting to materialize the data index table later on.
                final ColumnSource<?>[] keySources = Arrays.stream(keyColumnNames)
                        .map(columnSources::get)
                        .toArray(ColumnSource[]::new);
                if (Arrays.stream(keySources).anyMatch(Objects::isNull)) {
                    // If any of the key sources are missing (e.g. because the user supplied a TableDefinition that
                    // excludes them, or has used a RedefinableTable operation to drop them), we can't create the
                    // DataIndex.
                    continue;
                }
                final DataIndex mergedIndex = new MergedDataIndex(keyColumnNames, keySources, this);
                retainedDataIndexes.add(mergedIndex);
                // Not refreshing, so no need to manage mergedIndex
                DataIndexer.of(initialRowSet).addDataIndex(mergedIndex);
            }
        }
        return initialRowSet;
    }

    @Override
    public synchronized TableUpdate refresh() {
        if (!isRefreshing) {
            throw new UnsupportedOperationException("Cannot refresh a static table");
        }
        return update(false);
    }

    @Override
    public void deliverError(@NotNull final Throwable error, @Nullable final TableListener.Entry entry) {
        // Notify any listeners to the locations table that we had an error
        final long currentStep = includedLocationsTable.getUpdateGraph().clock().currentStep();
        if (includedLocationsTable.getLastNotificationStep() == currentStep) {
            delayedErrorReference = new DelayedErrorNotifier(error, entry, includedLocationsTable);
        } else {
            includedLocationsTable.notifyListenersOnError(error, entry);
            includedLocationsTable.forceReferenceCountToZero();
        }
    }

    private TableUpdateImpl update(final boolean initializing) {
        final RowSetBuilderSequential addedRowSetBuilder = RowSetFactory.builderSequential();

        final RowSetBuilderSequential removedRowSetBuilder =
                removedTableLocations.isEmpty() ? null : RowSetFactory.builderSequential();
        final RowSetBuilderSequential removedRegionBuilder =
                removedTableLocations.isEmpty() ? null : RowSetFactory.builderSequential();

        // Sort the removed locations by region index, so that we can process them in order.
        removedTableLocations.sort(Comparator.comparingInt(e -> e.regionIndex));
        for (final IncludedTableLocationEntry removedLocation : removedTableLocations) {
            final long regionFirstKey = RegionedColumnSource.getFirstRowKey(removedLocation.regionIndex);
            removedRowSetBuilder.appendRowSequenceWithOffset(removedLocation.rowSetAtLastUpdate, regionFirstKey);
            removedRegionBuilder.appendKey(removedLocation.regionIndex);
        }

        // Swap invalidatedLocations with removedTableLocations.
        final List<IncludedTableLocationEntry> tmpTableLocations = removedTableLocations;
        removedTableLocations = invalidatedLocations;
        invalidatedLocations = tmpTableLocations;
        Assert.eqTrue(removedTableLocations.isEmpty(), "removedTableLocations.isEmpty()");

        final RowSetBuilderSequential modifiedRegionBuilder = initializing ? null : RowSetFactory.builderSequential();

        // Ordering matters, since we're using a sequential builder.
        for (final IncludedTableLocationEntry entry : orderedIncludedTableLocations) {
            if (entry.pollUpdates(addedRowSetBuilder)) {
                // Changes were detected, update the row set in the table and mark the row/column as modified.
                /*
                 * We should consider adding an UpdateCommitter to close() the previous row sets for modified locations.
                 * This is not important for current implementations, since they always allocate new, flat RowSets.
                 */
                rowSetSource.set(entry.regionIndex, entry.rowSetAtLastUpdate.shift(getFirstRowKey(entry.regionIndex)));
                if (modifiedRegionBuilder != null) {
                    modifiedRegionBuilder.appendKey(entry.regionIndex);
                }
            }
        }

        //@formatter:off
        final Collection<EmptyTableLocationEntry> entriesToInclude = StreamSupport.stream(Spliterators.spliterator(
                                emptyTableLocations.iterator(),
                                emptyTableLocations.size(),
                                Spliterator.IMMUTABLE | Spliterator.DISTINCT | Spliterator.NONNULL),
                        true) //@formatter:on
                .peek(EmptyTableLocationEntry::refresh)
                .filter((final EmptyTableLocationEntry emptyEntry) -> {
                    final RowSet locationRowSet = emptyEntry.location.getRowSet();
                    if (locationRowSet == null) {
                        return false;
                    }
                    if (locationRowSet.isEmpty()) {
                        locationRowSet.close();
                        return false;
                    }
                    emptyEntry.initialRowSet = locationRowSet;
                    return true;
                }).collect(Collectors.toList());

        emptyTableLocations.removeAll(entriesToInclude);

        final RowSetBuilderSequential addedRegionBuilder =
                entriesToInclude.isEmpty() ? null : RowSetFactory.builderSequential();

        final int prevMaxIndex = nextRegionIndex;
        final int maxIndex = nextRegionIndex + (entriesToInclude.isEmpty() ? 0 : entriesToInclude.size());
        if (!entriesToInclude.isEmpty()) {
            partitioningColumnValueSources.values().forEach(
                    (final WritableColumnSource<?> wcs) -> wcs.ensureCapacity(maxIndex));
            locationSource.ensureCapacity(maxIndex);
            rowSetSource.ensureCapacity(maxIndex);

            entriesToInclude.stream().sorted().forEachOrdered((final EmptyTableLocationEntry entryToInclude) -> {
                final IncludedTableLocationEntry entry = new IncludedTableLocationEntry(entryToInclude);
                includedTableLocations.add(entry);
                orderedIncludedTableLocations.add(entry);
                entry.processInitial(addedRowSetBuilder, entryToInclude.initialRowSet);

                // We have a new location, add the row set to the table and mark the row as added.
                // @formatter:off
                // noinspection rawtypes,unchecked
                partitioningColumnValueSources.forEach(
                        (final String key, final WritableColumnSource wcs) ->
                                wcs.set(entry.regionIndex, entry.location.getKey().getPartitionValue(key)));
                // @formatter:on
                locationSource.set(entry.regionIndex, entry.location);
                rowSetSource.set(entry.regionIndex, entry.rowSetAtLastUpdate.shift(getFirstRowKey(entry.regionIndex)));
                addedRegionBuilder.appendKey(entry.regionIndex);
            });
        }
        final RowSet addedRegions = addedRegionBuilder == null ? RowSetFactory.empty() : addedRegionBuilder.build();

        if (addedRegions.isNonempty()) {
            includedLocationsTable.getRowSet().writableCast().insert(addedRegions);
        }

        if (initializing) {
            Assert.eqZero(prevMaxIndex, "prevMaxIndex");
            if (isRefreshing) {
                rowSetSource.startTrackingPrevValues();
                includedLocationsTable.getRowSet().writableCast().initializePreviousValue();
                includedLocationsTable.initializeLastNotificationStep(includedLocationsTable.getUpdateGraph().clock());
            } else {
                emptyTableLocations.clear();
            }
        } else {
            final RowSet modifiedRegions = modifiedRegionBuilder.build();
            final RowSet removedRegions =
                    removedRegionBuilder == null ? RowSetFactory.empty() : removedRegionBuilder.build();
            if (addedRegions.isEmpty() && modifiedRegions.isEmpty() && removedRegions.isEmpty()) {
                SafeCloseable.closeAll(addedRegions, modifiedRegions, removedRegions);
            } else {
                includedLocationsTable.getRowSet().writableCast().remove(removedRegions);
                final TableUpdate update = new TableUpdateImpl(
                        addedRegions,
                        removedRegions,
                        modifiedRegions,
                        RowSetShiftData.EMPTY,
                        modifiedRegions.isNonempty() ? rowSetModifiedColumnSet : ModifiedColumnSet.EMPTY);
                includedLocationsTable.notifyListeners(update);
            }
        }
        return new TableUpdateImpl(
                addedRowSetBuilder.build(),
                removedRowSetBuilder == null ? RowSetFactory.empty() : removedRowSetBuilder.build(),
                RowSetFactory.empty(),
                RowSetShiftData.EMPTY,
                ModifiedColumnSet.EMPTY);
    }

    @Override
    public final synchronized Collection<TableLocation> allLocations() {
        //@formatter:off
        return Stream.concat(
                        orderedIncludedTableLocations.stream().map(e -> e.location),
                        emptyTableLocations.values().stream().sorted().map(e -> e.location)) //@formatter:on
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public final synchronized Collection<TableLocation> includedLocations() {
        return orderedIncludedTableLocations.stream().map(e -> e.location)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    private synchronized ArrayList<IncludedTableLocationEntry> includedLocationEntries() {
        return new ArrayList<>(orderedIncludedTableLocations);
    }

    @Override
    public Table locationTable() {
        return includedLocationsTable;
    }

    @Override
    public String locationColumnName() {
        return LOCATION_COLUMN_NAME;
    }

    @Override
    public String rowSetColumnName() {
        return ROWS_SET_COLUMN_NAME;
    }

    @Override
    public final synchronized boolean isEmpty() {
        return includedTableLocations.isEmpty();
    }

    @Override
    public final Map<String, ? extends ColumnSource<?>> getColumnSources() {
        return sharedColumnSources;
    }

    public LivenessNode asLivenessNode() {
        return livenessNode;
    }

    /**
     * State keeper for a table location and its subscription buffer if it hasn't been found to have a non-null,
     * non-zero size yet.
     */
    private class EmptyTableLocationEntry implements Comparable<EmptyTableLocationEntry> {

        private final TableLocation location;
        private final TableLocationUpdateSubscriptionBuffer subscriptionBuffer;

        private RowSet initialRowSet;

        private EmptyTableLocationEntry(@NotNull final TableLocation location) {
            this.location = location;
            if (isRefreshing) {
                subscriptionBuffer = new TableLocationUpdateSubscriptionBuffer(location);
            } else {
                subscriptionBuffer = null;
            }
        }

        private void refresh() {
            if (subscriptionBuffer != null) {
                subscriptionBuffer.processPending();
            } else {
                // NB: This should be hit only once per entry - subscription buffers handle all "isRefreshing"
                // (i.e. "live") tables, regardless of whether the underlying locations support subscriptions.
                location.refresh();
            }
        }

        @Override
        public int compareTo(@NotNull final EmptyTableLocationEntry other) {
            if (this == other) {
                return 0;
            }
            return location.getKey().compareTo(other.location.getKey());
        }
    }

    private static final KeyedObjectKey<ImmutableTableLocationKey, EmptyTableLocationEntry> EMPTY_TABLE_LOCATION_ENTRY_KEY =
            new KeyedObjectKey.Basic<>() {

                @Override
                public ImmutableTableLocationKey getKey(
                        @NotNull final EmptyTableLocationEntry emptyTableLocationEntry) {
                    return emptyTableLocationEntry.location.getKey();
                }
            };

    /**
     * State-keeper for a table location and its column locations, once it's been found to have a positive size.
     */
    private class IncludedTableLocationEntry implements Comparable<IncludedTableLocationEntry> {

        private final TableLocation location;
        private final TableLocationUpdateSubscriptionBuffer subscriptionBuffer;

        // New regions indices are assigned in order of insertion, starting from 0 with no re-use of removed indices.
        // If this logic changes, the `getTableAttributes()` logic needs to be updated.
        private final int regionIndex = nextRegionIndex++;

        /**
         * RowSet in the region's space, not the table's space.
         */
        private RowSet rowSetAtLastUpdate;

        private IncludedTableLocationEntry(final EmptyTableLocationEntry nonexistentEntry) {
            this.location = nonexistentEntry.location;
            this.subscriptionBuffer = nonexistentEntry.subscriptionBuffer;
        }

        private void processInitial(final RowSetBuilderSequential addedRowSetBuilder, final RowSet initialRowSet) {
            Assert.neqNull(initialRowSet, "initialRowSet");
            Assert.eqTrue(initialRowSet.isNonempty(), "initialRowSet.isNonempty()");
            Assert.eqNull(rowSetAtLastUpdate, "rowSetAtLastUpdate");
            if (initialRowSet.lastRowKey() > ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK) {
                throw new TableDataException(String.format(
                        "Location %s has initial last key %#016X, larger than maximum supported key %#016X",
                        location, initialRowSet.lastRowKey(),
                        ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK));
            }

            final long regionFirstKey = getFirstRowKey(regionIndex);
            initialRowSet.forAllRowKeyRanges((subRegionFirstKey, subRegionLastKey) -> addedRowSetBuilder
                    .appendRange(regionFirstKey + subRegionFirstKey, regionFirstKey + subRegionLastKey));

            for (final ColumnDefinition<?> columnDefinition : columnDefinitions) {
                final RegionedColumnSource<?> regionedColumnSource = columnSources.get(columnDefinition.getName());
                final ColumnLocation columnLocation = location.getColumnLocation(columnDefinition.getName());
                Assert.eq(regionIndex, "regionIndex", regionedColumnSource.addRegion(columnDefinition, columnLocation),
                        "regionedColumnSource.addRegion((definition, location)");
            }

            rowSetAtLastUpdate = initialRowSet;
        }

        /**
         * Returns {@code true} if there were changes to the row set for this location.
         */
        private boolean pollUpdates(final RowSetBuilderSequential addedRowSetBuilder) {
            Assert.neqNull(subscriptionBuffer, "subscriptionBuffer"); // Effectively, this is asserting "isRefreshing".
            try {
                if (!subscriptionBuffer.processPending()) {
                    return false;
                }
            } catch (Exception ex) {
                invalidate();
                throw ex;
            }

            final RowSet updateRowSet = location.getRowSet();
            try {
                if (updateRowSet == null) {
                    // This should be impossible - the subscription buffer transforms a transition to null into a
                    // pending exception
                    invalidate();
                    throw new TableDataException(
                            "Location " + location + " is no longer available, data has been removed");
                }

                if (!rowSetAtLastUpdate.subsetOf(updateRowSet)) { // Bad change
                    invalidate();
                    // noinspection ThrowableNotThrown
                    Assert.statementNeverExecuted(
                            "Row keys removed at location " + location + ": "
                                    + rowSetAtLastUpdate.minus(updateRowSet));
                }

                if (rowSetAtLastUpdate.size() == updateRowSet.size()) {
                    // Nothing to do
                    return false;
                }
                if (updateRowSet.lastRowKey() > ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK) {
                    throw new TableDataException(String.format(
                            "Location %s has updated last key %#016X, larger than maximum supported key %#016X",
                            location, updateRowSet.lastRowKey(),
                            ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK));
                }

                if (log.isDebugEnabled()) {
                    log.debug().append("LOCATION_SIZE_CHANGE:").append(location.toString())
                            .append(",FROM:").append(rowSetAtLastUpdate.size())
                            .append(",TO:").append(updateRowSet.size()).endl();
                }
                try (final RowSet addedRowSet = updateRowSet.minus(rowSetAtLastUpdate)) {
                    final long regionFirstKey = getFirstRowKey(regionIndex);
                    addedRowSet.forAllRowKeyRanges((subRegionFirstKey, subRegionLastKey) -> addedRowSetBuilder
                            .appendRange(regionFirstKey + subRegionFirstKey, regionFirstKey + subRegionLastKey));
                }
            } finally {
                /*
                 * Since we record rowSetAtLastUpdate in the RowSet column of our includedLocationsTable, we must not
                 * close() the old rowSetAtLastUpdate here. We should instead consider adding an UpdateCommitter to
                 * close() the previous RowSets for modified locations, but this is not important for current
                 * implementations since they always allocate new, flat RowSets.
                 */
                rowSetAtLastUpdate = updateRowSet;
            }
            // There was a change to the row set.
            return true;
        }

        private void invalidate() {
            columnSources.values().forEach(source -> source.invalidateRegion(regionIndex));
        }

        @Override
        public int compareTo(@NotNull final IncludedTableLocationEntry other) {
            // This Comparable implementation is currently unused, as we maintain ordering in
            // orderedIncludedTableLocations by insertion
            if (this == other) {
                return 0;
            }
            return Integer.compare(regionIndex, other.regionIndex);
        }

        /**
         * Get the row set for this region that intersects with the input row set, shifted to the region's key space.
         * The user is responsible for closing the returned row set.
         *
         * @param inputRows The input row set
         * @return A writable row set for the location
         */
        private WritableRowSet getOverlappingShiftedRowSet(final RowSet inputRows) {
            final long locationStartKey = getFirstRowKey(regionIndex);
            final long locationEndKey = getLastRowKey(regionIndex);

            // Extract the portion of inputRows that overlaps this region.
            final WritableRowSet overlappingRows = inputRows.subSetByKeyRange(locationStartKey, locationEndKey);

            // Shift to the region's key space
            overlappingRows.shiftInPlace(-locationStartKey);

            // Retain only rows that still exist in the region.
            overlappingRows.retain(rowSetAtLastUpdate);
            return overlappingRows;
        }
    }

    private static final KeyedObjectKey<ImmutableTableLocationKey, IncludedTableLocationEntry> INCLUDED_TABLE_LOCATION_ENTRY_KEY =
            new KeyedObjectKey.Basic<>() {

                @Override
                public ImmutableTableLocationKey getKey(
                        @NotNull final IncludedTableLocationEntry includedTableLocationEntry) {
                    return includedTableLocationEntry.location.getKey();
                }
            };

    public Map<String, Object> getTableAttributes(
            @NotNull TableUpdateMode tableUpdateMode,
            @NotNull TableUpdateMode tableLocationUpdateMode) {
        final Map<String, Object> attributes = new HashMap<>();
        // NOTE: Current RegionedColumnSourceManager implementation appends new locations and does not reuse
        // region indices. This is important for the following attributes to be correct.

        if (tableUpdateMode == TableUpdateMode.APPEND_ONLY
                && tableLocationUpdateMode == TableUpdateMode.STATIC) {
            // This table is APPEND_ONLY IFF the set of locations is APPEND_ONLY
            // and the location contents are STATIC
            attributes.put(Table.APPEND_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        } else if (!tableUpdateMode.removeAllowed()
                && !tableLocationUpdateMode.removeAllowed()) {
            // This table is ADD_ONLY IFF the set of locations is not allowed to remove locations
            // and the locations contents are not allowed to remove rows
            attributes.put(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
        }
        return attributes;
    }

    /**
     * Lightweight wrapper that couples an {@link IncludedTableLocationEntry} with the {@link WritableRowSet}
     * representing the subset of rows from that region that participate in a filter-pushdown operation.
     */
    private static class RegionInfoHolder implements AutoCloseable {
        private final IncludedTableLocationEntry tle;
        private final WritableRowSet rowSet;

        private RegionInfoHolder(
                @NotNull final IncludedTableLocationEntry tle,
                @NotNull final WritableRowSet rowSet) {
            this.tle = tle;
            this.rowSet = rowSet;
        }

        @Override
        public void close() {
            rowSet.close();
        }
    }

    /**
     * Return up to {@code maxRegions} regions and their corresponding row sets that overlap with the input row set. The
     * user is responsible for closing the returned objects.
     */
    private ArrayList<RegionInfoHolder> getOverlappingRegions(final RowSet inputRowSet, final int maxRegions) {
        final List<IncludedTableLocationEntry> tableLocationEntries = includedLocationEntries();
        final ArrayList<RegionInfoHolder> includedRegions = new ArrayList<>();
        try (final RegionIndexIterator rit = RegionIndexIterator.of(inputRowSet)) {
            while (includedRegions.size() < maxRegions && rit.hasNext()) {
                final int regionIndex = rit.nextInt();
                if (regionIndex >= tableLocationEntries.size()) {
                    throw new IllegalStateException("Region index " + regionIndex + " exceeds the number of included " +
                            "locations: " + tableLocationEntries.size() + " for input row set: " + inputRowSet);
                }
                final IncludedTableLocationEntry entry = tableLocationEntries.get(regionIndex);
                // Note: Based on the cost of computing overlapping row sets, we can push overlap computation into the
                // parallel region-processing jobs in the future. For now, we can compute it serially here.
                try (final WritableRowSet overlappingShiftedRowSet = entry.getOverlappingShiftedRowSet(inputRowSet)) {
                    if (overlappingShiftedRowSet.isEmpty()) {
                        throw new IllegalStateException(
                                "Overlapping row set is empty for region index " + regionIndex + " and input row set: "
                                        + inputRowSet);
                    }
                    includedRegions.add(new RegionInfoHolder(entry, overlappingShiftedRowSet.copy()));
                }
            }
        }
        return includedRegions;
    }

    /**
     * Get all regions and corresponding row set that overlap with the input row set. The user is responsible for
     * closing the returned row sets.
     *
     * @param inputRowSet The input row set
     * @return A list of included region information
     */
    private ArrayList<RegionInfoHolder> getOverlappingRegions(final RowSet inputRowSet) {
        return getOverlappingRegions(inputRowSet, Integer.MAX_VALUE);
    }

    @Override
    public void estimatePushdownFilterCost(WhereFilter filter, RowSet selection,
            RowSet fullSet, boolean usePrev, PushdownFilterContext context, JobScheduler jobScheduler,
            LongConsumer onComplete, Consumer<Exception> onError) {
        final List<RegionInfoHolder> overlappingRegionsSample =
                getOverlappingRegions(selection, PUSHDOWN_LOCATION_SAMPLES);
        final MutableLong min = new MutableLong(Long.MAX_VALUE);
        jobScheduler.iterateParallel(
                ExecutionContext.getContext(),
                logOutput -> logOutput.append("RegionedColumnSourceManager#estimatePushdownFilterCost"),
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, overlappingRegionsSample.size(),
                (ctx, idx, locationNec, locationResume) -> {
                    final RegionInfoHolder overlappingRegion = overlappingRegionsSample.get(idx);
                    overlappingRegion.tle.location.estimatePushdownFilterCost(filter,
                            overlappingRegion.rowSet, fullSet, usePrev, context, jobScheduler, value -> {
                                synchronized (min) {
                                    if (value < min.get()) {
                                        min.set(value);
                                    }
                                }
                                locationResume.run();
                            }, locationNec);
                },
                () -> onComplete.accept(min.get()),
                () -> SafeCloseable.closeAll(overlappingRegionsSample.iterator()),
                (e) -> {
                    try (final SafeCloseable ignored =
                            () -> SafeCloseable.closeAll(overlappingRegionsSample.iterator())) {
                        onError.accept(e);
                    }
                });
    }

    @Override
    public void pushdownFilter(
            final WhereFilter filter,
            final RowSet input,
            final RowSet fullSet,
            final boolean usePrev,
            final PushdownFilterContext context,
            final long costCeiling,
            final JobScheduler jobScheduler,
            final Consumer<PushdownResult> onComplete,
            final Consumer<Exception> onError) {
        final ArrayList<RegionInfoHolder> overlappingRegions = getOverlappingRegions(input);
        if (overlappingRegions.isEmpty()) {
            onComplete.accept(PushdownResult.of(RowSetFactory.empty(), RowSetFactory.empty()));
            return;
        }

        final RowSetBuilderRandom matchBuilder = RowSetFactory.builderRandom();
        final RowSetBuilderRandom maybeMatchBuilder = RowSetFactory.builderRandom();
        final MutableLong maybeMatchCount = new MutableLong(0);

        // Use the job scheduler to run every location in parallel.
        jobScheduler.iterateParallel(
                ExecutionContext.getContext(),
                logOutput -> logOutput.append("RegionedColumnSourceManager#pushdownFilter"),
                JobScheduler.DEFAULT_CONTEXT_FACTORY,
                0, overlappingRegions.size(),
                (ctx, idx, locationNec, locationResume) -> {
                    try (final RegionInfoHolder regionInfo = overlappingRegions.get(idx)) {
                        final int regionIndex = regionInfo.tle.regionIndex;
                        final TableLocation location = regionInfo.tle.location;
                        final WritableRowSet overlappingRowSet = regionInfo.rowSet;

                        final long locationStartKey = getFirstRowKey(regionIndex);
                        final Consumer<PushdownResult> resultConsumer = result -> {
                            try (final PushdownResult ignored = result) {
                                // Add the results to the global set.
                                if (result.match().isNonempty()) {
                                    synchronized (matchBuilder) {
                                        result.match().shiftInPlace(locationStartKey);
                                        matchBuilder.addRowSet(result.match());
                                    }
                                }
                                if (result.maybeMatch().isNonempty()) {
                                    synchronized (maybeMatchBuilder) {
                                        result.maybeMatch().shiftInPlace(locationStartKey);
                                        maybeMatchBuilder.addRowSet(result.maybeMatch());
                                        maybeMatchCount.add(result.maybeMatch().size());
                                    }
                                }
                            }
                        };
                        location.pushdownFilter(filter, overlappingRowSet, fullSet, usePrev, context, costCeiling,
                                jobScheduler, resultConsumer, onError);
                    }
                    locationResume.run();
                },
                () -> onComplete.accept(PushdownResult.of(matchBuilder.build(),
                        maybeMatchCount.get() == input.size() ? input.copy() : maybeMatchBuilder.build())),
                () -> {
                },
                onError);
    }

    /**
     * Get (or create) a map from column source to column name.
     */
    private IdentityHashMap<ColumnSource<?>, String> columnSourceToName() {
        if (columnSourceToName == null) {
            synchronized (this) {
                if (columnSourceToName == null) {
                    final IdentityHashMap<ColumnSource<?>, String> tmp = new IdentityHashMap<>(columnSources.size());
                    columnSources.forEach((name, src) -> tmp.put(src, name));
                    columnSourceToName = tmp;
                }
            }
        }
        return columnSourceToName;
    }

    public static class RegionedColumnSourcePushdownFilterContext extends BasePushdownFilterContext {
        private final Map<String, String> renameMap;

        public RegionedColumnSourcePushdownFilterContext(
                final RegionedColumnSourceManager manager,
                final WhereFilter filter,
                final List<ColumnSource<?>> columnSources) {
            final List<String> filterColumns = filter.getColumns();
            Require.eq(filterColumns.size(), "filterColumns.size()",
                    columnSources.size(), "columnSources.size()");

            final IdentityHashMap<ColumnSource<?>, String> columnSourceToName = manager.columnSourceToName();
            renameMap = new HashMap<>();
            for (int ii = 0; ii < filterColumns.size(); ii++) {
                final String filterColumnName = filterColumns.get(ii);
                final ColumnSource<?> filterSource = columnSources.get(ii);
                final String localColumnName = columnSourceToName.get(filterSource);
                if (localColumnName == null) {
                    throw new IllegalArgumentException(
                            "No associated source for '" + filterColumnName + "' found in column sources");
                }

                // Add the rename (if needed)
                if (localColumnName.equals(filterColumnName)) {
                    continue;
                }
                renameMap.put(filterColumnName, localColumnName);
            }
        }

        @Override
        public Map<String, String> renameMap() {
            return renameMap;
        }
    }

    @Override
    public PushdownFilterContext makePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> filterSources) {
        return new RegionedColumnSourcePushdownFilterContext(this, filter, filterSources);
    }
}
