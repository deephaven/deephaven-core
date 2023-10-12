/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.ColumnSourceManager;
import io.deephaven.engine.table.impl.ColumnToCodecMappings;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.dataindex.DataIndexBuilder;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manage column sources made up of regions in their own row key address space.
 */
public class RegionedColumnSourceManager implements ColumnSourceManager {

    private static final Logger log = LoggerFactory.getLogger(RegionedColumnSourceManager.class);

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

    /**
     * Table locations stored in a table.
     */
    private final QueryTable orderedLocationsTable;
    private final ObjectArraySource<TableLocation> locationSource;
    private final LongArraySource offsetSource;
    private final ObjectArraySource<RowSet> rowSetSource;
    private final String LOCATION_COLUMN_NAME = "Location";
    private final String OFFSET_COLUMN_NAME = "Offset";
    private final String ROWSET_COLUMN_NAME = "RowSet";
    private final ModifiedColumnSet locationModifiedColumnSet;
    private final ModifiedColumnSet rowSetModifiedColumnSet;

    /**
     * Whether grouping is enabled.
     */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private boolean isGroupingEnabled = true; // We always start out with grouping enabled.

    /**
     * Construct a column manager with the specified component factory and definitions.
     *
     * @param isRefreshing Whether the table using this column source manager is refreshing
     * @param componentFactory The component factory
     * @param columnDefinitions The column definitions
     */
    RegionedColumnSourceManager(final boolean isRefreshing,
            @NotNull final RegionedTableComponentFactory componentFactory,
            @NotNull final ColumnToCodecMappings codecMappings,
            @NotNull final List<ColumnDefinition<?>> columnDefinitions) {
        this.isRefreshing = isRefreshing;
        this.columnDefinitions = columnDefinitions;
        for (final ColumnDefinition<?> columnDefinition : columnDefinitions) {
            final RegionedColumnSource<?> columnSource = componentFactory.createRegionedColumnSource(columnDefinition,
                    codecMappings);
            columnSource.setColumnSourceManager(this);
            columnSources.put(
                    columnDefinition.getName(),
                    columnSource);
        }

        // Create the table that will hold the location data
        locationSource = (ObjectArraySource<TableLocation>)ArrayBackedColumnSource.getMemoryColumnSource(0, TableLocation.class);
        offsetSource = (LongArraySource)ArrayBackedColumnSource.getMemoryColumnSource(0, long.class);
        rowSetSource = (ObjectArraySource<RowSet>)ArrayBackedColumnSource.getMemoryColumnSource(0, RowSet.class);
        final Map<String, ColumnSource<?>> columnLocationMap = new LinkedHashMap<>();
        columnLocationMap.put(LOCATION_COLUMN_NAME, locationSource);
        columnLocationMap.put(OFFSET_COLUMN_NAME, offsetSource);
        columnLocationMap.put(ROWSET_COLUMN_NAME, rowSetSource);

        orderedLocationsTable = new QueryTable(RowSetFactory.empty().toTracking(), columnLocationMap);
        if (isRefreshing) {
            orderedLocationsTable.setRefreshing(true);
            locationSource.startTrackingPrevValues();
            rowSetSource.startTrackingPrevValues();
            locationModifiedColumnSet = orderedLocationsTable.newModifiedColumnSet(LOCATION_COLUMN_NAME);
            rowSetModifiedColumnSet = orderedLocationsTable.newModifiedColumnSet(ROWSET_COLUMN_NAME);
        } else {
            locationModifiedColumnSet = null;
            rowSetModifiedColumnSet = null;
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
    public boolean removeLocationKey(@NotNull final ImmutableTableLocationKey locationKey) {
        final IncludedTableLocationEntry includedLocation = includedTableLocations.remove(locationKey);
        final EmptyTableLocationEntry emptyLocation = emptyTableLocations.remove(locationKey);

        if (emptyLocation != null) {
            if (log.isDebugEnabled()) {
                log.debug().append("EMPTY_LOCATION_REMOVED:").append(locationKey.toString()).endl();
            }
        } else if (includedLocation != null) {
            includedLocation.invalidate();
            return true;
        }

        return false;
    }

    @Override
    public synchronized WritableRowSet refresh() {
        final RowSetBuilderSequential addedRowSetBuilder = RowSetFactory.builderSequential();

        final WritableRowSet added = RowSetFactory.empty();
        final WritableRowSet modified = RowSetFactory.empty();
        final TableUpdate update = new TableUpdateImpl(
                added, RowSetFactory.empty(), modified, RowSetShiftData.EMPTY, orderedLocationsTable.getModifiedColumnSetForUpdates());

        // Ordering matters, since we're using a sequential builder.
        for (final IncludedTableLocationEntry entry : orderedIncludedTableLocations) {
            if (entry.pollUpdates(addedRowSetBuilder)) {
                // Changes were detected, update the row set in the table and mark the row/column as modified.
                rowSetSource.set(entry.regionIndex, entry.location.getRowSet());
                modified.insert(entry.regionIndex);
            }
            if (modified.isNonempty()) {
                update.modifiedColumnSet().setAll(rowSetModifiedColumnSet);
            }
        }

        Collection<EmptyTableLocationEntry> entriesToInclude = null;
        for (final Iterator<EmptyTableLocationEntry> iterator = emptyTableLocations.iterator(); iterator.hasNext();) {
            final EmptyTableLocationEntry nonexistentEntry = iterator.next();
            nonexistentEntry.refresh();
            final RowSet locationRowSet = nonexistentEntry.location.getRowSet();
            if (locationRowSet != null) {
                if (locationRowSet.isEmpty()) {
                    locationRowSet.close();
                } else {
                    nonexistentEntry.initialRowSet = locationRowSet;
                    (entriesToInclude == null ? entriesToInclude = new TreeSet<>() : entriesToInclude)
                            .add(nonexistentEntry);
                    iterator.remove();
                }
            }
        }
        if (entriesToInclude != null) {
            for (final EmptyTableLocationEntry entryToInclude : entriesToInclude) {
                final IncludedTableLocationEntry entry = new IncludedTableLocationEntry(entryToInclude);
                includedTableLocations.add(entry);
                orderedIncludedTableLocations.add(entry);
                entry.processInitial(addedRowSetBuilder, entryToInclude.initialRowSet);

                // We have a new location, add the row set to the table and mark the row as added.
                locationSource.ensureCapacity(entry.regionIndex);
                locationSource.set(entry.regionIndex, entry.location);

                offsetSource.ensureCapacity(entry.regionIndex);
                offsetSource.set(entry.regionIndex, RegionedColumnSource.getFirstRowKey(entry.regionIndex));

                rowSetSource.ensureCapacity(entry.regionIndex);
                rowSetSource.set(entry.regionIndex, entryToInclude.initialRowSet);

                added.insert(entry.regionIndex);
            }
        }
        if (!isRefreshing) {
            emptyTableLocations.clear();
        } else {
            // Send the downstream updates to any listeners of the table.
            if (!update.empty()) {
                orderedLocationsTable.notifyListeners(update);
            }
        }
        return addedRowSetBuilder.build();
    }

    @Override
    public final synchronized Collection<TableLocation> allLocations() {
        return Stream.concat(
                orderedIncludedTableLocations.stream().map(e -> e.location),
                emptyTableLocations.values().stream().sorted().map(e -> e.location))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public final synchronized Collection<TableLocation> includedLocations() {
        return orderedIncludedTableLocations.stream().map(e -> e.location)
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public Table locationTable() {
        return orderedLocationsTable;
    }

    @Override
    public String locationColumnName() {
        return LOCATION_COLUMN_NAME;
    }

    @Override
    public String offsetColumnName() {
        return OFFSET_COLUMN_NAME;
    }

    @Override
    public String rowSetColumnName() {
        return ROWSET_COLUMN_NAME;
    }

    @Override
    public final synchronized boolean isEmpty() {
        return includedTableLocations.isEmpty();
    }

    @Override
    public final Map<String, ? extends ColumnSource<?>> getColumnSources() {
        return sharedColumnSources;
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

        private final int regionIndex = includedTableLocations.size();
        private final List<ColumnLocationState> columnLocationStates = new ArrayList<>();

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
            if (initialRowSet.lastRowKey() > RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK) {
                throw new TableDataException(String.format(
                        "Location %s has initial last key %#016X, larger than maximum supported key %#016X",
                        location, initialRowSet.lastRowKey(),
                        RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK));
            }

            final long regionFirstKey = RegionedColumnSource.getFirstRowKey(regionIndex);
            initialRowSet.forAllRowKeyRanges((subRegionFirstKey, subRegionLastKey) -> addedRowSetBuilder
                    .appendRange(regionFirstKey + subRegionFirstKey, regionFirstKey + subRegionLastKey));
            RowSet addRowSetInTable = null;
            try {
                for (final ColumnDefinition columnDefinition : columnDefinitions) {
                    // noinspection unchecked
                    final ColumnLocationState state = new ColumnLocationState(
                            columnDefinition,
                            columnSources.get(columnDefinition.getName()),
                            location.getColumnLocation(columnDefinition.getName()));
                    columnLocationStates.add(state);
                    state.regionAllocated(regionIndex);
                }
            } finally {
                if (addRowSetInTable != null) {
                    addRowSetInTable.close();
                }
            }
            rowSetAtLastUpdate = initialRowSet;
        }

        /** Returns {@code true} if there were changes to the row set for this location. */
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
                if (updateRowSet.lastRowKey() > RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK) {
                    throw new TableDataException(String.format(
                            "Location %s has updated last key %#016X, larger than maximum supported key %#016X",
                            location, updateRowSet.lastRowKey(),
                            RegionedColumnSource.ROW_KEY_TO_SUB_REGION_ROW_INDEX_MASK));
                }

                if (log.isDebugEnabled()) {
                    log.debug().append("LOCATION_SIZE_CHANGE:").append(location.toString())
                            .append(",FROM:").append(rowSetAtLastUpdate.size())
                            .append(",TO:").append(updateRowSet.size()).endl();
                }
                try (final RowSet addedRowSet = updateRowSet.minus(rowSetAtLastUpdate)) {
                    final long regionFirstKey = RegionedColumnSource.getFirstRowKey(regionIndex);
                    addedRowSet.forAllRowKeyRanges((subRegionFirstKey, subRegionLastKey) -> addedRowSetBuilder
                            .appendRange(regionFirstKey + subRegionFirstKey, regionFirstKey + subRegionLastKey));
                }
            } finally {
                rowSetAtLastUpdate.close();
                rowSetAtLastUpdate = updateRowSet;
                // There was a change to the row set.
                return true;
            }
        }

        private void invalidate() {
            columnLocationStates.forEach(cls -> cls.source.invalidateRegion(regionIndex));
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
    }

    private static final KeyedObjectKey<ImmutableTableLocationKey, IncludedTableLocationEntry> INCLUDED_TABLE_LOCATION_ENTRY_KEY =
            new KeyedObjectKey.Basic<>() {

                @Override
                public ImmutableTableLocationKey getKey(
                        @NotNull final IncludedTableLocationEntry includedTableLocationEntry) {
                    return includedTableLocationEntry.location.getKey();
                }
            };

    /**
     * Batches up a definition, source, and location for ease of use. Implements grouping maintenance.
     */
    private class ColumnLocationState<T> {

        protected final ColumnDefinition<T> definition;
        protected final RegionedColumnSource<T> source;
        protected final ColumnLocation location;

        private ColumnLocationState(ColumnDefinition<T> definition,
                RegionedColumnSource<T> source,
                ColumnLocation location) {
            this.definition = definition;
            this.source = source;
            this.location = location;
        }

        private void regionAllocated(final int regionIndex) {
            Assert.eq(regionIndex, "regionIndex", source.addRegion(definition, location),
                    "source.addRegion((definition, location)");
        }
    }
}
