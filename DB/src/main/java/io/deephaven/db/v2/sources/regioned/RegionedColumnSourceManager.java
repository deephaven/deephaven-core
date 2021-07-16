/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.ColumnToCodecMappings;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.v2.ColumnSourceManager;
import io.deephaven.db.v2.locations.*;
import io.deephaven.db.v2.sources.DeferredGroupingColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.internal.log.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Manage column sources made up of regions in their own index address space.
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
    private final ColumnDefinition[] columnDefinitions;

    /**
     * The column sources that make up this table.
     */
    private final Map<String, RegionedColumnSource<?>> columnSources = new LinkedHashMap<>();

    /**
     * An unmodifiable view of columnSources.
     */
    private final Map<String, ? extends DeferredGroupingColumnSource<?>> sharedColumnSources = Collections.unmodifiableMap(columnSources);

    /**
     * State for table locations that have been added, but have never been found to exist with non-zero size.
     */
    private final KeyedObjectHashMap<TableLocationKey, EmptyTableLocationEntry> emptyTableLocations = new KeyedObjectHashMap<>(EMPTY_TABLE_LOCATION_ENTRY_KEY);

    /**
     * State for table locations that provide the regions backing our column sources.
     */
    private final KeyedObjectHashMap<TableLocationKey, IncludedTableLocationEntry> includedTableLocations = new KeyedObjectHashMap<>(INCLUDED_TABLE_LOCATION_ENTRY_KEY);

    /**
     * Table locations that provide the regions backing our column sources, in insertion order.
     */
    private final List<IncludedTableLocationEntry> orderedIncludedTableLocations = new ArrayList<>();

    /**
     * Whether grouping is enabled.
     */
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private boolean isGroupingEnabled = true; // We always start out with grouping enabled.

    /**
     * Construct a column manager with the specified component factory and definitions.
     *
     * @param isRefreshing      Whether the table using this column source manager is refreshing
     * @param componentFactory  The component factory
     * @param columnDefinitions The column definitions
     */
    RegionedColumnSourceManager(final boolean isRefreshing,
                                @NotNull final RegionedTableComponentFactory componentFactory,
                                @NotNull final ColumnToCodecMappings codecMappings,
                                @NotNull final ColumnDefinition... columnDefinitions) {
        this.isRefreshing = isRefreshing;
        this.columnDefinitions = columnDefinitions;
        for (final ColumnDefinition<?> columnDefinition : columnDefinitions) {
            columnSources.put(
                    columnDefinition.getName(),
                    componentFactory.createRegionedColumnSource(columnDefinition, codecMappings));
        }
    }

    @Override
    public synchronized void addLocation(@NotNull final TableLocation tableLocation) {
        final IncludedTableLocationEntry includedLocation = includedTableLocations.get(tableLocation);
        final EmptyTableLocationEntry emptyLocation = emptyTableLocations.get(tableLocation);

        if (includedLocation == null && emptyLocation == null) {
            if (log.isDebugEnabled()) {
                log.debug().append("LOCATION_ADDED:").append(tableLocation.toString()).endl();
            }
            emptyTableLocations.put(tableLocation, new EmptyTableLocationEntry(tableLocation));
        } else {
            // Duplicate location - not allowed
            final TableLocation duplicateLocation = includedLocation != null ? includedLocation.location : emptyLocation.location;
            if (tableLocation != duplicateLocation) {
                // If it ever transpires that we need to compare the locations and not just detect a second add, then
                // we need to add plumbing to include access to the location provider
                throw new TableDataException("Data Routing Configuration error: TableDataService elements overlap at locations " +
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
    public synchronized Index refresh() {
        final Index.SequentialBuilder addedIndexBuilder = Index.FACTORY.getSequentialBuilder();
        for (final IncludedTableLocationEntry entry : orderedIncludedTableLocations) { // Ordering matters, since we're using a sequential builder.
            entry.pollSizeUpdates(addedIndexBuilder);
        }
        Collection<EmptyTableLocationEntry> entriesToInclude = null;
        for (final Iterator<EmptyTableLocationEntry> iterator = emptyTableLocations.iterator(); iterator.hasNext(); ) {
            final EmptyTableLocationEntry nonexistentEntry = iterator.next();
            nonexistentEntry.refresh();
            final long size = nonexistentEntry.location.getSize();
            //noinspection ConditionCoveredByFurtherCondition
            if (size != TableLocationState.NULL_SIZE && size > 0) {
                (entriesToInclude == null ? entriesToInclude = new TreeSet<>() : entriesToInclude).add(nonexistentEntry);
                iterator.remove();
            }
        }
        if (entriesToInclude != null) {
            for (final EmptyTableLocationEntry entryToInclude : entriesToInclude) {
                final IncludedTableLocationEntry entry = new IncludedTableLocationEntry(entryToInclude);
                includedTableLocations.add(entry);
                orderedIncludedTableLocations.add(entry);
                entry.processInitialSize(addedIndexBuilder, entryToInclude.location.getSize());
            }
        }
        if (!isRefreshing) {
            emptyTableLocations.clear();
        }
        return addedIndexBuilder.getIndex();
    }

    @Override
    public final synchronized Collection<TableLocation> allLocations() {
        return Stream.concat(
                orderedIncludedTableLocations.stream().map(e -> e.location),
                emptyTableLocations.values().stream().sorted().map(e -> e.location)
        ).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public final synchronized Collection<TableLocation> includedLocations() {
        return orderedIncludedTableLocations.stream().map(e -> e.location).collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public final synchronized boolean isEmpty() {
        return includedTableLocations.isEmpty();
    }

    @Override
    public final Map<String, ? extends DeferredGroupingColumnSource<?>> getColumnSources() {
        return sharedColumnSources;
    }

    @Override
    public final synchronized void disableGrouping() {
        if (!isGroupingEnabled) {
            return;
        }
        isGroupingEnabled = false;
        for (ColumnDefinition<?> columnDefinition : columnDefinitions) {
            if (columnDefinition.isGrouping()) {
                DeferredGroupingColumnSource<?> columnSource = getColumnSources().get(columnDefinition.getName());
                columnSource.setGroupingProvider(null);
                columnSource.setGroupToRange(null);
            }
        }
    }

    /**
     * State keeper for a table location and its subscription buffer if it hasn't been found to have a non-null, non-zero size yet.
     */
    private class EmptyTableLocationEntry implements Comparable<EmptyTableLocationEntry> {

        private final TableLocation location;
        private final TableLocationUpdateSubscriptionBuffer subscriptionBuffer;

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
                //     (i.e. "live") tables, regardless of whether the underlying locations support subscriptions.
                location.refresh();
            }
        }

        @Override
        public int compareTo(@NotNull final EmptyTableLocationEntry other) {
            if (this == other) {
                return 0;
            }
            return TableLocationKey.COMPARATOR.compare(location, other.location);
        }
    }

    private static final KeyedObjectKey<TableLocationKey, EmptyTableLocationEntry> EMPTY_TABLE_LOCATION_ENTRY_KEY = new TableLocationKey.KeyedObjectKeyImpl<EmptyTableLocationEntry>() {

        @Override
        public TableLocationKey getKey(@NotNull final EmptyTableLocationEntry emptyTableLocationEntry) {
            return emptyTableLocationEntry.location;
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

        private long sizeAtLastUpdate = 0;

        private IncludedTableLocationEntry(final EmptyTableLocationEntry nonexistentEntry) {
            this.location = nonexistentEntry.location;
            this.subscriptionBuffer = nonexistentEntry.subscriptionBuffer;
        }

        private void processInitialSize(final Index.SequentialBuilder addedIndexBuilder, final long size) {
            Assert.neq(size, "size", TableLocationState.NULL_SIZE);
            Assert.eqZero(sizeAtLastUpdate, "sizeAtLastUpdate");
            if (size > RegionedColumnSource.REGION_CAPACITY_IN_ELEMENTS) {
                throw new TableDataException("Location " + location + " has initial size " + size
                        + ", larger than maximum supported location size " + RegionedColumnSource.REGION_CAPACITY_IN_ELEMENTS);
            }

            final long firstKeyAdded = RegionedPageStore.getFirstElementIndex(regionIndex);
            final long lastKeyAdded = firstKeyAdded + size - 1;
            addedIndexBuilder.appendRange(firstKeyAdded, lastKeyAdded);
            for (final ColumnDefinition columnDefinition : columnDefinitions) {
                final ColumnLocationState state = new ColumnLocationState(
                        columnDefinition,
                        columnSources.get(columnDefinition.getName()),
                        location.getColumnLocation(columnDefinition.getName()));
                columnLocationStates.add(state);
                state.regionAllocated(regionIndex);
                state.regionExtended(firstKeyAdded, lastKeyAdded);
            }
            sizeAtLastUpdate = size;
        }

        private void pollSizeUpdates(final Index.SequentialBuilder addedIndexBuilder) {
            Assert.neqNull(subscriptionBuffer, "subscriptionBuffer"); // Effectively, this is asserting "isRefreshing".
            if (!subscriptionBuffer.processPending()) {
                return;
            }
            final long size = location.getSize();
            if (size == TableLocationState.NULL_SIZE) {
                // This should be impossible - the subscription buffer transforms a transition to NULL_SIZE into a pending exception
                throw new TableDataException("Location " + location + " is no longer available, data has been removed");
            }
            if (size < sizeAtLastUpdate) { // Bad change
                throw new IllegalStateException("Size decreased for location " + location + ": was " + sizeAtLastUpdate + ", now " + size);
            }
            if (size == sizeAtLastUpdate) {
                // Nothing to do
                return;
            }
            if (size > RegionedColumnSource.REGION_CAPACITY_IN_ELEMENTS) {
                throw new TableDataException("Location " + location + " has updated size " + size
                        + ", larger than maximum supported location size " + RegionedColumnSource.REGION_CAPACITY_IN_ELEMENTS);
            }

            if (log.isDebugEnabled()) {
                log.debug().append("LOCATION_SIZE_CHANGE:").append(location.toString()).append(",FROM:").append(sizeAtLastUpdate).append(",TO:").append(size).endl();
            }
            final long firstKeyAdded = RegionedPageStore.getFirstElementIndex(regionIndex) + sizeAtLastUpdate;
            final long lastKeyAdded = firstKeyAdded + size - sizeAtLastUpdate - 1;
            addedIndexBuilder.appendRange(firstKeyAdded, lastKeyAdded);
            for (final ColumnLocationState state : columnLocationStates) {
                state.regionExtended(firstKeyAdded, lastKeyAdded);
            }
            sizeAtLastUpdate = size;
        }

        @Override
        public int compareTo(@NotNull final IncludedTableLocationEntry other) {
            // This Comparable implementation is currently unused, as we maintain ordering in orderedIncludedTableLocations by insertion
            if (this == other) {
                return 0;
            }
            return Integer.compare(regionIndex, other.regionIndex);
        }
    }

    private static final KeyedObjectKey<TableLocationKey, IncludedTableLocationEntry> INCLUDED_TABLE_LOCATION_ENTRY_KEY = new TableLocationKey.KeyedObjectKeyImpl<IncludedTableLocationEntry>() {

        @Override
        public TableLocationKey getKey(@NotNull final IncludedTableLocationEntry includedTableLocationEntry) {
            return includedTableLocationEntry.location;
        }
    };

    /**
     * Batches up a definition, source, and location for ease of use.  Implements grouping maintenance.
     */
    private class ColumnLocationState {

        protected final ColumnDefinition<?> definition;
        protected final RegionedColumnSource<?> source;
        protected final ColumnLocation location;

        private ColumnLocationState(ColumnDefinition<?> definition,
                                    RegionedColumnSource<?> source,
                                    ColumnLocation location) {
            this.definition = definition;
            this.source = source;
            this.location = location;
        }

        private void regionAllocated(final int regionIndex) {
            Assert.eq(regionIndex, "regionIndex", source.addRegion(definition, location), "source.addRegion((definition, location)");
        }

        private void regionExtended(final long firstKeyAdded, final long lastKeyAdded) {
            updateGrouping(firstKeyAdded, lastKeyAdded);
        }

        /**
         * Update column groupings, if appropriate.
         *
         * @param firstKeyAdded The first key added
         * @param lastKeyAdded  The last key added
         */
        private void updateGrouping(final long firstKeyAdded, final long lastKeyAdded) {
            if (definition.isGrouping()) {
                if (!isGroupingEnabled) {
                    return;
                }
                GroupingProvider groupingProvider = source.getGroupingProvider();
                if (groupingProvider == null) {
                    groupingProvider = GroupingProvider.makeGroupingProvider(definition);
                    //noinspection unchecked
                    source.setGroupingProvider(groupingProvider);
                }
                if (groupingProvider instanceof KeyRangeGroupingProvider) {
                    ((KeyRangeGroupingProvider) groupingProvider).addSource(location, firstKeyAdded, lastKeyAdded);
                }
            } else if (definition.isPartitioning()) {
                //noinspection unchecked
                final DeferredGroupingColumnSource<String> partitioningColumnSource = (DeferredGroupingColumnSource<String>) source;
                Map<String, Index> columnPartitionToIndex = partitioningColumnSource.getGroupToRange();
                if (columnPartitionToIndex == null) {
                    columnPartitionToIndex = new LinkedHashMap<>();
                    partitioningColumnSource.setGroupToRange(columnPartitionToIndex);
                }
                final Index added = Index.FACTORY.getIndexByRange(firstKeyAdded, lastKeyAdded);
                final String columnPartition = location.getTableLocation().getColumnPartition().toString();
                final Index current = columnPartitionToIndex.get(columnPartition);
                if (current == null) {
                    columnPartitionToIndex.put(columnPartition, added);
                } else {
                    current.insert(added);
                }
            }
        }
    }
}
