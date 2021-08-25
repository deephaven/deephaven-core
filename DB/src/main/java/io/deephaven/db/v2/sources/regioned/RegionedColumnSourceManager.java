/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources.regioned;

import io.deephaven.db.v2.ColumnToCodecMappings;
import io.deephaven.db.v2.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.db.v2.utils.ReadOnlyIndex;
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
    private final Map<String, ? extends DeferredGroupingColumnSource<?>> sharedColumnSources =
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
    public synchronized Index refresh() {
        final Index.SequentialBuilder addedIndexBuilder = Index.FACTORY.getSequentialBuilder();
        for (final IncludedTableLocationEntry entry : orderedIncludedTableLocations) { // Ordering matters, since we're
                                                                                       // using a sequential builder.
            entry.pollUpdates(addedIndexBuilder);
        }
        Collection<EmptyTableLocationEntry> entriesToInclude = null;
        for (final Iterator<EmptyTableLocationEntry> iterator = emptyTableLocations.iterator(); iterator.hasNext();) {
            final EmptyTableLocationEntry nonexistentEntry = iterator.next();
            nonexistentEntry.refresh();
            final ReadOnlyIndex locationIndex = nonexistentEntry.location.getIndex();
            if (locationIndex != null) {
                if (locationIndex.empty()) {
                    locationIndex.close();
                } else {
                    nonexistentEntry.initialIndex = locationIndex;
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
                entry.processInitial(addedIndexBuilder, entryToInclude.initialIndex);
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
                emptyTableLocations.values().stream().sorted().map(e -> e.location))
                .collect(Collectors.toCollection(ArrayList::new));
    }

    @Override
    public final synchronized Collection<TableLocation> includedLocations() {
        return orderedIncludedTableLocations.stream().map(e -> e.location)
                .collect(Collectors.toCollection(ArrayList::new));
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
     * State keeper for a table location and its subscription buffer if it hasn't been found to have a non-null,
     * non-zero size yet.
     */
    private class EmptyTableLocationEntry implements Comparable<EmptyTableLocationEntry> {

        private final TableLocation location;
        private final TableLocationUpdateSubscriptionBuffer subscriptionBuffer;

        private ReadOnlyIndex initialIndex;

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
            new KeyedObjectKey.Basic<ImmutableTableLocationKey, EmptyTableLocationEntry>() {

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
         * Index in the region's space, not the table's space.
         */
        private ReadOnlyIndex indexAtLastUpdate;

        private IncludedTableLocationEntry(final EmptyTableLocationEntry nonexistentEntry) {
            this.location = nonexistentEntry.location;
            this.subscriptionBuffer = nonexistentEntry.subscriptionBuffer;
        }

        private void processInitial(final Index.SequentialBuilder addedIndexBuilder, final ReadOnlyIndex initialIndex) {
            Assert.neqNull(initialIndex, "initialIndex");
            Assert.eqTrue(initialIndex.nonempty(), "initialIndex.nonempty()");
            Assert.eqNull(indexAtLastUpdate, "indexAtLastUpdate");
            if (initialIndex.lastKey() > RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK) {
                throw new TableDataException(String.format(
                        "Location %s has initial last key %#016X, larger than maximum supported key %#016X",
                        location, initialIndex.lastKey(),
                        RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK));
            }

            final long regionFirstKey = RegionedColumnSource.getFirstElementIndex(regionIndex);
            initialIndex.forAllLongRanges((subRegionFirstKey, subRegionLastKey) -> addedIndexBuilder
                    .appendRange(regionFirstKey + subRegionFirstKey, regionFirstKey + subRegionLastKey));
            ReadOnlyIndex addIndexInTable = null;
            try {
                for (final ColumnDefinition columnDefinition : columnDefinitions) {
                    // noinspection unchecked
                    final ColumnLocationState state = new ColumnLocationState(
                            columnDefinition,
                            columnSources.get(columnDefinition.getName()),
                            location.getColumnLocation(columnDefinition.getName()));
                    columnLocationStates.add(state);
                    state.regionAllocated(regionIndex);
                    if (state.needToUpdateGrouping()) {
                        state.updateGrouping(
                                addIndexInTable == null ? addIndexInTable = initialIndex.shift(regionFirstKey)
                                        : addIndexInTable);
                    }
                }
            } finally {
                if (addIndexInTable != null) {
                    addIndexInTable.close();
                }
            }
            indexAtLastUpdate = initialIndex;
        }

        private void pollUpdates(final Index.SequentialBuilder addedIndexBuilder) {
            Assert.neqNull(subscriptionBuffer, "subscriptionBuffer"); // Effectively, this is asserting "isRefreshing".
            if (!subscriptionBuffer.processPending()) {
                return;
            }
            final ReadOnlyIndex updateIndex = location.getIndex();
            try {
                if (updateIndex == null) {
                    // This should be impossible - the subscription buffer transforms a transition to null into a
                    // pending exception
                    throw new TableDataException(
                            "Location " + location + " is no longer available, data has been removed");
                }
                if (!indexAtLastUpdate.subsetOf(updateIndex)) { // Bad change
                    // noinspection ThrowableNotThrown
                    Assert.statementNeverExecuted(
                            "Index keys removed at location " + location + ": " + indexAtLastUpdate.minus(updateIndex));
                }
                if (indexAtLastUpdate.size() == updateIndex.size()) {
                    // Nothing to do
                    return;
                }
                if (updateIndex.lastKey() > RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK) {
                    throw new TableDataException(String.format(
                            "Location %s has updated last key %#016X, larger than maximum supported key %#016X",
                            location, updateIndex.lastKey(),
                            RegionedColumnSource.ELEMENT_INDEX_TO_SUB_REGION_ELEMENT_INDEX_MASK));
                }

                if (log.isDebugEnabled()) {
                    log.debug().append("LOCATION_SIZE_CHANGE:").append(location.toString())
                            .append(",FROM:").append(indexAtLastUpdate.size())
                            .append(",TO:").append(updateIndex.size()).endl();
                }
                try (final ReadOnlyIndex addedIndex = updateIndex.minus(indexAtLastUpdate)) {
                    final long regionFirstKey = RegionedColumnSource.getFirstElementIndex(regionIndex);
                    addedIndex.forAllLongRanges((subRegionFirstKey, subRegionLastKey) -> addedIndexBuilder
                            .appendRange(regionFirstKey + subRegionFirstKey, regionFirstKey + subRegionLastKey));
                    ReadOnlyIndex addIndexInTable = null;
                    try {
                        for (final ColumnLocationState state : columnLocationStates) {
                            if (state.needToUpdateGrouping()) {
                                state.updateGrouping(
                                        addIndexInTable == null ? addIndexInTable = updateIndex.shift(regionFirstKey)
                                                : addIndexInTable);
                            }
                        }
                    } finally {
                        if (addIndexInTable != null) {
                            addIndexInTable.close();
                        }
                    }
                }
            } finally {
                indexAtLastUpdate.close();
                indexAtLastUpdate = updateIndex;
            }
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
            new KeyedObjectKey.Basic<ImmutableTableLocationKey, IncludedTableLocationEntry>() {

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

        private boolean needToUpdateGrouping() {
            return (definition.isGrouping() && isGroupingEnabled) || definition.isPartitioning();
        }

        /**
         * Update column groupings, if appropriate.
         *
         * @param locationAddedIndexInTable The added index, in the table's address space
         */
        private void updateGrouping(@NotNull final ReadOnlyIndex locationAddedIndexInTable) {
            if (definition.isGrouping()) {
                Assert.eqTrue(isGroupingEnabled, "isGroupingEnabled");
                GroupingProvider groupingProvider = source.getGroupingProvider();
                if (groupingProvider == null) {
                    groupingProvider = GroupingProvider.makeGroupingProvider(definition);
                    // noinspection unchecked
                    source.setGroupingProvider(groupingProvider);
                }
                if (groupingProvider instanceof KeyRangeGroupingProvider) {
                    ((KeyRangeGroupingProvider) groupingProvider).addSource(location, locationAddedIndexInTable);
                }
            } else if (definition.isPartitioning()) {
                final DeferredGroupingColumnSource<T> partitioningColumnSource = source;
                Map<T, Index> columnPartitionToIndex = partitioningColumnSource.getGroupToRange();
                if (columnPartitionToIndex == null) {
                    columnPartitionToIndex = new LinkedHashMap<>();
                    partitioningColumnSource.setGroupToRange(columnPartitionToIndex);
                }
                final T columnPartitionValue =
                        location.getTableLocation().getKey().getPartitionValue(definition.getName());
                final Index current = columnPartitionToIndex.get(columnPartitionValue);
                if (current == null) {
                    columnPartitionToIndex.put(columnPartitionValue, locationAddedIndexInTable.clone());
                } else {
                    current.insert(locationAddedIndexInTable);
                }
            }
        }
    }
}
