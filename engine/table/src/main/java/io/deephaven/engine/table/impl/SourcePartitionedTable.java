//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.api.filter.Filter;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.*;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.SingleTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.table.iterators.ChunkedObjectColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link PartitionedTable} of single-location {@link SourceTable}s keyed by {@link TableLocationKey}. Refer to
 * {@link TableLocationKey} for an explanation of partitioning.
 */
public class SourcePartitionedTable extends PartitionedTableImpl {

    private static final String STATE_COLUMN_NAME = "LocationState";
    private static final String EXISTS_COLUMN_NAME = "LocationExists";
    private static final String KEY_COLUMN_NAME = "TableLocationKey";
    private static final String CONSTITUENT_COLUMN_NAME = "LocationTable";

    /**
     * Construct a {@link SourcePartitionedTable} from the supplied parameters.
     * <p>
     * Note that refreshLocations and refreshSizes are distinct because there are use cases that supply an external
     * RowSet and hence don't require size refreshes. Others might care for size refreshes, but only the
     * initially-available set of locations.
     *
     * @param constituentDefinition The {@link TableDefinition} expected of constituent {@link Table tables}
     * @param applyTablePermissions Function to apply in order to correctly restrict the visible result rows
     * @param tableLocationProvider Source for table locations
     * @param refreshLocations Whether the set of locations should be refreshed
     * @param refreshSizes Whether the locations found should be refreshed
     * @param locationKeyMatcher Function to filter desired location keys
     */
    @Deprecated(forRemoval = true)
    public SourcePartitionedTable(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final UnaryOperator<Table> applyTablePermissions,
            @NotNull final TableLocationProvider tableLocationProvider,
            final boolean refreshLocations,
            final boolean refreshSizes,
            @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher) {
        this(constituentDefinition, applyTablePermissions, tableLocationProvider, refreshLocations, refreshSizes,
                locationKeyMatcher, true);
    }

    /**
     * Construct a {@link SourcePartitionedTable} from the supplied parameters.
     * <p>
     * Note that refreshLocations and refreshSizes are distinct because there are use cases that supply an external
     * RowSet and hence don't require size refreshes. Others might care for size refreshes, but only the
     * initially-available set of locations.
     *
     * @param constituentDefinition The {@link TableDefinition} expected of constituent {@link Table tables}
     * @param applyTablePermissions Function to apply in order to correctly restrict the visible result rows
     * @param tableLocationProvider Source for table locations
     * @param refreshLocations Whether the set of locations should be refreshed
     * @param refreshSizes Whether the locations found should be refreshed
     * @param locationKeyMatcher Function to filter desired location keys
     * @param preCheckExistence Whether to pre-check the existence (non-null, non-zero size) of locations before
     *        including them in the result SourcePartitionedTable as constituents. It is recommended to set this to
     *        {@code false} if you will do subsequent filtering on the result, or if you are confident that all
     *        locations are valid.
     */
    public SourcePartitionedTable(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final UnaryOperator<Table> applyTablePermissions,
            @NotNull final TableLocationProvider tableLocationProvider,
            final boolean refreshLocations,
            final boolean refreshSizes,
            @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher,
            final boolean preCheckExistence) {
        super(new UnderlyingTableMaintainer(
                constituentDefinition,
                applyTablePermissions,
                tableLocationProvider,
                refreshLocations,
                refreshSizes,
                locationKeyMatcher,
                preCheckExistence).activateAndGetResult(),
                Set.of(KEY_COLUMN_NAME),
                true,
                CONSTITUENT_COLUMN_NAME,
                constituentDefinition,
                refreshLocations,
                false);
    }

    private static class UnderlyingTableMaintainer
            extends ReferenceCountedLivenessNode
            implements NotificationQueue.Dependency {

        private final TableDefinition constituentDefinition;
        private final UnaryOperator<Table> applyTablePermissions;
        private final TableLocationProvider tableLocationProvider;
        private final boolean refreshSizes;
        private final Predicate<ImmutableTableLocationKey> locationKeyMatcher;

        private final TrackingWritableRowSet resultRows;
        private final String[] partitioningColumnNames;
        private final WritableColumnSource<?>[] resultPartitionValues;
        private final WritableColumnSource<LocationState> resultLocationStates;
        private final QueryTable result;

        private final UpdateSourceCombiner refreshCombiner;
        private final TableLocationSubscriptionBuffer subscriptionBuffer;
        @SuppressWarnings("FieldCanBeLocal") // We need to hold onto this reference for reachability purposes.
        private final Runnable processNewLocationsUpdateRoot;

        private final UpdateCommitter<UnderlyingTableMaintainer> removedLocationsCommitter;
        private List<Table> removedConstituents = null;

        private UnderlyingTableMaintainer(
                @NotNull final TableDefinition constituentDefinition,
                @NotNull final UnaryOperator<Table> applyTablePermissions,
                @NotNull final TableLocationProvider tableLocationProvider,
                final boolean refreshLocations,
                final boolean refreshSizes,
                @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher,
                final boolean preCheckExistence) {
            super(false);

            this.constituentDefinition = constituentDefinition;
            this.applyTablePermissions = applyTablePermissions;
            this.tableLocationProvider = tableLocationProvider;
            this.refreshSizes = refreshSizes;
            this.locationKeyMatcher = locationKeyMatcher;

            resultRows = RowSetFactory.empty().toTracking();
            final List<ColumnDefinition<?>> partitioningColumns = constituentDefinition.getPartitioningColumns();
            partitioningColumnNames = partitioningColumns.stream()
                    .map(ColumnDefinition::getName)
                    .toArray(String[]::new);
            resultPartitionValues = partitioningColumns.stream()
                    .map(cd -> ArrayBackedColumnSource.getMemoryColumnSource(cd.getDataType(), cd.getComponentType()))
                    .toArray(WritableColumnSource[]::new);
            resultLocationStates = ArrayBackedColumnSource.getMemoryColumnSource(LocationState.class, null);

            final List<Selectable> resultColumns = new ArrayList<>(partitioningColumns.size() + 2);
            final Map<String, ColumnSource<?>> rawResultSources = new LinkedHashMap<>(partitioningColumns.size() + 2);
            for (int pci = 0; pci < partitioningColumns.size(); ++pci) {
                resultColumns.add(new SourceColumn(partitioningColumnNames[pci]));
                rawResultSources.put(partitioningColumnNames[pci], resultPartitionValues[pci]);
            }
            resultColumns.add(new FunctionalColumn<>(
                    STATE_COLUMN_NAME, LocationState.class,
                    KEY_COLUMN_NAME, TableLocationKey.class, null,
                    LocationState::key));
            resultColumns.add(new FunctionalColumn<>(
                    STATE_COLUMN_NAME, LocationState.class,
                    CONSTITUENT_COLUMN_NAME, Table.class, null,
                    LocationState::table));
            rawResultSources.put(STATE_COLUMN_NAME, resultLocationStates);

            final QueryTable rawResult = new QueryTable(resultRows, rawResultSources);

            final boolean needToRefreshLocations = refreshLocations && tableLocationProvider.supportsSubscriptions();
            if (needToRefreshLocations || refreshSizes) {
                rawResult.setRefreshing(true);
                refreshCombiner = new UpdateSourceCombiner(rawResult.getUpdateGraph());
                rawResult.addParentReference(this);
                manage(refreshCombiner);
            } else {
                refreshCombiner = null;
            }

            if (needToRefreshLocations) {
                Arrays.stream(resultPartitionValues).forEach(ColumnSource::startTrackingPrevValues);
                resultLocationStates.startTrackingPrevValues();

                subscriptionBuffer = new TableLocationSubscriptionBuffer(tableLocationProvider);
                manage(subscriptionBuffer);

                processNewLocationsUpdateRoot = new InstrumentedTableUpdateSource(
                        rawResult,
                        SourcePartitionedTable.class.getSimpleName() + '[' + tableLocationProvider + ']'
                                + "-processBufferedLocationChanges") {
                    @Override
                    protected void instrumentedRefresh() {
                        processBufferedLocationChanges(true);
                    }
                };
                refreshCombiner.addSource(processNewLocationsUpdateRoot);

                this.removedLocationsCommitter = new UpdateCommitter<>(
                        this,
                        rawResult.getUpdateGraph(),
                        ignored -> {
                            Assert.neqNull(removedConstituents, "removedConstituents");
                            result.unmanage(removedConstituents.stream());
                            removedConstituents = null;
                        });
                processBufferedLocationChanges(false);
            } else {
                subscriptionBuffer = null;
                processNewLocationsUpdateRoot = null;
                removedLocationsCommitter = null;
                tableLocationProvider.refresh();

                final Collection<TableLocation> locations = new ArrayList<>();
                try {
                    retainReference();
                    tableLocationProvider.getTableLocationKeys(
                            (final LiveSupplier<ImmutableTableLocationKey> lstlk) -> {
                                final TableLocation tableLocation = tableLocationProvider.getTableLocation(lstlk.get());
                                manage(tableLocation);
                                locations.add(tableLocation);
                            },
                            locationKeyMatcher);
                    try (final RowSet added = sortAndAddLocations(locations.stream())) {
                        resultRows.insert(added);
                    }
                } finally {
                    dropReference();
                }
            }

            final Table filteredResult;
            if (preCheckExistence) {
                final List<Selectable> existenceColumns = List.of(new FunctionalColumn<>(
                        STATE_COLUMN_NAME, LocationState.class,
                        EXISTS_COLUMN_NAME, Boolean.class, null,
                        LocationState::exists));
                final Filter existsFilter = new MatchFilter(MatchFilter.MatchType.Regular, EXISTS_COLUMN_NAME, true);
                filteredResult = rawResult.updateView(existenceColumns).where(existsFilter);
            } else {
                filteredResult = rawResult;
            }
            result = (QueryTable) filteredResult.view(resultColumns);
        }

        private QueryTable activateAndGetResult() {
            if (refreshCombiner != null) {
                refreshCombiner.install();
            }
            return result;
        }

        private RowSet sortAndAddLocations(@NotNull final Stream<ImmutableTableLocationKey> locationKeys) {
            final long initialLastRowKey = resultRows.lastRowKey();
            final MutableLong lastInsertedRowKey = new MutableLong(initialLastRowKey);
            // Note that makeConstituentTable expects us to subsequently unmanage the TableLocations
            unmanage(locations.sorted(Comparator.comparing(TableLocation::getKey)).peek(tl -> {
                final long constituentRowKey = lastInsertedRowKey.incrementAndGet();

                for (int pci = 0; pci < resultPartitionValues.length; ++pci) {
                    addPartitionValue(tl.getKey(), partitioningColumnNames[pci], resultPartitionValues[pci],
                            constituentRowKey);
                }

                resultLocationStates.ensureCapacity(constituentRowKey + 1);
                resultLocationStates.set(constituentRowKey, tl.getKey());

                resultLocationTables.ensureCapacity(constituentRowKey + 1);
                final Table constituentTable = makeConstituentTable(tl);
                resultLocationTables.set(constituentRowKey, constituentTable);

                if (result.isRefreshing()) {
                    result.manage(constituentTable);
                }
            }));
            return initialLastRowKey == lastInsertedRowKey.get()
                    ? RowSetFactory.empty()
                    : RowSetFactory.fromRange(initialLastRowKey + 1, lastInsertedRowKey.get());
        }

        private static <T> void addPartitionValue(
                @NotNull final TableLocationKey tableLocationKey,
                @NotNull final String partitioningColumnName,
                @NotNull final WritableColumnSource<T> partitionValueColumn,
                final long rowKey) {
            partitionValueColumn.ensureCapacity(rowKey + 1);
            partitionValueColumn.set(rowKey, tableLocationKey.getPartitionValue(partitioningColumnName));
        }

        private Table makeConstituentTable(@NotNull final TableLocation tableLocation) {
            final PartitionAwareSourceTable constituent = new PartitionAwareSourceTable(
                    constituentDefinition,
                    "SingleLocationSourceTable-" + tableLocation,
                    RegionedTableComponentFactoryImpl.INSTANCE,
                    new SingleTableLocationProvider(tableLocation, refreshSizes
                            ? tableLocationProvider.getLocationUpdateMode()
                            : TableUpdateMode.STATIC),
                    refreshSizes ? refreshCombiner : null);

            // Transfer management to the constituent CSM. NOTE: this is likely to end up double-managed
            // after the CSM adds the location to the table, but that's acceptable.
            constituent.columnSourceManager.manage(tableLocation);
            // Note that the caller is now responsible for unmanaging tableLocation on behalf of this.

            // Be careful to propagate the systemic attribute properly to child tables
            constituent.setAttribute(Table.SYSTEMIC_TABLE_ATTRIBUTE, result.isSystemicObject());
            return applyTablePermissions.apply(constituent);
        }

        private void processBufferedLocationChanges(final boolean notifyListeners) {
            final RowSet removed;
            final RowSet added;

            try (final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate =
                         subscriptionBuffer.processPending()) {
                if (locationUpdate == null) {
                    removed = null;
                } else {
                    removed = processRemovals(locationUpdate);
                    processAdditions(locationUpdate);
                }
                checkPendingLocations();
                added = addReadyLocations();
            }

            if (removed == null) {
                if (added == null) {
                    return;
                }
                resultRows.insert(added);
            } else if (added == null) {
                resultRows.remove(removed);
            } else {
                resultRows.update(added, removed);
            }
            if (notifyListeners) {
                result.notifyListeners(new TableUpdateImpl(
                        added == null ? RowSetFactory.empty() : added,
                        removed == null ? RowSetFactory.empty() : removed,
                        RowSetFactory.empty(),
                        RowSetShiftData.EMPTY,
                        ModifiedColumnSet.EMPTY));
            } else {
                if (added != null) {
                    added.close();
                }
                if (removed != null) {
                    removed.close();
                }
            }
        }

        private void processAdditions(final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate) {
            final Stream<LocationState> newPendingLocations =
                    locationUpdate.getPendingAddedLocationKeys().stream()
                            .map(LiveSupplier::get)
                            .filter(locationKeyMatcher)
                            .map(tableLocationProvider::getTableLocation)
                            .peek(this::manage)
                            .map(LocationState::new);
            if (pendingLocationStates != null) {
                newPendingLocations.forEach(pendingLocationStates::offer);
            } else {
                newPendingLocations.forEach(readyLocationStates::offer);
            }
        }

        private void checkPendingLocations() {
            if (pendingLocationStates == null) {
                return;
            }

            for (final Iterator<LocationState> iter = pendingLocationStates.iterator(); iter.hasNext(); ) {
                final LocationState pendingLocationState = iter.next();
                if (pendingLocationState.exists()) {
                    iter.remove();
                    readyLocationStates.offer(pendingLocationState);
                }
            }
        }

        private RowSet addReadyLocations() {
            if (readyLocationStates.isEmpty()) {
                return null;
            }

            final RowSet added = sortAndAddLocations(readyLocationStates.stream().map(LocationState::release));
            readyLocationStates.clearFast();
            return added;
        }

        private RowSet processRemovals(final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate) {
            final Set<ImmutableTableLocationKey> relevantRemovedLocations =
                    locationUpdate.getPendingRemovedLocationKeys()
                            .stream()
                            .map(LiveSupplier::get)
                            .filter(locationKeyMatcher)
                            .collect(Collectors.toSet());

            if (relevantRemovedLocations.isEmpty()) {
                return RowSetFactory.empty();
            }

            // Iterate through the pending locations and remove any that are in the removed set.
            if (pendingLocationStates != null) {
                List<LivenessReferent> toUnmanage = null;
                for (final Iterator<LocationState> iter = pendingLocationStates.iterator(); iter.hasNext(); ) {
                    final LocationState pendingLocationState = iter.next();
                    if (relevantRemovedLocations.contains(pendingLocationState.location.getKey())) {
                        iter.remove();
                        // Release the state and plan to unmanage the location
                        if (toUnmanage == null) {
                            toUnmanage = new ArrayList<>();
                        }
                        toUnmanage.add(pendingLocationState.release());
                    }
                }
                if (toUnmanage != null) {
                    unmanage(toUnmanage.stream());
                    // noinspection UnusedAssignment
                    toUnmanage = null;
                }
            }

            // At the end of the cycle we need to make sure we unmanage any removed constituents.
            this.removedConstituents = new ArrayList<>(relevantRemovedLocations.size());
            final RowSetBuilderSequential deleteBuilder = RowSetFactory.builderSequential();

            // We don't have a map of location key to row key, so we have to iterate them. If we decide this is too
            // slow, we could add a TObjectIntMap as we process pending added locations and then we can just make an
            // RowSet of rows to remove by looking up in that map.
            // @formatter:off
            try (final CloseableIterator<ImmutableTableLocationKey> keysIterator =
                         ChunkedObjectColumnIterator.make(resultTableLocationKeys, resultRows);
                 final CloseableIterator<Table> constituentsIterator =
                         ChunkedObjectColumnIterator.make(resultLocationTables, resultRows);
                 final RowSet.Iterator rowsIterator = resultRows.iterator()) {
                // @formatter:on
                while (keysIterator.hasNext()) {
                    final TableLocationKey key = keysIterator.next();
                    final Table constituent = constituentsIterator.next();
                    final long rowKey = rowsIterator.nextLong();
                    if (relevantRemovedLocations.contains(key)) {
                        deleteBuilder.appendKey(rowKey);
                        removedConstituents.add(constituent);
                    }
                }
            }

            if (removedConstituents.isEmpty()) {
                removedConstituents = null;
                return RowSetFactory.empty();
            }
            this.removedLocationsCommitter.maybeActivate();

            final WritableRowSet deletedRows = deleteBuilder.build();
            Arrays.stream(resultPartitionValues).forEach(cs -> cs.setNull(deletedRows));
            resultTableLocationKeys.setNull(deletedRows);
            resultLocationTables.setNull(deletedRows);
            return deletedRows;
        }

        @Override
        public boolean satisfied(final long step) {
            if (refreshCombiner == null) {
                throw new UnsupportedOperationException("This method should not be called when result is static");
            }
            return refreshCombiner.satisfied(step);
        }

        @Override
        public UpdateGraph getUpdateGraph() {
            if (refreshCombiner == null) {
                throw new UnsupportedOperationException("This method should not be called when result is static");
            }
            return refreshCombiner.getUpdateGraph();
        }

        private final class LocationState implements LivenessNode, TableLocationProvider {

            private final LiveSupplier<ImmutableTableLocationKey> keySupplier;

            private volatile TableLocation location;
            private volatile TableLocationUpdateSubscriptionBuffer subscriptionBuffer;
            private volatile Table table;

            private LocationState(@NotNull final LiveSupplier<ImmutableTableLocationKey> keySupplier) {
                this.keySupplier = keySupplier;
                keySupplier.retainReference();
            }

            private TableLocationKey key() {
                return keySupplier.get();
            }

            private TableLocation location() {
                // We do not manage the location. It is sufficient for our purposes to manage the key supplier, because
                // that keeps the location available from the source provider.
                TableLocation localLocation;
                if ((localLocation = location) == null) {
                    synchronized (this) {
                        if ((localLocation = location) == null) {
                            location = localLocation = tableLocationProvider.getTableLocation(key());
                        }
                    }
                }
                return localLocation;
            }

            private TableLocationUpdateSubscriptionBuffer subscriptionBuffer() {
                TableLocationUpdateSubscriptionBuffer localSubscriptionBuffer;
                if ((localSubscriptionBuffer = subscriptionBuffer) == null) {
                    final TableLocation localLocation = location();
                    synchronized (this) {
                        if ((localSubscriptionBuffer = subscriptionBuffer) == null) {
                            subscriptionBuffer = localSubscriptionBuffer =
                                    new TableLocationUpdateSubscriptionBuffer(localLocation);
                        }
                    }
                }
                return localSubscriptionBuffer;
            }

            /**
             * Test if the pending location is ready for inclusion in the result table. This means it must have non-null,
             * non-zero size. We expect that this means the location will be immediately included in the resulting table's
             * {@link ColumnSourceManager}, which is a
             * {@link io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager} in all cases.
             *
             * @return Whether this location exists for purposes of inclusion in the result table
             */
            private boolean exists() {
                subscriptionBuffer().processPending();
                final long localSize = location().getSize();
                // noinspection ConditionCoveredByFurtherCondition
                return localSize != TableLocationState.NULL_SIZE && localSize > 0;
            }

            private Table table() {
                Table localTable;
                if ((localTable = table) == null) {
                    final TableLocation localLocation = location();
                    synchronized (this) {
                        if ((localTable = table) == null) {
                            table = localTable = makeConstituentTable(localLocation);
                        }
                    }
                }
                return localTable;
            }

            /**
             * Get rid of the underlying subscription in this state, and return the location.
             *
             * @return The location
             */
            private TableLocation release() {
                subscriptionBuffer.reset();
                return location;
            }
        }
    }
}
