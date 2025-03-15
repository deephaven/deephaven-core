//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Selectable;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.*;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.table.impl.locations.impl.SingleTableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.table.iterators.ChunkedObjectColumnIterator;
import io.deephaven.engine.updategraph.*;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
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
    private static final TableDefinition RAW_RESULT_DEFINITION =
            TableDefinition.of(
                    ColumnDefinition.fromGenericType(STATE_COLUMN_NAME, UnderlyingTableMaintainer.LocationState.class));

    private static final String KEY_COLUMN_NAME = "TableLocationKey";
    private static final String CONSTITUENT_COLUMN_NAME = "LocationTable";

    /**
     * Construct a {@link SourcePartitionedTable} from the supplied parameters, excluding empty locations.
     * <p>
     * Note that {@code subscribeToTableLocationProvider} and {@code subscribeToTableLocations} are distinct because
     * there may be use cases that supply their own {@link RowSet} for constituents. Others might care to observe
     * changes to constituent rows, but only the initially-available set of locations.
     *
     * @param constituentDefinition The {@link TableDefinition} expected of constituent {@link Table tables}
     * @param applyTablePermissions Function to apply in order to correctly restrict the visible result rows in
     *        constituent tables; may be {@code null} if no permissions are needed
     * @param tableLocationProvider Source for table locations
     * @param subscribeToTableLocationProvider Whether changes to the set of available locations after instantiation
     *        should be reflected in the result SourcePartitionedTable; that is, whether constituents should be added or
     *        removed
     * @param subscribeToTableLocations Whether constituents should be updated to reflect changes in their available
     *        rows
     * @param locationKeyMatcher Function to filter desired location keys; only locations for which
     *        {@link Predicate#test test} returns {@code true} will be included; may be {@code null} to include all
     */
    public SourcePartitionedTable(
            @NotNull final TableDefinition constituentDefinition,
            @Nullable final UnaryOperator<Table> applyTablePermissions,
            @NotNull final TableLocationProvider tableLocationProvider,
            final boolean subscribeToTableLocationProvider,
            final boolean subscribeToTableLocations,
            @Nullable final Predicate<ImmutableTableLocationKey> locationKeyMatcher) {
        super(new UnderlyingTableMaintainer(
                constituentDefinition,
                applyTablePermissions == null
                        ? UnaryOperator.identity()
                        : applyTablePermissions,
                tableLocationProvider,
                locationKeyMatcher == null
                        ? key -> true
                        : locationKeyMatcher,
                subscribeToTableLocationProvider
                        && tableLocationProvider.supportsSubscriptions()
                        && tableLocationProvider.getUpdateMode() != TableUpdateMode.STATIC,
                subscribeToTableLocations
                        && tableLocationProvider.getLocationUpdateMode() != TableUpdateMode.STATIC)
                .activateAndGetResult(),
                Set.of(KEY_COLUMN_NAME),
                true,
                CONSTITUENT_COLUMN_NAME,
                constituentDefinition,
                subscribeToTableLocationProvider
                        && tableLocationProvider.supportsSubscriptions()
                        && tableLocationProvider.getUpdateMode() != TableUpdateMode.STATIC,
                false);
    }

    private static class UnderlyingTableMaintainer
            extends ReferenceCountedLivenessNode
            implements NotificationQueue.Dependency {

        private final TableDefinition constituentDefinition;
        private final UnaryOperator<Table> applyTablePermissions;
        private final TableLocationProvider tableLocationProvider;
        private final boolean subscribeToTableLocations;
        private final Predicate<ImmutableTableLocationKey> locationKeyMatcher;

        private final TrackingWritableRowSet resultRows;
        private final WritableColumnSource<LocationState> resultLocationStates;
        private final QueryTable result;

        private final UpdateSourceCombiner refreshCombiner;
        private final TableLocationSubscriptionBuffer sourceTableLocations;
        @ReferentialIntegrity
        private final Runnable processLocationsUpdateRoot;

        private final UpdateCommitter<UnderlyingTableMaintainer> removedLocationsCommitter;
        private List<LocationState> removedLocationStates = null;

        private UnderlyingTableMaintainer(
                @NotNull final TableDefinition constituentDefinition,
                @NotNull final UnaryOperator<Table> applyTablePermissions,
                @NotNull final TableLocationProvider tableLocationProvider,
                @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher,
                final boolean subscribeToTableLocationProvider,
                final boolean subscribeToTableLocations) {
            super(false);

            this.constituentDefinition = constituentDefinition;
            this.applyTablePermissions = applyTablePermissions;
            this.tableLocationProvider = tableLocationProvider;
            this.locationKeyMatcher = locationKeyMatcher;
            this.subscribeToTableLocations = subscribeToTableLocations;

            resultRows = RowSetFactory.empty().toTracking();
            final List<ColumnDefinition<?>> partitioningColumns = constituentDefinition.getPartitioningColumns();
            resultLocationStates = ArrayBackedColumnSource.getMemoryColumnSource(LocationState.class, null);

            final List<Selectable> resultColumns = new ArrayList<>(partitioningColumns.size() + 2);
            resultColumns.add(new FunctionalColumn<>(
                    STATE_COLUMN_NAME, LocationState.class,
                    KEY_COLUMN_NAME, TableLocationKey.class, null,
                    LocationState::key));
            for (final ColumnDefinition<?> pcd : partitioningColumns) {
                final String partitioningColumnName = pcd.getName();
                resultColumns.add(new FunctionalColumn<>(
                        KEY_COLUMN_NAME, TableLocationKey.class,
                        partitioningColumnName, pcd.getDataType(), pcd.getComponentType(),
                        (TableLocationKey tlk) -> getPartitionValue(tlk, partitioningColumnName)
                ));
            }
            resultColumns.add(new FunctionalColumn<>(
                    STATE_COLUMN_NAME, LocationState.class,
                    CONSTITUENT_COLUMN_NAME, Table.class, null,
                    LocationState::table));

            final LinkedHashMap<String, ColumnSource<?>> rawResultSources = new LinkedHashMap<>(1);
            rawResultSources.put(STATE_COLUMN_NAME, resultLocationStates);
            final QueryTable rawResult =
                    new QueryTable(RAW_RESULT_DEFINITION, resultRows, rawResultSources, null, null);

            if (subscribeToTableLocationProvider || subscribeToTableLocations) {
                rawResult.setRefreshing(true);
                refreshCombiner = new UpdateSourceCombiner(rawResult.getUpdateGraph());
                rawResult.addParentReference(this);
                manage(refreshCombiner);
            } else {
                refreshCombiner = null;
            }

            if (subscribeToTableLocationProvider) {
                resultLocationStates.startTrackingPrevValues();

                sourceTableLocations = new TableLocationSubscriptionBuffer(tableLocationProvider);
                manage(sourceTableLocations);

                processLocationsUpdateRoot = new InstrumentedTableUpdateSource(
                        refreshCombiner,
                        rawResult,
                        SourcePartitionedTable.class.getSimpleName() + '[' + tableLocationProvider + ']'
                                + "-processBufferedLocationChanges") {
                    @Override
                    protected void instrumentedRefresh() {
                        processBufferedLocationChanges(true);
                    }
                };
                refreshCombiner.addSource(processLocationsUpdateRoot);

                this.removedLocationsCommitter = new UpdateCommitter<>(
                        this,
                        rawResult.getUpdateGraph(),
                        ignored -> {
                            Assert.neqNull(removedLocationStates, "removedLocationStates");
                            rawResult.unmanage(removedLocationStates.stream());
                            removedLocationStates = null;
                        });
                processBufferedLocationChanges(false);
            } else {
                sourceTableLocations = null;
                processLocationsUpdateRoot = null;
                removedLocationsCommitter = null;
                tableLocationProvider.refresh();

                final Collection<LocationState> locationStates = new ArrayList<>();
                try {
                    retainReference();
                    tableLocationProvider.getTableLocationKeys(
                            lstlk -> locationStates.add(new LocationState(lstlk)),
                            locationKeyMatcher);
                    try (final RowSet added = sortAndAddLocations(locationStates.stream())) {
                        if (added != null) {
                            resultRows.insert(added);
                        }
                    }
                } finally {
                    dropReference();
                }
            }

            // TODO: I think we want to make the constituents use the regular update graph. Then, we can probably use
            // filteredResult as the "resultUpdatedDependency" for purposes of a ConstituentDependency on
            // LocationStates, which can be Dependencies that bleed through to the Table iff its been made.
            // TODO: How to force parallel construction for constituents?
            result = (QueryTable) rawResult.view(resultColumns);
        }

        private QueryTable activateAndGetResult() {
            if (refreshCombiner != null) {
                refreshCombiner.install();
            }
            return result;
        }

        private RowSet sortAndAddLocations(@NotNull final Stream<LocationState> locationStates) {
            final long initialLastRowKey = resultRows.lastRowKey();
            final MutableLong lastInsertedRowKey = new MutableLong(initialLastRowKey);
            locationStates.sorted(Comparator.comparing(LocationState::key)).forEach(ls -> {
                final long constituentRowKey = lastInsertedRowKey.incrementAndGet();

                for (int pci = 0; pci < resultPartitionValues.length; ++pci) {
                    addPartitionValue(
                            ls.key(),
                            partitioningColumnNames[pci],
                            resultPartitionValues[pci],
                            constituentRowKey);
                }

                resultLocationStates.ensureCapacity(constituentRowKey + 1);
                resultLocationStates.set(constituentRowKey, ls);
            });
            return initialLastRowKey == lastInsertedRowKey.get()
                    ? RowSetFactory.empty()
                    : RowSetFactory.fromRange(initialLastRowKey + 1, lastInsertedRowKey.get());
        }

        private static <T> T getPartitionValue(
                @NotNull final TableLocationKey tableLocationKey,
                @NotNull final String partitioningColumnName) {
            return tableLocationKey.getPartitionValue(partitioningColumnName));
        }

        private void processBufferedLocationChanges(final boolean notifyListeners) {
            final RowSet removed;
            final RowSet added;

            try (final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate =
                    sourceTableLocations.processPending()) {
                if (locationUpdate == null) {
                    return;
                }
                removed = processRemovals(locationUpdate);
                added = processAdditions(locationUpdate);
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

        @Nullable
        private RowSet processAdditions(final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate) {
            return sortAndAddLocations(locationUpdate.getPendingAddedLocationKeys()
                    .stream()
                    .filter(lstlk -> locationKeyMatcher.test(lstlk.get()))
                    .map(LocationState::new));
        }

        @Nullable
        private RowSet processRemovals(final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate) {
            final Set<ImmutableTableLocationKey> relevantRemovedLocationKeys =
                    locationUpdate.getPendingRemovedLocationKeys()
                            .stream()
                            .map(LiveSupplier::get)
                            .filter(locationKeyMatcher)
                            .collect(Collectors.toSet());

            if (relevantRemovedLocationKeys.isEmpty()) {
                return null;
            }

            // At the end of the cycle we need to make sure we unmanage the key providers for removed location states.
            this.removedLocationStates = new ArrayList<>(relevantRemovedLocationKeys.size());
            final RowSetBuilderSequential deleteBuilder = RowSetFactory.builderSequential();

            // We don't have a map of location key to row key, so we have to iterate the location states. If this
            // becomes a performance issue, we can consider adding a map.
            // @formatter:off
            try (final CloseableIterator<LocationState> locationStatesIterator =
                         ChunkedObjectColumnIterator.make(resultLocationStates, resultRows);
                 final RowSet.Iterator rowsIterator = resultRows.iterator()) {
                // @formatter:on
                while (locationStatesIterator.hasNext()) {
                    final LocationState locationState = locationStatesIterator.next();
                    final long rowKey = rowsIterator.nextLong();
                    if (relevantRemovedLocationKeys.contains(locationState.key())) {
                        deleteBuilder.appendKey(rowKey);
                        removedLocationStates.add(locationState);
                    }
                }
            }

            if (removedLocationStates.isEmpty()) {
                removedLocationStates = null;
                return null;
            }
            this.removedLocationsCommitter.maybeActivate();

            final WritableRowSet deletedRows = deleteBuilder.build();
            resultLocationStates.setNull(deletedRows);
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

        @Override
        public void addSource(@NotNull final Runnable updateSource) {

        }

        @Override
        public void removeSource(@NotNull final Runnable updateSource) {

        }

        @Override
        public void requestRefresh() {

        }

        @Override
        public void run() {

        }

        private final class LocationState {

            private final LiveSupplier<ImmutableTableLocationKey> keySupplier;

            private volatile WeakReference<Table> tableRef;

            private LocationState(@NotNull final LiveSupplier<ImmutableTableLocationKey> keySupplier) {
                this.keySupplier = keySupplier;
                UnderlyingTableMaintainer.this.manage(keySupplier);
            }

            private ImmutableTableLocationKey key() {
                return keySupplier.get();
            }

            private Table table() {
                Table localTable;
                if ((localTable = existingCachedTable()) == null) {
                    final TableLocationKey locationKey = key();
                    synchronized (this) {
                        if ((localTable = existingCachedTable()) == null) {
                            localTable = LivenessScopeStack.computeEnclosed(
                                    () -> makeConstituentTable(locationKey),
                                    () -> subscribeToTableLocations,
                                    Table::isRefreshing);
                            tableRef = new WeakReference<>(localTable);
                        }
                    }
                }
                return localTable;
            }

            private Table existingCachedTable() {
                final WeakReference<Table> localTableRef = tableRef;
                if (localTableRef == null) {
                    return null;
                }
                final Table localTable = localTableRef.get();
                if (localTable == null || localTable.isFailed() || !Liveness.verifyCachedObjectForReuse(localTable)) {
                    return null;
                }
                return localTable;
            }

            private Table makeConstituentTable(@NotNull final TableLocationKey locationKey) {
                final PartitionAwareSourceTable constituent = new PartitionAwareSourceTable(
                        constituentDefinition,
                        "SingleLocationSourceTable-" + locationKey,
                        RegionedTableComponentFactoryImpl.INSTANCE,
                        new SingleTableLocationProvider(
                                tableLocationProvider.getTableLocation(key()),
                                subscribeToTableLocations
                                        ? tableLocationProvider.getLocationUpdateMode()
                                        : TableUpdateMode.STATIC),
                        subscribeToTableLocations ? refreshCombiner : null);

                // Be careful to propagate the systemic attribute properly to child tables
                constituent.setAttribute(Table.SYSTEMIC_TABLE_ATTRIBUTE, result.isSystemicObject());
                // TODO: Make sure we manage the constituent appropriately
                // TODO: How to ensure that we coalesce constituents in parallel?
                return applyTablePermissions.apply(constituent);
            }

            /**
             * Get rid of the underlying subscription in this state, if any, and release the key supplier.
             */
            private void release() {
                final Table localTable = table;
                if (localTable != null && localTable.isRefreshing()) {
                    localTable.dropReference();
                }
                keySupplier.dropReference();
            }
        }
    }
}
