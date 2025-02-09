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
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.table.impl.locations.impl.TableLocationUpdateSubscriptionBuffer;
import io.deephaven.engine.table.impl.partitioned.PartitionedTableImpl;
import io.deephaven.engine.table.impl.select.FunctionalColumn;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.SourceColumn;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.table.iterators.ChunkedObjectColumnIterator;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
    private static final String EMPTY_COLUMN_NAME = "LocationEmpty";
    private static final String KEY_COLUMN_NAME = "TableLocationKey";
    private static final String CONSTITUENT_COLUMN_NAME = "LocationTable";

    /**
     * Construct a {@link SourcePartitionedTable} from the supplied parameters, excluding empty locations.
     * <p>
     * Note that {@code refreshLocations} and {@code refreshLocationSizes} are distinct because there are use cases that
     * supply an external {@link RowSet} and hence don't require size refreshes. Others might care for size refreshes,
     * but only the initially-available set of locations.
     *
     * @param constituentDefinition The {@link TableDefinition} expected of constituent {@link Table tables}
     * @param applyTablePermissions Function to apply in order to correctly restrict the visible result rows
     * @param tableLocationProvider Source for table locations
     * @param refreshLocations Whether changes to the set of available locations after instantiation should be reflected
     *        in the result SourcePartitionedTable; that is, whether constituents should be added or removed
     * @param refreshLocationSizes Whether empty locations should be re-examined on subsequent {@link UpdateGraph}
     *        cycles for possible inclusion if their size has changed.
     * @param locationKeyMatcher Function to filter desired location keys
     */
    @Deprecated(forRemoval = true)
    public SourcePartitionedTable(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final UnaryOperator<Table> applyTablePermissions,
            @NotNull final TableLocationProvider tableLocationProvider,
            final boolean refreshLocations,
            final boolean refreshLocationSizes,
            @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher) {
        this(constituentDefinition, applyTablePermissions, tableLocationProvider, locationKeyMatcher,
                refreshLocations, true, refreshLocationSizes);
    }

    // TODO: Make the full constructor private? Encourage include-empty?

    /**
     * Construct a {@link SourcePartitionedTable} from the supplied parameters.
     * <p>
     * Note that {@code refreshLocations} and {@code refreshLocationSizes} are distinct because there are use cases that
     * supply an external {@link RowSet} and hence don't require size refreshes. Others might care for size refreshes,
     * but only the initially-available set of locations.
     *
     * @param constituentDefinition The {@link TableDefinition} expected of constituent {@link Table tables}
     * @param applyTablePermissions Function to apply in order to correctly restrict the visible result rows
     * @param tableLocationProvider Source for table locations
     * @param locationKeyMatcher Function to accept desired location keys
     * @param refreshLocations Whether changes to the set of available locations after instantiation should be reflected
     *        in the result SourcePartitionedTable; that is, whether constituents should be added or removed
     * @param excludeEmptyLocations Whether to exclude empty locations (that is, locations with null or zero
     *        {@link TableLocation#getSize() size}) instead of including them in the result SourcePartitionedTable as
     *        constituents. It is recommended to set this to {@code false} if you will do subsequent filtering on the
     *        result, or if you are confident that all locations are valid. Note that once a constituent is added, it
     *        will not be removed if its size becomes zero, only if {@code refreshLocations == true} and it is removed
     *        by the {@link TableLocationProvider}.
     * @param refreshLocationSizes Whether empty locations should be re-examined on subsequent {@link UpdateGraph}
     *        cycles for possible inclusion if their size has changed. This is only relevant if
     *        {@code excludeEmptyLocations == true}.
     */
    public SourcePartitionedTable(
            @NotNull final TableDefinition constituentDefinition,
            @NotNull final UnaryOperator<Table> applyTablePermissions,
            @NotNull final TableLocationProvider tableLocationProvider,
            @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher,
            final boolean refreshLocations,
            final boolean excludeEmptyLocations,
            final boolean refreshLocationSizes) {
        super(new UnderlyingTableMaintainer(
                constituentDefinition,
                applyTablePermissions,
                tableLocationProvider,
                locationKeyMatcher,
                refreshLocations,
                excludeEmptyLocations,
                refreshLocationSizes).activateAndGetResult(),
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
        private final Predicate<ImmutableTableLocationKey> locationKeyMatcher;
        private final boolean refreshSizes;

        private final TrackingWritableRowSet resultRows;
        private final String[] partitioningColumnNames;
        private final WritableColumnSource<?>[] resultPartitionValues;
        private final WritableColumnSource<LocationState> resultLocationStates;
        private final QueryTable result;

        private final UpdateSourceCombiner refreshCombiner;
        private final TableLocationSubscriptionBuffer subscriptionBuffer;
        @ReferentialIntegrity
        private final Runnable processNewLocationsUpdateRoot;
        @ReferentialIntegrity
        private final Runnable nonEmptyFilterRequestRecomputeUpdateRoot;

        private final UpdateCommitter<UnderlyingTableMaintainer> removedLocationsCommitter;
        private List<LocationState> removedLocationStates = null;

        private UnderlyingTableMaintainer(
                @NotNull final TableDefinition constituentDefinition,
                @NotNull final UnaryOperator<Table> applyTablePermissions,
                @NotNull final TableLocationProvider tableLocationProvider,
                @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher,
                final boolean refreshLocations,
                final boolean excludeEmptyLocations,
                final boolean refreshLocationSizes) {
            super(false);

            this.constituentDefinition = constituentDefinition;
            this.applyTablePermissions = applyTablePermissions;
            this.tableLocationProvider = tableLocationProvider;
            this.locationKeyMatcher = locationKeyMatcher;
            this.refreshSizes = excludeEmptyLocations && refreshLocationSizes;

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
                        refreshCombiner,
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
                            Assert.neqNull(removedLocationStates, "removedLocationStates");
                            rawResult.unmanage(removedLocationStates.stream());
                            removedLocationStates = null;
                        });
                processBufferedLocationChanges(false);
            } else {
                subscriptionBuffer = null;
                processNewLocationsUpdateRoot = null;
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

            final Table filteredResult;
            if (excludeEmptyLocations) {
                final List<Selectable> emptyColumns = List.of(new FunctionalColumn<>(
                        STATE_COLUMN_NAME, LocationState.class,
                        EMPTY_COLUMN_NAME, Boolean.class, null,
                        LocationState::empty));
                final MutableObject<WhereFilter.RecomputeListener> recomputeListenerHolder = new MutableObject<>();
                final Filter nonEmptyFilter = new MatchFilter(MatchFilter.MatchType.Regular, EMPTY_COLUMN_NAME, false) {
                    @Override
                    public void setRecomputeListener(@NotNull final RecomputeListener recomputeListener) {
                        recomputeListenerHolder.setValue(recomputeListener);
                    }
                };
                filteredResult = rawResult.updateView(emptyColumns).where(nonEmptyFilter);
                if (refreshLocationSizes) {
                    nonEmptyFilterRequestRecomputeUpdateRoot = new InstrumentedTableUpdateSource(
                            refreshCombiner,
                            recomputeListenerHolder.getValue().getTable(),
                            SourcePartitionedTable.class.getSimpleName() + '[' + tableLocationProvider + ']'
                                    + "-nonEmptyFilterRequestRecompute") {
                        private final WhereFilter.RecomputeListener recomputeListener =
                                recomputeListenerHolder.getValue();

                        @Override
                        protected void instrumentedRefresh() {
                            // Note: We could exclude constituents that become empty if we change the request method we
                            // invoke.
                            recomputeListener.requestRecomputeUnmatched();
                        }
                    };
                    refreshCombiner.addSource(nonEmptyFilterRequestRecomputeUpdateRoot);
                } else {
                    nonEmptyFilterRequestRecomputeUpdateRoot = null;
                }
            } else {
                filteredResult = rawResult;
                nonEmptyFilterRequestRecomputeUpdateRoot = null;
            }
            result = (QueryTable) filteredResult.view(resultColumns);
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

        private static <T> void addPartitionValue(
                @NotNull final TableLocationKey tableLocationKey,
                @NotNull final String partitioningColumnName,
                @NotNull final WritableColumnSource<T> partitionValueColumn,
                final long rowKey) {
            partitionValueColumn.ensureCapacity(rowKey + 1);
            partitionValueColumn.set(rowKey, tableLocationKey.getPartitionValue(partitioningColumnName));
        }

        private void processBufferedLocationChanges(final boolean notifyListeners) {
            final RowSet removed;
            final RowSet added;

            try (final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate =
                    subscriptionBuffer.processPending()) {
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

            // At the end of the cycle we need to make sure we unmanage any removed constituents.
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
            Arrays.stream(resultPartitionValues).forEach(cs -> cs.setNull(deletedRows));
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

        private final class LocationState implements LivenessNode, TableLocationProvider {

            private final LiveSupplier<ImmutableTableLocationKey> keySupplier;

            private volatile TableLocationUpdateSubscriptionBuffer subscriptionBuffer;
            private volatile Table table;

            private LocationState(@NotNull final LiveSupplier<ImmutableTableLocationKey> keySupplier) {
                this.keySupplier = keySupplier;
                keySupplier.retainReference();
            }

            private ImmutableTableLocationKey key() {
                return keySupplier.get();
            }

            private TableLocation location() {
                // We do not manage the location. It is sufficient for our purposes to manage the key supplier, because
                // that keeps the location available from the source provider.
                return tableLocationProvider.getTableLocation(key());
            }

            private TableLocationUpdateSubscriptionBuffer subscriptionBuffer() {
                Assert.assertion(refreshSizes, "refreshSizes");
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
             * Test if the location is empty (that is, whether it has {@link TableLocation#NULL_SIZE null} size and thus
             * doesn't exist, or zero size). When excluding empty locations, pending locations that are empty by this
             * definition are not ready for inclusion in the result table.
             *
             * @return Whether this state's location is empty (non-existent, or zero size)
             */
            private boolean empty() {
                if (refreshSizes) {
                    subscriptionBuffer().processPending();
                } else {
                    // TODO: Confirm that this is the correct behavior for static cases
                    location().refresh();
                }
                final long localSize = location().getSize();
                // noinspection ConditionCoveredByFurtherCondition
                return localSize == TableLocationState.NULL_SIZE || localSize <= 0;
            }

            private Table table() {
                Table localTable;
                if ((localTable = table) == null) {
                    final TableLocationKey locationKey = key();
                    synchronized (this) {
                        if ((localTable = table) == null) {
                            table = localTable = makeConstituentTable(locationKey);
                            releaseSubscription();
                        }
                    }
                }
                return localTable;
            }

            private Table makeConstituentTable(@NotNull final TableLocationKey locationKey) {
                final PartitionAwareSourceTable constituent = new PartitionAwareSourceTable(
                        constituentDefinition,
                        "SingleLocationSourceTable-" + locationKey,
                        RegionedTableComponentFactoryImpl.INSTANCE,
                        this,
                        refreshSizes ? refreshCombiner : null);

                // Be careful to propagate the systemic attribute properly to child tables
                constituent.setAttribute(Table.SYSTEMIC_TABLE_ATTRIBUTE, result.isSystemicObject());
                // TODO: Make sure we manage the constituent appropriately
                // TODO: How to ensure that we coalesce constituents in parallel?
                return applyTablePermissions.apply(constituent);
            }

            /**
             * Get rid of the underlying subscription in this state, if any.
             */
            private void releaseSubscription() {
                final TableLocationUpdateSubscriptionBuffer localSubscriptionBuffer = subscriptionBuffer;
                if (localSubscriptionBuffer != null) {
                    localSubscriptionBuffer.reset();
                }
            }

            /**
             * Get rid of the underlying subscription in this state, if any, and release the key supplier.
             */
            private void release() {
                releaseSubscription();
                keySupplier.dropReference();
            }
        }
    }
}
