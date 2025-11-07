//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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
import io.deephaven.engine.table.impl.select.analyzers.ViewColumnLayer;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.regioned.RegionedTableComponentFactoryImpl;
import io.deephaven.engine.table.iterators.ChunkedObjectColumnIterator;
import io.deephaven.engine.updategraph.*;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.datastructures.LinkedWeakReferenceManager;
import io.deephaven.util.mutable.MutableLong;
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
     * @param constituentTransformer Function to apply to each "raw" constituent before making the result available to
     *        downstream consumers. This may be used, for example, in order to correctly restrict the visible result
     *        rows in constituent tables. May be {@code null} if no transformations are needed. Must not return
     *        {@code null} tables, or tables with a {@link Table#getDefinition() definition} that does not match
     *        {@code constituentDefinition}.
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
            @Nullable final UnaryOperator<Table> constituentTransformer,
            @NotNull final TableLocationProvider tableLocationProvider,
            final boolean subscribeToTableLocationProvider,
            final boolean subscribeToTableLocations,
            @Nullable final Predicate<ImmutableTableLocationKey> locationKeyMatcher) {
        super(new UnderlyingTableMaintainer(
                constituentDefinition,
                constituentTransformer == null
                        ? UnaryOperator.identity()
                        : constituentTransformer,
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
        private final UnaryOperator<Table> constituentTransformer;
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
                @NotNull final UnaryOperator<Table> constituentTransformer,
                @NotNull final TableLocationProvider tableLocationProvider,
                @NotNull final Predicate<ImmutableTableLocationKey> locationKeyMatcher,
                final boolean subscribeToTableLocationProvider,
                final boolean subscribeToTableLocations) {
            super(false);

            this.constituentDefinition = constituentDefinition;
            this.constituentTransformer = constituentTransformer;
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
                        (TableLocationKey tlk) -> getPartitionValue(tlk, partitioningColumnName)));
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
                // It's tempting to make the UTM be the UpdateSourceRegistrar instead of a separate tool, but that
                // would mean any surviving constituent keeps the UTM referenced and live.
                refreshCombiner = new UpdateSourceCombiner(
                        rawResult.getUpdateGraph(), true, LinkedWeakReferenceManager::new);
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
                        UnderlyingTableMaintainer::unmanageForRemovedLocationStates);
                processBufferedLocationChanges(false);
            } else {
                sourceTableLocations = null;
                processLocationsUpdateRoot = null;
                removedLocationsCommitter = null;
                tableLocationProvider.refresh();

                // "Leak" a reference to this UTM. We want to keep the key suppliers in the LocationStates alive until
                // the UTM is GC'd. This is necessary because we are deferring constituent creation; the constituents
                // will eventually manage the TLs, and we need the key suppliers to keep them from being fully removed
                // until then.
                retainReference();
                final Collection<LocationState> locationStates = new ArrayList<>();
                tableLocationProvider.getTableLocationKeys(
                        lstlk -> locationStates.add(new LocationState(lstlk)),
                        locationKeyMatcher);
                try (final RowSet added = sortAndAddLocations(locationStates.stream())) {
                    if (added != null) {
                        resultRows.insert(added);
                    }
                }
            }

            result = ViewColumnLayer.allowLivenessReferentResults(() -> (QueryTable) rawResult.view(resultColumns));
        }

        private static <T> T getPartitionValue(
                @NotNull final TableLocationKey tableLocationKey,
                @NotNull final String partitioningColumnName) {
            return tableLocationKey.getPartitionValue(partitioningColumnName);
        }

        private static void unmanageForRemovedLocationStates(
                @NotNull final UnderlyingTableMaintainer underlyingTableMaintainer) {
            final List<LocationState> removedLocationStates = underlyingTableMaintainer.removedLocationStates;
            Assert.neqNull(removedLocationStates, "removedLocationStates");
            underlyingTableMaintainer.unmanage(
                    removedLocationStates.stream().flatMap(LocationState::referentsToUnmanage));
            underlyingTableMaintainer.removedLocationStates = null;
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
                resultLocationStates.ensureCapacity(constituentRowKey + 1);
                resultLocationStates.set(constituentRowKey, ls);
            });
            return initialLastRowKey == lastInsertedRowKey.get()
                    ? null
                    : RowSetFactory.fromRange(initialLastRowKey + 1, lastInsertedRowKey.get());
        }

        private void processBufferedLocationChanges(final boolean notifyListeners) {
            final RowSet removed;
            final RowSet added;

            try (final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate =
                    sourceTableLocations.processPending()) {
                if (locationUpdate == null) {
                    return;
                }
                removed = processRemovals(locationUpdate.getPendingRemovedLocationKeys());
                added = processAdditions(locationUpdate.getPendingAddedLocationKeys());
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
        private RowSet processAdditions(final Collection<LiveSupplier<ImmutableTableLocationKey>> addedKeySuppliers) {
            if (addedKeySuppliers.isEmpty()) {
                return null;
            }
            return sortAndAddLocations(addedKeySuppliers
                    .stream()
                    .filter(lstlk -> locationKeyMatcher.test(lstlk.get()))
                    .map(LocationState::new));
        }

        @Nullable
        private RowSet processRemovals(final Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeySuppliers) {
            if (removedKeySuppliers.isEmpty()) {
                return null;
            }

            final Set<ImmutableTableLocationKey> relevantRemovedLocationKeys = removedKeySuppliers
                    .stream()
                    .map(LiveSupplier::get)
                    .filter(locationKeyMatcher)
                    .collect(Collectors.toSet());

            if (relevantRemovedLocationKeys.isEmpty()) {
                return null;
            }

            // At the end of the cycle we need to make sure we unmanage the key providers for removed location states.
            removedLocationStates = new ArrayList<>(relevantRemovedLocationKeys.size());
            final RowSetBuilderSequential deleteBuilder = RowSetFactory.builderSequential();

            // We don't have a map of location key to row key, so we have to iterate the location states. If this
            // becomes a performance issue, we can consider adding a map.
            // @formatter:off
            try (final CloseableIterator<LocationState> locationStatesIterator =
                         ChunkedObjectColumnIterator.make(resultLocationStates, resultRows);
                 final RowSet.Iterator rowsIterator = resultRows.iterator()) {
                // @formatter:on
                while (locationStatesIterator.hasNext() && !relevantRemovedLocationKeys.isEmpty()) {
                    final LocationState locationState = locationStatesIterator.next();
                    final long rowKey = rowsIterator.nextLong();
                    if (relevantRemovedLocationKeys.remove(locationState.key())) {
                        deleteBuilder.appendKey(rowKey);
                        removedLocationStates.add(locationState);
                    }
                }
            }

            if (removedLocationStates.isEmpty()) {
                removedLocationStates = null;
                return null;
            }
            removedLocationsCommitter.maybeActivate();

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

        private final class LocationState {

            private final LiveSupplier<ImmutableTableLocationKey> keySupplier;

            private volatile Table table;

            private LocationState(@NotNull final LiveSupplier<ImmutableTableLocationKey> keySupplier) {
                this.keySupplier = keySupplier;
                // Note that this is intentionally unconditional. As long as the LocationState is reachable, the UTM
                // will be reachable.
                UnderlyingTableMaintainer.this.manage(keySupplier);
            }

            private ImmutableTableLocationKey key() {
                return keySupplier.get();
            }

            private Table table() {
                Table localTable;
                if ((localTable = table) == null) {
                    final TableLocationKey locationKey = key();
                    synchronized (this) {
                        if ((localTable = table) == null) {
                            table = localTable = makeConstituentTable(locationKey);
                        }
                    }
                }
                return localTable;
            }

            private Table makeConstituentTable(@NotNull final TableLocationKey locationKey) {
                final TableLocation tableLocation = tableLocationProvider.getTableLocation(locationKey);
                final boolean refreshing = subscribeToTableLocations && tableLocation.supportsSubscriptions();
                try (final SafeCloseable ignored = refreshing ? LivenessScopeStack.open() : null) {
                    final BaseTable<?> constituent = new PartitionAwareSourceTable(
                            constituentDefinition,
                            SourcePartitionedTable.class.getSimpleName() + '[' + tableLocationProvider + ']'
                                    + "-Constituent-" + locationKey,
                            // We could consider a simpler/smaller CSM implementation, but for now RCSM is all we have.
                            RegionedTableComponentFactoryImpl.INSTANCE,
                            // Note, it might be tempting to use the LocationState as a TLP, but that would cause each
                            // constituent to reference the UTM, which would in turn keep all constituents around as
                            // long as any are referenced externally.
                            new SingleTableLocationProvider(
                                    tableLocation,
                                    refreshing
                                            ? tableLocationProvider.getLocationUpdateMode()
                                            : TableUpdateMode.STATIC),
                            refreshing ? refreshCombiner : null);

                    // SourceTable keeps a reference to its UpdateSourceRegistrar, but in this case we must also manage
                    // it to ensure that the refresh combiner stays live while the constituent is live. We don't need it
                    // to be a parent since BaseTable will check the UpdateGraph directly for satisfaction.
                    if (refreshing) {
                        constituent.manage(refreshCombiner);
                    }

                    // Be careful to propagate the systemic attribute properly to child tables
                    constituent.setAttribute(Table.SYSTEMIC_TABLE_ATTRIBUTE, result.isSystemicObject());

                    // Apply the provided transformer
                    final Table transformed = constituentTransformer.apply(constituent);

                    // Be sure the UTM manages the result if its refreshing
                    if (transformed.isRefreshing()) {
                        UnderlyingTableMaintainer.this.manage(transformed);
                    }

                    return transformed;
                }
            }

            /**
             * @return A {@link Stream} of {@link LivenessReferent referents} to be unmanaged after this location state
             *         is removed. The stream may contain {@code null} elements. Clears the {@link #table} reference as
             *         a side effect.
             */
            private Stream<? extends LivenessReferent> referentsToUnmanage() {
                final Table localTable = table;
                table = null;
                return localTable == null || !localTable.isRefreshing()
                        ? Stream.of(keySupplier)
                        : Stream.of(keySupplier, localTable);
            }
        }
    }
}
