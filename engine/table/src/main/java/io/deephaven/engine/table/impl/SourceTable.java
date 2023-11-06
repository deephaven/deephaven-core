/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.dataindex.PartitioningColumnDataIndexImpl;
import io.deephaven.engine.table.impl.dataindex.StorageBackedDataIndexImpl;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ReferentialIntegrity;
import io.deephaven.util.annotations.TestUseOnly;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

/**
 * Basic uncoalesced table that only adds keys.
 */
public abstract class SourceTable<IMPL_TYPE extends SourceTable<IMPL_TYPE>> extends RedefinableTable<IMPL_TYPE> {

    /**
     * Component factory. Mostly held for redefinitions.
     */
    final SourceTableComponentFactory componentFactory;

    /**
     * A column source manager to maintain our column sources and define how our RowSet is updated.
     */
    final ColumnSourceManager columnSourceManager;

    /**
     * Location discovery.
     */
    final TableLocationProvider locationProvider;

    /**
     * Registrar for update sources that need to be refreshed.
     */
    final UpdateSourceRegistrar updateSourceRegistrar;

    /**
     * Whether we've done our initial location fetch.
     */
    private volatile boolean locationsInitialized;

    /**
     * Whether we've done our initial location size fetches and initialized our RowSet.
     */
    private volatile boolean locationSizesInitialized;

    /**
     * The RowSet that backs this table, shared with all child tables.
     */
    private TrackingWritableRowSet rowSet;

    /**
     * The update source object for refreshing locations and location sizes.
     */
    private Runnable locationChangePoller;

    /**
     * A reference to a delayed error notifier, if one is pending.
     */
    @ReferentialIntegrity
    private Runnable delayedErrorReference;

    /**
     * Construct a new disk-backed table.
     *
     * @param tableDefinition A TableDefinition
     * @param description A human-readable description for this table
     * @param componentFactory A component factory for creating column source managers
     * @param locationProvider A TableLocationProvider, for use in discovering the locations that compose this table
     * @param updateSourceRegistrar Callback for registering update sources for refreshes, null if this table is not
     *        refreshing
     */
    SourceTable(@NotNull final TableDefinition tableDefinition,
            @NotNull final String description,
            @NotNull final SourceTableComponentFactory componentFactory,
            @NotNull final TableLocationProvider locationProvider,
            final UpdateSourceRegistrar updateSourceRegistrar) {
        super(tableDefinition, description);

        this.componentFactory = Require.neqNull(componentFactory, "componentFactory");
        this.locationProvider = Require.neqNull(locationProvider, "locationProvider");
        this.updateSourceRegistrar = updateSourceRegistrar;

        final boolean isRefreshing = updateSourceRegistrar != null;
        columnSourceManager = componentFactory.createColumnSourceManager(isRefreshing, ColumnToCodecMappings.EMPTY,
                definition.getColumns() // NB: this is the *re-written* definition passed to the super-class
                                        // constructor.
        );

        // Add a liveness reference to the location table.
        manage(columnSourceManager.locationTable());

        setRefreshing(isRefreshing);
        setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, Boolean.TRUE);
    }

    /**
     * Force this table to determine its initial state (available locations, size, RowSet) if it hasn't already done so.
     */
    private void initialize() {
        initializeAvailableLocations();
        initializeLocationSizes();
    }

    /**
     * This is only for unit tests, at this time.
     */
    @TestUseOnly
    public final void refresh() {
        if (locationChangePoller != null) {
            locationChangePoller.run();
        }
    }

    @SuppressWarnings("WeakerAccess")
    protected final void initializeAvailableLocations() {
        if (locationsInitialized) {
            return;
        }
        synchronized (this) {
            if (locationsInitialized) {
                return;
            }
            QueryPerformanceRecorder.withNugget(description + ".initializeAvailableLocations()", () -> {
                if (isRefreshing()) {
                    final TableLocationSubscriptionBuffer locationBuffer =
                            new TableLocationSubscriptionBuffer(locationProvider);
                    final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate =
                            locationBuffer.processPending();

                    maybeRemoveLocations(locationUpdate.getPendingRemovedLocationKeys());
                    maybeAddLocations(locationUpdate.getPendingAddedLocationKeys());
                    updateSourceRegistrar.addSource(locationChangePoller = new LocationChangePoller(locationBuffer));
                } else {
                    locationProvider.refresh();
                    maybeAddLocations(locationProvider.getTableLocationKeys());
                }
            });
            locationsInitialized = true;
        }
    }

    private void maybeAddLocations(@NotNull final Collection<ImmutableTableLocationKey> locationKeys) {
        if (locationKeys.isEmpty()) {
            return;
        }
        filterLocationKeys(locationKeys)
                .forEach(lk -> columnSourceManager.addLocation(locationProvider.getTableLocation(lk)));
    }

    private ImmutableTableLocationKey[] maybeRemoveLocations(
            @NotNull final Collection<ImmutableTableLocationKey> removedKeys) {
        if (removedKeys.isEmpty()) {
            return ImmutableTableLocationKey.ZERO_LENGTH_IMMUTABLE_TABLE_LOCATION_KEY_ARRAY;
        }

        return filterLocationKeys(removedKeys).stream()
                .filter(columnSourceManager::removeLocationKey)
                .toArray(ImmutableTableLocationKey[]::new);
    }

    private void initializeLocationSizes() {
        Assert.assertion(locationsInitialized, "locationInitialized");
        if (locationSizesInitialized) {
            return;
        }
        synchronized (this) {
            if (locationSizesInitialized) {
                return;
            }
            QueryPerformanceRecorder.withNugget(description + ".initializeLocationSizes()", sizeForInstrumentation(),
                    () -> {
                        Assert.eqNull(rowSet, "rowSet");
                        rowSet = refreshLocationSizes(true).toTracking();
                        if (!isRefreshing()) {
                            return;
                        }
                        rowSet.initializePreviousValue();
                        final long currentClockValue = getUpdateGraph().clock().currentValue();
                        setLastNotificationStep(LogicalClock.getState(currentClockValue) == LogicalClock.State.Updating
                                ? LogicalClock.getStep(currentClockValue) - 1
                                : LogicalClock.getStep(currentClockValue));
                    });
            locationSizesInitialized = true;
        }
    }

    private WritableRowSet refreshLocationSizes(final boolean initializing) {
        try {
            return columnSourceManager.refresh(initializing);
        } catch (Exception e) {
            throw new TableDataException("Error refreshing location sizes", e);
        }
    }

    private class LocationChangePoller extends InstrumentedUpdateSource {
        private final TableLocationSubscriptionBuffer locationBuffer;

        private LocationChangePoller(@NotNull final TableLocationSubscriptionBuffer locationBuffer) {
            super(updateGraph, description + ".rowSetUpdateSource");
            this.locationBuffer = locationBuffer;
        }

        @Override
        protected void instrumentedRefresh() {
            try {
                final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate = locationBuffer.processPending();
                final ImmutableTableLocationKey[] removedKeys =
                        maybeRemoveLocations(locationUpdate.getPendingRemovedLocationKeys());
                if (removedKeys.length > 0) {
                    throw new TableLocationRemovedException("Source table does not support removed locations",
                            removedKeys);
                }
                maybeAddLocations(locationUpdate.getPendingAddedLocationKeys());

                // NB: This class previously had functionality to notify "location listeners", but it was never used.
                // Resurrect from git history if needed.
                if (!locationSizesInitialized) {
                    // We don't want to start polling size changes until the initial RowSet has been computed.
                    return;
                }

                final RowSet added = refreshLocationSizes(false);
                if (added.isEmpty()) {
                    return;
                }

                rowSet.insert(added);
                notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
            } catch (Exception e) {
                updateSourceRegistrar.removeSource(this);

                // Notify listeners to the SourceTable when we had an issue refreshing available locations.
                notifyListenersOnError(e, null);

                // Notify any listeners to the locations table that we had an error.
                final BaseTable locationTable = (BaseTable) columnSourceManager.locationTable();
                if (locationTable.getLastNotificationStep() == updateGraph.clock().currentStep()) {
                    delayedErrorReference = new DelayedErrorNotifier(e, locationTable);
                } else {
                    locationTable.notifyListenersOnError(e, null);
                    locationTable.forceReferenceCountToZero();
                }
            }
        }
    }

    /**
     * Hook to allow found location keys to be filtered (e.g. according to a where-clause on the partitioning columns of
     * a {@link PartitionAwareSourceTable}. The default implementation returns its input unmolested.
     *
     * @param foundLocationKeys A collection of {@link ImmutableTableLocationKey}s fetched from the
     *        {@link TableLocationProvider}, but not yet incorporated into the table
     * @return A sub-collection of the input
     */
    protected Collection<ImmutableTableLocationKey> filterLocationKeys(
            @NotNull final Collection<ImmutableTableLocationKey> foundLocationKeys) {
        return foundLocationKeys;
    }

    @Override
    protected final QueryTable doCoalesce() {
        initialize();

        final OperationSnapshotControl snapshotControl =
                createSnapshotControlIfRefreshing((final BaseTable<?> parent) -> new OperationSnapshotControl(parent) {

                    @Override
                    public boolean subscribeForUpdates(@NotNull final TableUpdateListener listener) {
                        return addUpdateListenerUncoalesced(listener, lastNotificationStep);
                    }
                });

        final Mutable<QueryTable> result = new MutableObject<>();
        initializeWithSnapshot("SourceTable.coalesce", snapshotControl, (usePrev, beforeClockValue) -> {
            final QueryTable resultTable = new QueryTable(definition, rowSet, columnSourceManager.getColumnSources());
            copyAttributes(resultTable, CopyAttributeOperation.Coalesce);
            if (rowSet.isEmpty()) {
                resultTable.setAttribute(INITIALLY_EMPTY_COALESCED_SOURCE_TABLE_ATTRIBUTE, true);
            }

            if (snapshotControl != null) {
                final ListenerImpl listener =
                        new ListenerImpl("SourceTable.coalesce", this, resultTable) {

                            @Override
                            protected void destroy() {
                                // NB: This implementation cannot call super.destroy() for the same reason as the swap
                                // listener
                                removeUpdateListenerUncoalesced(this);
                            }
                        };
                snapshotControl.setListenerAndResult(listener, resultTable);
            }

            // As part of coalescing, create the RowSet-level data indexes for partitioning and indexed columns.
            final DataIndexer dataIndexer = DataIndexer.of(rowSet);

            final Map<String, ? extends ColumnSource<?>> columnSourceMap = columnSourceManager.getColumnSources();

            // Add the partitioning columns as trivial data indexes
            final TableDefinition tableDefinition = getDefinition();
            for (final ColumnDefinition<?> columnDefinition : tableDefinition.getColumns()) {
                if (columnDefinition.isPartitioning()) {
                    final ColumnSource<?> keySource = columnSourceMap.get(columnDefinition.getName());
                    final DataIndex dataIndex = new PartitioningColumnDataIndexImpl(
                            resultTable,
                            keySource,
                            columnSourceManager,
                            columnDefinition.getName());
                    dataIndexer.addDataIndex(dataIndex);
                }
            }

            // Inspect the locations to see what other data indexes exist.
            final Collection<TableLocation> locations = columnSourceManager.allLocations();
            if (!locations.isEmpty()) {
                // Use the first location as a proxy for the whole table
                final TableLocation firstLocation = locations.iterator().next();

                for (final String[] keyArr : firstLocation.getDataIndexColumns()) {
                    final ColumnSource<?>[] keySources = Arrays.stream(keyArr).map(columnSourceMap::get)
                            .toArray(ColumnSource[]::new);

                    final DataIndex dataIndex = new StorageBackedDataIndexImpl(
                            resultTable,
                            keySources,
                            columnSourceManager,
                            keyArr);
                    dataIndexer.addDataIndex(dataIndex);
                }
            }

            result.setValue(resultTable);
            return true;
        });

        return result.getValue();
    }

    protected static class QueryTableReference extends DeferredViewTable.TableReference {

        protected final SourceTable<?> table;

        QueryTableReference(SourceTable<?> table) {
            super(table);
            this.table = table;
        }

        @Override
        public long getSize() {
            return QueryConstants.NULL_LONG;
        }

        @Override
        public TableDefinition getDefinition() {
            return table.getDefinition();
        }

        @Override
        public Table get() {
            return table.coalesce();
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        if (updateSourceRegistrar != null) {
            if (locationChangePoller != null) {
                updateSourceRegistrar.removeSource(locationChangePoller);
            }
        }
    }

    private static final class DelayedErrorNotifier implements Runnable {

        private final Throwable error;
        private final UpdateGraph updateGraph;
        private final WeakReference<BaseTable<?>> tableReference;

        private DelayedErrorNotifier(@NotNull final Throwable error,
                @NotNull final BaseTable<?> table) {
            this.error = error;
            updateGraph = table.getUpdateGraph();
            tableReference = new WeakReference<>(table);
            updateGraph.addSource(this);
        }

        @Override
        public void run() {
            updateGraph.removeSource(this);

            final BaseTable<?> table = tableReference.get();
            if (table == null) {
                return;
            }

            table.notifyListenersOnError(error, null);
            table.forceReferenceCountToZero();
        }
    }
}
