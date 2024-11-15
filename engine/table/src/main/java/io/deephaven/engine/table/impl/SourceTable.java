//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.locations.TableLocationRemovedException;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.TestUseOnly;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.ArrayList;
import java.util.Collection;

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
    private LocationChangePoller locationChangePoller;

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
        try (final SafeCloseable ignored = isRefreshing ? LivenessScopeStack.open() : null) {
            columnSourceManager = componentFactory.createColumnSourceManager(
                    isRefreshing,
                    ColumnToCodecMappings.EMPTY,
                    definition.getColumns() // This is the *re-written* definition passed to the super-class constructor
            );
            if (isRefreshing) {
                manage(columnSourceManager);
            }
        }

        setRefreshing(isRefreshing);
        // Given the location provider's update modes, retrieve and set applicable table attributes from the CSM
        columnSourceManager.getTableAttributes(
                locationProvider.getUpdateMode(),
                locationProvider.getLocationUpdateMode()).forEach(this::setAttribute);
    }

    /**
     * Force this table to determine its initial state (available locations, size, RowSet) if it hasn't already done so.
     */
    protected final void initialize() {
        initializeAvailableLocations();
        initializeLocationSizes();
    }

    @TestUseOnly
    public final TableLocationProvider tableLocationProvider() {
        return locationProvider;
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

    private void initializeAvailableLocations() {
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
                    manage(locationBuffer);
                    try (final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate =
                            locationBuffer.processPending()) {
                        maybeRemoveLocations(locationUpdate.getPendingRemovedLocationKeys());
                        maybeAddLocations(locationUpdate.getPendingAddedLocationKeys());
                    }
                    updateSourceRegistrar.addSource(locationChangePoller = new LocationChangePoller(locationBuffer));
                } else {
                    locationProvider.refresh();
                    final Collection<LiveSupplier<ImmutableTableLocationKey>> keySuppliers = new ArrayList<>();
                    try {
                        locationProvider.getTableLocationKeys(ttlk -> {
                            // Retain each of the location key suppliers as we see them (since the TLP is not guaranteed
                            // to retain them outside the callback).
                            ttlk.retainReference();
                            keySuppliers.add(ttlk);
                        });
                        maybeAddLocations(keySuppliers);
                    } finally {
                        // Now we can drop the location key supplier references.
                        keySuppliers.forEach(LivenessReferent::dropReference);
                    }
                }
            });
            locationsInitialized = true;
        }
    }

    private void maybeAddLocations(@NotNull final Collection<LiveSupplier<ImmutableTableLocationKey>> locationKeys) {
        if (locationKeys.isEmpty()) {
            return;
        }
        filterLocationKeys(locationKeys)
                .parallelStream()
                .forEach(lk -> columnSourceManager.addLocation(locationProvider.getTableLocation(lk.get())));
    }

    private void maybeRemoveLocations(@NotNull final Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeys) {
        if (removedKeys.isEmpty()) {
            return;
        }

        filterLocationKeys(removedKeys).stream()
                .map(LiveSupplier::get)
                .forEach(columnSourceManager::removeLocationKey);
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
                        try {
                            rowSet = columnSourceManager.initialize();
                        } catch (Exception e) {
                            throw new TableDataException("Error initializing location sizes", e);
                        }
                        if (!isRefreshing()) {
                            return;
                        }
                        initializeLastNotificationStep(getUpdateGraph().clock());
                    });
            locationSizesInitialized = true;
        }
    }

    private class LocationChangePoller extends InstrumentedTableUpdateSource {

        private final TableLocationSubscriptionBuffer locationBuffer;

        private LocationChangePoller(@NotNull final TableLocationSubscriptionBuffer locationBuffer) {
            super(SourceTable.this.updateSourceRegistrar, SourceTable.this, description + ".rowSetUpdateSource");
            this.locationBuffer = locationBuffer;
        }

        @Override
        protected void instrumentedRefresh() {
            try (final TableLocationSubscriptionBuffer.LocationUpdate locationUpdate =
                    locationBuffer.processPending()) {
                if (!locationProvider.getUpdateMode().removeAllowed()
                        && !locationUpdate.getPendingRemovedLocationKeys().isEmpty()) {
                    // This TLP doesn't support removed locations, we need to throw an exception.
                    final ImmutableTableLocationKey[] keys = locationUpdate.getPendingRemovedLocationKeys().stream()
                            .map(LiveSupplier::get).toArray(ImmutableTableLocationKey[]::new);
                    throw new TableLocationRemovedException("Source table does not support removed locations", keys);
                }

                maybeRemoveLocations(locationUpdate.getPendingRemovedLocationKeys());
                maybeAddLocations(locationUpdate.getPendingAddedLocationKeys());
            }

            // This class previously had functionality to notify "location listeners", but it was never used.
            // Resurrect from git history if needed.
            if (!locationSizesInitialized) {
                // We don't want to start polling size changes until the initial RowSet has been computed.
                return;
            }

            final TableUpdate update = columnSourceManager.refresh();
            if (update.empty()) {
                update.release();
                return;
            }

            Assert.assertion(update.shifted().empty(), "update.shifted().empty()");
            rowSet.remove(update.removed());
            rowSet.insert(update.added());
            notifyListeners(update);
        }

        @Override
        protected void onRefreshError(@NotNull final Exception error) {
            super.onRefreshError(error);
            // Be sure that the ColumnSourceManager is aware
            columnSourceManager.deliverError(error, entry);
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
    protected Collection<LiveSupplier<ImmutableTableLocationKey>> filterLocationKeys(
            @NotNull final Collection<LiveSupplier<ImmutableTableLocationKey>> foundLocationKeys) {
        return foundLocationKeys;
    }

    @Override
    protected final QueryTable doCoalesce() {
        initialize();

        final OperationSnapshotControl snapshotControl =
                createSnapshotControlIfRefreshing((final BaseTable<?> parent) -> new OperationSnapshotControl(parent) {

                    @Override
                    public boolean subscribeForUpdates(@NotNull final TableUpdateListener listener) {
                        // This impl cannot call super.subscribeForUpdates(), because we must subscribe to the actual
                        // (uncoalesced) SourceTable.
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
                // noinspection MethodDoesntCallSuperMethod
                final ListenerImpl listener =
                        new ListenerImpl("SourceTable.coalesce", this, resultTable) {

                            @OverridingMethodsMustInvokeSuper
                            @Override
                            protected void destroy() {
                                // This impl cannot call super.destroy() because we must unsubscribe from the actual
                                // (uncoalesced) SourceTable.
                                removeUpdateListenerUncoalesced(this);
                            }
                        };
                snapshotControl.setListenerAndResult(listener, resultTable);
            }

            result.setValue(resultTable);
            return true;
        });

        return result.getValue();
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();
        if (updateSourceRegistrar != null) {
            if (locationChangePoller != null) {
                updateSourceRegistrar.removeSource(locationChangePoller);
                // NB: we do not want to null out any locationChangePoller.locationBuffer here, as they may still be in
                // use by a notification delivery running currently with this destroy.
                locationChangePoller.locationBuffer.reset();
            }
        }
    }
}
