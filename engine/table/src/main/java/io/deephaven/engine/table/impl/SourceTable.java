/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.updategraph.UpdateSourceRegistrar;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.engine.table.impl.locations.impl.TableLocationSubscriptionBuffer;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.TestUseOnly;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Basic uncoalesced table that only adds keys.
 */
public abstract class SourceTable extends RedefinableTable {

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
        if (isRefreshing) {
            // NB: There's no reason to start out trying to group, if this is a refreshing table.
            columnSourceManager.disableGrouping();
        }

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
                    maybeAddLocations(locationBuffer.processPending());
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
                        rowSet = refreshLocationSizes().toTracking();
                        if (!isRefreshing()) {
                            return;
                        }
                        rowSet.initializePreviousValue();
                        final long currentClockValue = LogicalClock.DEFAULT.currentValue();
                        setLastNotificationStep(LogicalClock.getState(currentClockValue) == LogicalClock.State.Updating
                                ? LogicalClock.getStep(currentClockValue) - 1
                                : LogicalClock.getStep(currentClockValue));
                    });
            locationSizesInitialized = true;
        }
    }

    private WritableRowSet refreshLocationSizes() {
        try {
            return columnSourceManager.refresh();
        } catch (Exception e) {
            throw new TableDataException("Error refreshing location sizes", e);
        }
    }

    private class LocationChangePoller extends InstrumentedUpdateSource {

        private final TableLocationSubscriptionBuffer locationBuffer;

        private LocationChangePoller(@NotNull final TableLocationSubscriptionBuffer locationBuffer) {
            super(description + ".rowSetUpdateSource");
            this.locationBuffer = locationBuffer;
        }

        @Override
        protected void instrumentedRefresh() {
            try {
                maybeAddLocations(locationBuffer.processPending());
                // NB: This class previously had functionality to notify "location listeners", but it was never used.
                // Resurrect from git history if needed.
                if (!locationSizesInitialized) {
                    // We don't want to start polling size changes until the initial RowSet has been computed.
                    return;
                }
                final RowSet added = refreshLocationSizes();
                if (added.size() == 0) {
                    return;
                }
                rowSet.insert(added);
                notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
            } catch (Exception e) {
                // Notify listeners to the SourceTable when we had an issue refreshing available locations.
                notifyListenersOnError(e, null);
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

        final SwapListener swapListener =
                createSwapListenerIfRefreshing((final BaseTable parent) -> new SwapListener(parent) {

                    @Override
                    public void destroy() {
                        // NB: We can't call super.destroy() because we don't want to try to remove ourselves from the
                        // coalesced table (see override for removeUpdateListener), but we are probably not missing
                        // anything by not having super.destroy() invoke its own super.destroy().
                        removeUpdateListenerUncoalesced(this);
                    }

                    @Override
                    public void subscribeForUpdates() {
                        addUpdateListenerUncoalesced(this);
                    }
                });

        final Mutable<QueryTable> result = new MutableObject<>();
        initializeWithSnapshot("SourceTable.coalesce", swapListener, (usePrev, beforeClockValue) -> {
            final QueryTable resultTable = new QueryTable(definition, rowSet, columnSourceManager.getColumnSources());
            copyAttributes(resultTable, CopyAttributeOperation.Coalesce);

            if (swapListener != null) {
                final ListenerImpl listener =
                        new ListenerImpl("SourceTable.coalesce", this, resultTable) {

                            @Override
                            protected void destroy() {
                                // NB: This implementation cannot call super.destroy() for the same reason as the swap
                                // listener
                                removeUpdateListenerUncoalesced(this);
                            }
                        };
                swapListener.setListenerAndResult(listener, resultTable);
            }

            result.setValue(resultTable);
            return true;
        });

        return result.getValue();
    }

    protected static class QueryTableReference extends DeferredViewTable.TableReference {

        protected final SourceTable table;

        QueryTableReference(SourceTable table) {
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
}
