/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Intermediates between push-based subscription to a TableLocationProvider and polling on update source refresh.
 */
public class TableLocationSubscriptionBuffer implements TableLocationProvider.Listener {

    private static final Set<ImmutableTableLocationKey> EMPTY_TABLE_LOCATION_KEYS = Collections.emptySet();
    private static final Set<TableLocation> EMPTY_TABLE_LOCATIONS = Collections.emptySet();

    private final TableLocationProvider tableLocationProvider;

    private boolean subscribed = false;

    private final Object updateLock = new Object();
    private Set<ImmutableTableLocationKey> pendingLocationKeys = EMPTY_TABLE_LOCATION_KEYS;

    private Set<TableLocation> pendingLocationsRemoved = EMPTY_TABLE_LOCATIONS;
    private TableDataException pendingException = null;

    public TableLocationSubscriptionBuffer(@NotNull final TableLocationProvider tableLocationProvider) {
        this.tableLocationProvider = Require.neqNull(tableLocationProvider, "tableLocationProvider");
    }

    /**
     * Subscribe if needed, and return any pending location keys (or throw a pending exception) from the table location
     * provider. A given location key will only be returned by a single call to processPending() (unless state is
     * reset). No order is maintained internally. If a pending exception is thrown, this signals that the subscription
     * is no longer valid and no subsequent location keys will be returned.
     *
     * @return The collection of pending location keys
     */
    public synchronized Collection<ImmutableTableLocationKey> processPending() {
        // TODO: Should I change this to instead re-use the collection?
        if (!subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.subscribe(this);
            } else {
                // NB: Providers that don't support subscriptions don't tick - this single call to run is
                // sufficient.
                tableLocationProvider.refresh();
                tableLocationProvider.getTableLocationKeys().forEach(this::handleTableLocationKey);
            }
            subscribed = true;
        }
        final Collection<ImmutableTableLocationKey> resultLocationKeys;
        final Collection<TableLocation> resultLocationsRemoved;
        final TableDataException resultException;
        synchronized (updateLock) {
            resultLocationKeys = pendingLocationKeys;
            pendingLocationKeys = EMPTY_TABLE_LOCATION_KEYS;
            resultLocationsRemoved = pendingLocationsRemoved;
            pendingLocationsRemoved = EMPTY_TABLE_LOCATIONS;
            resultException = pendingException;
            pendingException = null;
        }

        // TODO: Maybe we should combine these into a single exception -- or even better set the pending exception in
        //       handleRemoved, since this is not allowed in the first place.
        if (resultException != null) {
            throw new TableDataException("Processed pending exception", resultException);
        }

        if(!pendingLocationsRemoved.isEmpty()) {
            throw new TableDataException("Removed TableLocations are not handled in TableLocationSubscriptionBuffer: " + pendingLocationsRemoved);
        }
        return resultLocationKeys;
    }

    /**
     * Unsubscribe and clear any state pending processing.
     */
    public synchronized void reset() {
        if (subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.unsubscribe(this);
            }
            subscribed = false;
        }
        synchronized (updateLock) {
            pendingLocationKeys = EMPTY_TABLE_LOCATION_KEYS;
            pendingLocationsRemoved = EMPTY_TABLE_LOCATIONS;
            pendingException = null;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider.Listener implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void handleTableLocationKey(@NotNull final ImmutableTableLocationKey tableLocationKey) {
        synchronized (updateLock) {
            if (pendingLocationKeys == EMPTY_TABLE_LOCATION_KEYS) {
                pendingLocationKeys = new HashSet<>();
            }
            pendingLocationKeys.add(tableLocationKey);
        }
    }

    @Override
    public void handleTableLocationRemoved(@NotNull TableLocation tableLocation) {
        synchronized (updateLock) {
            if (pendingLocationsRemoved == EMPTY_TABLE_LOCATIONS) {
                pendingLocationsRemoved = new HashSet<>();
            }
            pendingLocationsRemoved.add(tableLocation);
        }
    }

    @Override
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}
