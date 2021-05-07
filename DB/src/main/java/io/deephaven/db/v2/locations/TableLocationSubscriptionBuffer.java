/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Intermediates between push-based subscription to a TableLocationProvider and polling on LiveTable refresh.
 */
public class TableLocationSubscriptionBuffer implements TableLocationProvider.Listener {

    private static final Set<TableLocation> EMPTY_TABLE_LOCATIONS = Collections.emptySet();

    private final TableLocationProvider tableLocationProvider;

    private boolean subscribed = false;

    private final Object updateLock = new Object();
    private Set<TableLocation> pendingLocations = EMPTY_TABLE_LOCATIONS;
    private TableDataException pendingException = null;

    public TableLocationSubscriptionBuffer(@NotNull final TableLocationProvider tableLocationProvider) {
        this.tableLocationProvider = Require.neqNull(tableLocationProvider, "tableLocationProvider");
    }

    /**
     * Subscribe if needed, and return any pending locations (or throw a pending exception) from the table location
     * provider.
     * A given location will only be returned by a single call to processPending() (unless state is reset).
     * No order is maintained internally.
     * If a pending exception is thrown, this signals that the subscription is no longer valid and no
     * subsequent locations will be returned.
     *
     * @return The collection of pending locations
     */
    public synchronized Collection<TableLocation> processPending() {
        // TODO: Should I change this to instead re-use the collection?
        if (!subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.subscribe(this);
            } else {
                // NB: Providers that don't support subscriptions don't tick - this single call to refresh is sufficient.
                tableLocationProvider.refresh();
                tableLocationProvider.getTableLocations().forEach(this::handleTableLocation);
            }
            subscribed = true;
        }
        final Collection<TableLocation> resultLocations;
        final TableDataException resultException;
        synchronized (updateLock) {
            resultLocations = pendingLocations;
            pendingLocations = EMPTY_TABLE_LOCATIONS;
            resultException = pendingException;
            pendingException = null;
        }
        if (resultException != null) {
            throw new TableDataException("Processed pending exception", resultException);
        }
        return resultLocations;
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
            pendingLocations = EMPTY_TABLE_LOCATIONS;
            pendingException = null;
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider.Listener implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    public void handleTableLocation(@NotNull final TableLocation tableLocation) {
        synchronized (updateLock) {
            if (pendingLocations == EMPTY_TABLE_LOCATIONS) {
                pendingLocations = new HashSet<>();
            }
            pendingLocations.add(tableLocation);
        }
    }

    @Override
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}
