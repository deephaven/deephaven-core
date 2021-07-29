/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.db.v2.locations.ImmutableTableLocationKey;
import io.deephaven.db.v2.locations.TableDataException;
import io.deephaven.db.v2.locations.TableLocationProvider;
import org.jetbrains.annotations.NotNull;

import java.util.*;

/**
 * Intermediates between push-based subscription to a TableLocationProvider and polling on LiveTable refresh.
 */
public class TableLocationSubscriptionBuffer implements TableLocationProvider.Listener {

    private static final Set<ImmutableTableLocationKey> EMPTY_TABLE_LOCATION_KEYS = Collections.emptySet();

    private final TableLocationProvider tableLocationProvider;

    private boolean subscribed = false;

    private final Object updateLock = new Object();
    private Set<ImmutableTableLocationKey> pendingLocationKeys = EMPTY_TABLE_LOCATION_KEYS;
    private TableDataException pendingException = null;

    public TableLocationSubscriptionBuffer(@NotNull final TableLocationProvider tableLocationProvider) {
        this.tableLocationProvider = Require.neqNull(tableLocationProvider, "tableLocationProvider");
    }

    /**
     * Subscribe if needed, and return any pending location keys (or throw a pending exception) from the table location
     * provider.
     * A given location key will only be returned by a single call to processPending() (unless state is reset).
     * No order is maintained internally.
     * If a pending exception is thrown, this signals that the subscription is no longer valid and no
     * subsequent location keys will be returned.
     *
     * @return The collection of pending location keys
     */
    public synchronized Collection<ImmutableTableLocationKey> processPending() {
        // TODO: Should I change this to instead re-use the collection?
        if (!subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.subscribe(this);
            } else {
                // NB: Providers that don't support subscriptions don't tick - this single call to refresh is sufficient.
                tableLocationProvider.refresh();
                tableLocationProvider.getTableLocationKeys().forEach(this::handleTableLocationKey);
            }
            subscribed = true;
        }
        final Collection<ImmutableTableLocationKey> resultLocationKeys;
        final TableDataException resultException;
        synchronized (updateLock) {
            resultLocationKeys = pendingLocationKeys;
            pendingLocationKeys = EMPTY_TABLE_LOCATION_KEYS;
            resultException = pendingException;
            pendingException = null;
        }
        if (resultException != null) {
            throw new TableDataException("Processed pending exception", resultException);
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
            pendingException = null;
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider.Listener implementation
    //------------------------------------------------------------------------------------------------------------------

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
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}
