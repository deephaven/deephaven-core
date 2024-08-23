//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Intermediates between push-based subscription to a TableLocationProvider and polling on update source refresh.
 */
public class TableLocationSubscriptionBuffer implements TableLocationProvider.Listener {

    private static final Set<ImmutableTableLocationKey> EMPTY_TABLE_LOCATION_KEYS = Collections.emptySet();

    private final TableLocationProvider tableLocationProvider;

    private boolean subscribed = false;

    private final Object updateLock = new Object();

    // These sets represent adds and removes from completed transactions.
    private Set<ImmutableTableLocationKey> pendingLocationsAdded = EMPTY_TABLE_LOCATION_KEYS;
    private Set<ImmutableTableLocationKey> pendingLocationsRemoved = EMPTY_TABLE_LOCATION_KEYS;

    private TableDataException pendingException = null;

    public TableLocationSubscriptionBuffer(@NotNull final TableLocationProvider tableLocationProvider) {
        this.tableLocationProvider = Require.neqNull(tableLocationProvider, "tableLocationProvider");
    }

    public static final class LocationUpdate {
        private final Collection<ImmutableTableLocationKey> pendingAddedLocationKeys;
        private final Collection<ImmutableTableLocationKey> pendingRemovedLocations;

        public LocationUpdate(@NotNull final Collection<ImmutableTableLocationKey> pendingAddedLocationKeys,
                @NotNull final Collection<ImmutableTableLocationKey> pendingRemovedLocations) {
            this.pendingAddedLocationKeys = pendingAddedLocationKeys;
            this.pendingRemovedLocations = pendingRemovedLocations;
        }

        public Collection<ImmutableTableLocationKey> getPendingAddedLocationKeys() {
            return pendingAddedLocationKeys;
        }

        public Collection<ImmutableTableLocationKey> getPendingRemovedLocationKeys() {
            return pendingRemovedLocations;
        }
    }

    /**
     * Subscribe if needed, and return any pending location keys (or throw a pending exception) from the table location
     * provider. A given location key will only be returned by a single call to processPending() (unless state is
     * reset). No order is maintained internally. If a pending exception is thrown, this signals that the subscription
     * is no longer valid and no subsequent location keys will be returned.
     *
     * @return The collection of pending location keys.
     */
    public synchronized LocationUpdate processPending() {
        // TODO: Should I change this to instead re-use the collection?
        if (!subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.subscribe(this);
            } else {
                // NB: Providers that don't support subscriptions don't tick - this single call to run is
                // sufficient.
                tableLocationProvider.refresh();
                handleTableLocationKeysUpdate(tableLocationProvider.getTableLocationKeys(), null);
            }
            subscribed = true;
        }
        final Collection<ImmutableTableLocationKey> resultLocationKeys;
        final Collection<ImmutableTableLocationKey> resultLocationsRemoved;
        final TableDataException resultException;
        synchronized (updateLock) {
            resultLocationKeys = pendingLocationsAdded;
            pendingLocationsAdded = EMPTY_TABLE_LOCATION_KEYS;
            resultLocationsRemoved = pendingLocationsRemoved;
            pendingLocationsRemoved = EMPTY_TABLE_LOCATION_KEYS;
            resultException = pendingException;
            pendingException = null;
        }

        if (resultException != null) {
            throw new TableDataException("Processed pending exception", resultException);
        }

        return new LocationUpdate(resultLocationKeys, resultLocationsRemoved);
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
            pendingLocationsAdded = EMPTY_TABLE_LOCATION_KEYS;
            pendingLocationsRemoved = EMPTY_TABLE_LOCATION_KEYS;
            pendingException = null;
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider.Listener implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    public void beginTransaction(@NotNull final Object token) {
        throw new UnsupportedOperationException("Transactions are not supported by this provider.");
    }

    @Override
    public void endTransaction(@NotNull final Object token) {
        throw new UnsupportedOperationException("Transactions are not supported by this provider.");
    }

    @Override
    public void handleTableLocationKeyAdded(
            @NotNull final ImmutableTableLocationKey tableLocationKey,
            @Nullable Object transactionToken) {
        synchronized (updateLock) {
            if (transactionToken != null) {
                throw new UnsupportedOperationException("Transactions are not supported by this provider.");
            }

            // Need to verify that we don't have stacked adds (without intervening removes).
            if (pendingLocationsAdded.contains(tableLocationKey)) {
                throw new IllegalStateException("TableLocationKey " + tableLocationKey
                        + " was already added by a previous transaction.");
            }
            if (pendingLocationsAdded == EMPTY_TABLE_LOCATION_KEYS) {
                pendingLocationsAdded = new HashSet<>();
            }
            pendingLocationsAdded.add(tableLocationKey);
        }
    }

    @Override
    public void handleTableLocationKeyRemoved(
            @NotNull final ImmutableTableLocationKey tableLocationKey,
            final Object transactionToken) {
        synchronized (updateLock) {
            if (transactionToken != null) {
                throw new UnsupportedOperationException("Transactions are not supported by this provider.");
            }

            // If we have a pending add, it is being cancelled by this remove.
            if (pendingLocationsAdded.remove(tableLocationKey)) {
                return;
            }
            // Verify that we don't have stacked removes (without intervening adds).
            if (pendingLocationsRemoved.contains(tableLocationKey)) {
                throw new IllegalStateException("TableLocationKey " + tableLocationKey
                        + " was already removed by a previous transaction.");
            }
            if (pendingLocationsRemoved == EMPTY_TABLE_LOCATION_KEYS) {
                pendingLocationsRemoved = new HashSet<>();
            }
            pendingLocationsRemoved.add(tableLocationKey);
        }
    }

    @Override
    public void handleTableLocationKeysUpdate(
            @Nullable Collection<ImmutableTableLocationKey> addedKeys,
            @Nullable Collection<ImmutableTableLocationKey> removedKeys) {
        synchronized (updateLock) {
            if (removedKeys != null) {
                for (final ImmutableTableLocationKey removedTableLocationKey : removedKeys) {
                    // If we have a pending add, it is being cancelled by this remove.
                    if (pendingLocationsAdded.remove(removedTableLocationKey)) {
                        continue;
                    }
                    // Verify that we don't have stacked removes.
                    if (pendingLocationsRemoved.contains(removedTableLocationKey)) {
                        throw new IllegalStateException("TableLocationKey " + removedTableLocationKey
                                + " was already removed by a previous transaction.");
                    }
                    if (pendingLocationsRemoved == EMPTY_TABLE_LOCATION_KEYS) {
                        pendingLocationsRemoved = new HashSet<>();
                    }
                    pendingLocationsRemoved.add(removedTableLocationKey);
                }
            }
            if (addedKeys != null) {
                for (final ImmutableTableLocationKey addedTableLocationKey : addedKeys) {
                    // Need to verify that we don't have stacked adds.
                    if (pendingLocationsAdded.contains(addedTableLocationKey)) {
                        throw new IllegalStateException("TableLocationKey " + addedTableLocationKey
                                + " was already added by a previous transaction.");
                    }
                    if (pendingLocationsAdded == EMPTY_TABLE_LOCATION_KEYS) {
                        pendingLocationsAdded = new HashSet<>();
                    }
                    pendingLocationsAdded.add(addedTableLocationKey);
                }
            }
        }
    }

    @Override
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}
