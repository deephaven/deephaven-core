//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Assert;
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

    // These sets represent open transactions that are being accumulated.
    private final Set<Object> transactionTokens = new HashSet<>();
    private final Map<Object, Set<ImmutableTableLocationKey>> accumulatedLocationsAdded = new HashMap<>();
    private final Map<Object, Set<ImmutableTableLocationKey>> accumulatedLocationsRemoved = new HashMap<>();

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
                // TODO: cast this to AbstractTableLocationProvider and call begin/endTransaction?
                tableLocationProvider.getTableLocationKeys()
                        .forEach(tlk -> handleTableLocationKeyAdded(tlk, null));
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
        synchronized (updateLock) {
            // Verify that we can start a new transaction with this token.
            if (transactionTokens.contains(token)) {
                throw new IllegalStateException("A transaction with token " + token + " is currently open.");
            }
            Assert.eqFalse(accumulatedLocationsAdded.containsKey(token),
                    "accumulatedLocationsAdded.containsKey(token)");
            Assert.eqFalse(accumulatedLocationsRemoved.containsKey(token),
                    "accumulatedLocationsRemoved.containsKey(token)");

            transactionTokens.add(token);
            accumulatedLocationsAdded.put(token, EMPTY_TABLE_LOCATION_KEYS);
            accumulatedLocationsRemoved.put(token, EMPTY_TABLE_LOCATION_KEYS);
        }
    }

    @Override
    public void endTransaction(@NotNull final Object token) {
        synchronized (updateLock) {
            // Verify that this transaction is open.
            if (!transactionTokens.remove(token)) {
                throw new IllegalStateException("No transaction with token " + token + " is currently open.");
            }

            final Set<ImmutableTableLocationKey> tokenLocationsAdded = accumulatedLocationsAdded.remove(token);
            final Set<ImmutableTableLocationKey> tokenLocationsRemoved = accumulatedLocationsRemoved.remove(token);

            if (tokenLocationsRemoved != EMPTY_TABLE_LOCATION_KEYS) {
                for (final ImmutableTableLocationKey tableLocationKey : tokenLocationsRemoved) {
                    // If we have a pending add that is removed by this transaction, we can remove it from the pending
                    // list because it is cancelled by this remove. This also covers the case where a `replace`
                    // operation has occurred in a previous transaction.
                    if (pendingLocationsAdded.remove(tableLocationKey)) {
                        continue;
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

            if (tokenLocationsAdded != EMPTY_TABLE_LOCATION_KEYS) {
                for (final ImmutableTableLocationKey tableLocationKey : tokenLocationsAdded) {
                    // Verify that we don't have stacked adds (without intervening removes).
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
        }
    }

    @Override
    public void handleTableLocationKeyAdded(
            @NotNull final ImmutableTableLocationKey tableLocationKey,
            @Nullable Object transactionToken) {
        synchronized (updateLock) {
            // When adding a location in a transaction, check for logical consistency.
            // 1. If the location was already added in this transaction, we have a problem. A transaction should not
            // add the same location twice.
            // 2. If the location was already removed in this transaction, we have a `replace` operation which is not a
            // logical error (although it may not be supported by all consumers).

            if (transactionToken == null) {
                // If we're not in a transaction, modify the pending locations directly.
                // Need to verify that we don't have stacked adds (without intervening removes).
                if (pendingLocationsAdded.contains(tableLocationKey)) {
                    throw new IllegalStateException("TableLocationKey " + tableLocationKey
                            + " was already added by a previous transaction.");
                }
                if (pendingLocationsAdded == EMPTY_TABLE_LOCATION_KEYS) {
                    pendingLocationsAdded = new HashSet<>();
                }
                pendingLocationsAdded.add(tableLocationKey);
                return;
            }

            if (accumulatedLocationsAdded.get(transactionToken) == EMPTY_TABLE_LOCATION_KEYS) {
                accumulatedLocationsAdded.put(transactionToken, new HashSet<>());
            }
            final Set<ImmutableTableLocationKey> locationsAdded = accumulatedLocationsAdded.get(transactionToken);

            if (accumulatedLocationsAdded.containsKey(tableLocationKey)) {
                throw new IllegalStateException("TableLocationKey " + tableLocationKey
                        + " was added multiple times in the same transaction.");
            }
            locationsAdded.add(tableLocationKey);
        }
    }

    @Override
    public void handleTableLocationKeyRemoved(
            @NotNull final ImmutableTableLocationKey tableLocationKey,
            final Object transactionToken) {
        synchronized (updateLock) {
            // When removing a location in a transaction, check for logical consistency.
            // 1. If the location was already removed in this transaction, we have a problem. A transaction should not
            // remove the same location twice.
            // 2. If the location was already added in this transaction, we have a problem. A transaction should not
            // add then remove the same location.

            if (transactionToken == null) {
                // If we're not in a transaction, modify the pending locations directly.
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
                return;
            }

            if (accumulatedLocationsRemoved.get(transactionToken) == EMPTY_TABLE_LOCATION_KEYS) {
                accumulatedLocationsRemoved.put(transactionToken, new HashSet<>());
            }
            final Set<ImmutableTableLocationKey> locationsRemoved = accumulatedLocationsRemoved.get(transactionToken);

            if (accumulatedLocationsRemoved.containsKey(tableLocationKey)) {
                throw new IllegalStateException("TableLocationKey " + tableLocationKey
                        + " was removed multiple times in the same transaction.");
            } else if (accumulatedLocationsAdded.containsKey(tableLocationKey)) {
                throw new IllegalStateException("TableLocationKey " + tableLocationKey
                        + " was removed after being added in the same transaction.");
            }
            locationsRemoved.add(tableLocationKey);
        }
    }

    @Override
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}
