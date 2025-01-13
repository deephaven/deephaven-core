//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Require;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.table.impl.locations.ImmutableTableLocationKey;
import io.deephaven.engine.table.impl.locations.TableDataException;
import io.deephaven.engine.table.impl.locations.TableLocationProvider;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Intermediates between push-based subscription to a TableLocationProvider and polling on update source refresh.
 */
public class TableLocationSubscriptionBuffer extends ReferenceCountedLivenessNode
        implements TableLocationProvider.Listener {

    private static final Map<ImmutableTableLocationKey, LiveSupplier<ImmutableTableLocationKey>> EMPTY_TABLE_LOCATION_KEYS =
            Collections.emptyMap();

    private final TableLocationProvider tableLocationProvider;

    private boolean subscribed = false;

    private final Object updateLock = new Object();

    private LocationUpdate pendingUpdate = null;
    private TableDataException pendingException = null;

    public TableLocationSubscriptionBuffer(@NotNull final TableLocationProvider tableLocationProvider) {
        super(false);
        this.tableLocationProvider = Require.neqNull(tableLocationProvider, "tableLocationProvider");
    }

    public final class LocationUpdate implements SafeCloseable {

        private final ReferenceCountedLivenessNode livenessNode = new ReferenceCountedLivenessNode(false) {};

        // These sets represent adds and removes from completed transactions.
        private Map<ImmutableTableLocationKey, LiveSupplier<ImmutableTableLocationKey>> added =
                EMPTY_TABLE_LOCATION_KEYS;
        private Map<ImmutableTableLocationKey, LiveSupplier<ImmutableTableLocationKey>> removed =
                EMPTY_TABLE_LOCATION_KEYS;

        private LocationUpdate() {
            TableLocationSubscriptionBuffer.this.manage(livenessNode);
        }

        private void processAdd(@NotNull final LiveSupplier<ImmutableTableLocationKey> addedKeySupplier) {
            final ImmutableTableLocationKey addedKey = addedKeySupplier.get();
            // Note that we might have a remove for this key if it previously existed and is being replaced. Hence, we
            // don't look for an existing remove, which is apparently asymmetric w.r.t. processRemove but still correct.
            // Consumers of a LocationUpdate must process removes before adds.

            // Need to verify that we don't have stacked adds (without intervening removes).
            if (added.containsKey(addedKey)) {
                throw new IllegalStateException("TableLocationKey " + addedKey
                        + " was already added by a previous transaction.");
            }
            if (added == EMPTY_TABLE_LOCATION_KEYS) {
                added = new HashMap<>();
            }
            livenessNode.manage(addedKeySupplier);
            added.put(addedKey, addedKeySupplier);
        }

        private void processRemove(@NotNull final LiveSupplier<ImmutableTableLocationKey> removedKeySupplier) {
            final ImmutableTableLocationKey removedKey = removedKeySupplier.get();
            // If we have a pending add, it is being cancelled by this remove.
            if (added.remove(removedKey) != null) {
                return;
            }
            // Verify that we don't have stacked removes (without intervening adds).
            if (removed.containsKey(removedKey)) {
                throw new IllegalStateException("TableLocationKey " + removedKey
                        + " was already removed and has not been replaced.");
            }
            if (removed == EMPTY_TABLE_LOCATION_KEYS) {
                removed = new HashMap<>();
            }
            livenessNode.manage(removedKeySupplier);
            removed.put(removedKey, removedKeySupplier);
        }

        private void processTransaction(
                @Nullable Collection<LiveSupplier<ImmutableTableLocationKey>> addedKeySuppliers,
                @Nullable Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeySuppliers) {
            if (removedKeySuppliers != null) {
                for (final LiveSupplier<ImmutableTableLocationKey> removedKeySupplier : removedKeySuppliers) {
                    processRemove(removedKeySupplier);
                }
            }
            if (addedKeySuppliers != null) {
                for (final LiveSupplier<ImmutableTableLocationKey> addedKeySupplier : addedKeySuppliers) {
                    processAdd(addedKeySupplier);
                }
            }
        }

        /**
         * @return The pending location keys to add. <em>Note that removes should be processed before adds.</em>
         */
        public Collection<LiveSupplier<ImmutableTableLocationKey>> getPendingAddedLocationKeys() {
            return added.values();
        }

        /**
         * @return The pending location keys to remove. <em>Note that removes should be processed before adds.</em>
         */
        public Collection<LiveSupplier<ImmutableTableLocationKey>> getPendingRemovedLocationKeys() {
            return removed.values();
        }

        @Override
        public void close() {
            TableLocationSubscriptionBuffer.this.unmanage(livenessNode);
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
        if (!subscribed) {
            if (tableLocationProvider.supportsSubscriptions()) {
                tableLocationProvider.subscribe(this);
            } else {
                // NB: Providers that don't support subscriptions don't tick - this single call to refresh is
                // sufficient.
                tableLocationProvider.refresh();
                final Collection<LiveSupplier<ImmutableTableLocationKey>> tableLocationKeys = new ArrayList<>();
                tableLocationProvider.getTableLocationKeys(tableLocationKeys::add);
                handleTableLocationKeysUpdate(tableLocationKeys, List.of());
            }
            subscribed = true;
        }
        final LocationUpdate resultUpdate;
        final TableDataException resultException;
        synchronized (updateLock) {
            resultUpdate = pendingUpdate;
            pendingUpdate = null;
            resultException = pendingException;
            pendingException = null;
        }

        if (resultException != null) {
            try (final SafeCloseable ignored = resultUpdate) {
                throw new TableDataException("Processed pending exception", resultException);
            }
        }
        return resultUpdate;
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
        final LocationUpdate toClose;
        synchronized (updateLock) {
            toClose = pendingUpdate;
            pendingUpdate = null;
            pendingException = null;
        }
        if (toClose != null) {
            toClose.close();
        }
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider.Listener implementation
    // ------------------------------------------------------------------------------------------------------------------

    private LocationUpdate ensurePendingUpdate() {
        if (pendingUpdate == null) {
            pendingUpdate = new LocationUpdate();
        }
        return pendingUpdate;
    }

    @Override
    public void handleTableLocationKeyAdded(@NotNull final LiveSupplier<ImmutableTableLocationKey> addedKeySupplier) {
        synchronized (updateLock) {
            // noinspection resource
            ensurePendingUpdate().processAdd(addedKeySupplier);
        }
    }

    @Override
    public void handleTableLocationKeyRemoved(
            @NotNull final LiveSupplier<ImmutableTableLocationKey> removedKeySupplier) {
        synchronized (updateLock) {
            // noinspection resource
            ensurePendingUpdate().processRemove(removedKeySupplier);
        }
    }

    @Override
    public void handleTableLocationKeysUpdate(
            @Nullable Collection<LiveSupplier<ImmutableTableLocationKey>> addedKeySuppliers,
            @Nullable Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeySuppliers) {
        synchronized (updateLock) {
            // noinspection resource
            ensurePendingUpdate().processTransaction(addedKeySuppliers, removedKeySuppliers);
        }
    }

    @Override
    public void handleException(@NotNull final TableDataException exception) {
        synchronized (updateLock) {
            pendingException = exception;
        }
    }
}
