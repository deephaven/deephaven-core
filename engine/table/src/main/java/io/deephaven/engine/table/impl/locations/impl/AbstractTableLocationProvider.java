//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.DelegatingLivenessNode;
import io.deephaven.engine.liveness.LivenessNode;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectHashSet;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Partial {@link TableLocationProvider} implementation for standalone use or as part of a {@link TableDataService}.
 * <p>
 * Presents an interface similar to {@link TableLocationProvider.Listener} for subclasses to use when communicating with
 * the parent; see {@link #handleTableLocationKeyAdded(TableLocationKey, Object)}.
 * <p>
 * Note that subclasses are responsible for determining when it's appropriate to call {@link #setInitialized()} and/or
 * override {@link #doInitialization()}.
 */
public abstract class AbstractTableLocationProvider
        extends SubscriptionAggregator<TableLocationProvider.Listener>
        implements TableLocationProvider, DelegatingLivenessNode {

    private static final Set<ImmutableTableLocationKey> EMPTY_TABLE_LOCATION_KEYS = Collections.emptySet();

    /**
     * Helper class to manage a transaction of added and removed location keys.
     */
    private static class Transaction {
        Set<ImmutableTableLocationKey> locationsAdded = EMPTY_TABLE_LOCATION_KEYS;
        Set<ImmutableTableLocationKey> locationsRemoved = EMPTY_TABLE_LOCATION_KEYS;

        synchronized void addLocationKey(ImmutableTableLocationKey locationKey) {
            if (locationsAdded == EMPTY_TABLE_LOCATION_KEYS) {
                locationsAdded = new HashSet<>();
            }
            // When adding a location in a transaction, check for logical consistency.
            // 1. If the location was already added in this transaction, we have a problem. A transaction should not
            // add the same location twice.
            // 2. If the location was already removed in this transaction, we have a `replace` operation which is not a
            // logical error (although it may not be supported by all consumers).
            if (!locationsAdded.add(locationKey)) {
                throw new IllegalStateException("TableLocationKey " + locationKey
                        + " was already added in this transaction.");
            }
        }

        synchronized void removeLocationKey(ImmutableTableLocationKey locationKey) {
            if (locationsRemoved == EMPTY_TABLE_LOCATION_KEYS) {
                locationsRemoved = new HashSet<>();
            }
            // When removing a location in a transaction, check for logical consistency.
            // 1. If the location was already removed in this transaction, we have a problem. A transaction should not
            // remove the same location twice.
            if (!locationsRemoved.add(locationKey)) {
                throw new IllegalStateException("TableLocationKey " + locationKey
                        + " was already removed and has not been replaced.");
            }
            // 2. If the location was already added in this transaction, we have a problem. A transaction should not
            // add then remove the same location.
            if (locationsAdded.contains(locationKey)) {
                throw new IllegalStateException("TableLocationKey " + locationKey
                        + " was removed after being added in the same transaction.");
            }
        }
    }

    private final ImmutableTableKey tableKey;
    private final ReferenceCountedLivenessNode livenessNode;

    // Open transactions that are being accumulated
    private final Map<Object, Transaction> transactions = Collections.synchronizedMap(new HashMap<>());

    /**
     * Map from {@link TableLocationKey} to itself, or to a {@link TableLocation}. The values are {@link TableLocation}s
     * if:
     * <ol>
     * <li>The location has been requested via {@link #getTableLocation(TableLocationKey)} or
     * {@link #getTableLocationIfPresent(TableLocationKey)}</li>
     * <li>The {@link TableLocationKey} <em>is</em> a {@link TableLocation}</li>
     * </ol>
     *
     * These values will not be cleared until all references to the {@link TableLocation} have been released by its
     * managers (i.e. {@link TableLocationSubscriptionBuffer subscriptions} and
     * {@link io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager column source managers}).
     */
    private final KeyedObjectHashMap<TableLocationKey, Object> tableLocations =
            new KeyedObjectHashMap<>(LocationKeyDefinition.INSTANCE);

    /**
     * The set of active location keys that will be returned to new subscribers.
     */
    private final KeyedObjectHashSet<ImmutableTableLocationKey, TrackedTableLocationKey> liveLocationKeys =
            new KeyedObjectHashSet<>(ImmutableTableLocationKeyDefinition.INSTANCE);

    private volatile boolean initialized;

    private List<String> partitionKeys;
    private boolean locationCreatedRecorder;

    /**
     * Construct a provider as part of a service.
     *
     * @param tableKey A key that will be used by this provider
     * @param supportsSubscriptions Whether this provider should support subscriptions
     */
    protected AbstractTableLocationProvider(@NotNull final TableKey tableKey, final boolean supportsSubscriptions) {
        super(supportsSubscriptions);
        this.tableKey = tableKey.makeImmutable();
        this.partitionKeys = null;

        livenessNode = new ReferenceCountedLivenessNode(false) {
            @Override
            protected void destroy() {
                AbstractTableLocationProvider.this.destroy();
            }
        };
    }

    /**
     * Construct a standalone provider.
     *
     * @param supportsSubscriptions Whether this provider should support subscriptions
     */
    protected AbstractTableLocationProvider(final boolean supportsSubscriptions) {
        this(StandaloneTableKey.getInstance(), supportsSubscriptions);
    }

    @Override
    public final String toString() {
        return getImplementationName() + '[' + tableKey + ']';
    }

    public final ImmutableTableKey getKey() {
        return tableKey;
    }

    @Override
    public LivenessNode asLivenessNode() {
        return livenessNode;
    }

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider/SubscriptionAggregator implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    protected final void deliverInitialSnapshot(@NotNull final TableLocationProvider.Listener listener) {
        // Lock the live set and deliver a copy to the listener
        synchronized (liveLocationKeys) {
            listener.handleTableLocationKeysUpdate(liveLocationKeys, List.of());
        }
    }

    /**
     * Internal method to begin an atomic transaction of location adds and removes.
     *
     * @param token A token to identify the transaction
     */
    protected void beginTransaction(@NotNull final Object token) {
        // Verify that we can start a new transaction with this token.
        transactions.compute(token, (key, val) -> {
            if (val != null) {
                throw new IllegalStateException("A transaction with token " + token + " is already open.");
            }
            return new Transaction();
        });
    }

    /**
     * Internal method to end an atomic transaction of location adds and removes.
     *
     * @param token A token to identify the transaction
     */
    protected void endTransaction(@NotNull final Object token) {
        // Verify that this transaction is open.
        final Transaction transaction = transactions.remove(token);
        if (transaction == null) {
            throw new IllegalStateException("No transaction with token " + token + " is currently open.");
        }

        // Return early if there are no changes to process
        if (transaction.locationsAdded == EMPTY_TABLE_LOCATION_KEYS
                && transaction.locationsRemoved == EMPTY_TABLE_LOCATION_KEYS) {
            return;
        }

        final Collection<TrackedTableLocationKey> addedKeys =
                new ArrayList<>(transaction.locationsAdded.size());
        final Collection<TrackedTableLocationKey> removedKeys =
                new ArrayList<>(transaction.locationsRemoved.size());

        synchronized (tableLocations) {
            for (ImmutableTableLocationKey locationKey : transaction.locationsRemoved) {
                // Remove this from the live set.
                final TrackedTableLocationKey trackedKey = liveLocationKeys.get(locationKey);
                Assert.neqNull(trackedKey, "trackedKey");
                liveLocationKeys.remove(trackedKey);
                // Pass this removed key to the subscribers
                removedKeys.add(trackedKey);
            }

            for (ImmutableTableLocationKey locationKey : transaction.locationsAdded) {
                locationCreatedRecorder = false;
                final Object result = tableLocations.putIfAbsent(locationKey, this::observeInsert);
                visitLocationKey(locationKey);
                if (locationCreatedRecorder) {
                    verifyPartitionKeys(locationKey);
                    addedKeys.add((TrackedTableLocationKey) result);
                }
            }
        }

        if (subscriptions != null && (!addedKeys.isEmpty() || !removedKeys.isEmpty())) {
            synchronized (subscriptions) {
                // Push the notifications to the subscribers
                if (subscriptions.deliverNotification(
                        Listener::handleTableLocationKeysUpdate,
                        addedKeys,
                        removedKeys,
                        true)) {
                    onEmpty();
                }
                // Release the keys that were removed only after we have delivered the notifications and the
                // subscribers have had a chance to process them
                removedKeys.forEach(this::unmanage);
            }
        }
    }

    /**
     * Deliver a possibly-new key.
     *
     * @param locationKey The new key
     * @apiNote This method is intended to be used by subclasses or by tightly-coupled discovery tools.
     */
    protected final void handleTableLocationKeyAdded(@NotNull final TableLocationKey locationKey) {
        handleTableLocationKeyAdded(locationKey, null);
    }

    /**
     * Deliver a possibly-new key, optionally as part of a transaction.
     *
     * @param locationKey The new key
     * @param transactionToken The token identifying the transaction (or null if not part of a transaction)
     * @apiNote This method is intended to be used by subclasses or by tightly-coupled discovery tools.
     */
    protected final void handleTableLocationKeyAdded(
            @NotNull final TableLocationKey locationKey,
            @Nullable final Object transactionToken) {

        if (transactionToken != null) {
            final Transaction transaction = transactions.get(transactionToken);
            if (transaction == null) {
                throw new IllegalStateException(
                        "No transaction with token " + transactionToken + " is currently open.");
            }
            // Store an immutable key
            transaction.addLocationKey(toKeyImmutable(locationKey));
            return;
        }

        if (!supportsSubscriptions()) {
            tableLocations.putIfAbsent(locationKey, this::observeInsert);
            visitLocationKey(locationKey);
            return;
        }

        // If we're not in a transaction, we should push this key immediately.
        synchronized (subscriptions) {
            // Since we're holding the lock on subscriptions, the following code is overly complicated - we could
            // certainly just deliver the notification in observeInsert. That said, I'm happier with this approach,
            // as it minimizes lock duration for tableLocations, exemplifies correct use of putIfAbsent, and keeps
            // observeInsert out of the business of subscription processing.
            locationCreatedRecorder = false;
            final Object result = tableLocations.putIfAbsent(locationKey, this::observeInsert);
            visitLocationKey(locationKey);
            if (locationCreatedRecorder) {
                verifyPartitionKeys(locationKey);
                if (subscriptions.deliverNotification(
                        Listener::handleTableLocationKeyAdded,
                        (TrackedTableLocationKey) result,
                        true)) {
                    onEmpty();
                }
            }
        }
    }

    /**
     * Handle a removal, optionally as part of a transaction. Notify subscribers that {@code locationKey} was removed if
     * necessary. See {@link #removeTableLocationKey(TableLocationKey)} for additional discussions of semantics.
     *
     * @param locationKey the TableLocation that was removed
     * @param transactionToken The token identifying the transaction (or null if not part of a transaction)
     */
    protected void handleTableLocationKeyRemoved(
            @NotNull final TableLocationKey locationKey,
            @Nullable final Object transactionToken) {
        if (transactionToken != null) {
            final Transaction transaction = transactions.get(transactionToken);
            if (transaction == null) {
                throw new IllegalStateException(
                        "No transaction with token " + transactionToken + " is currently open.");
            }
            transaction.removeLocationKey(toKeyImmutable(locationKey));
            return;
        }

        if (!supportsSubscriptions()) {
            // Remove this from the live set and un-manage it.
            final TrackedTableLocationKey trackedKey = liveLocationKeys.get(locationKey);
            liveLocationKeys.remove(trackedKey);
            unmanage(trackedKey);
            return;
        }

        // If we're not in a transaction, we should push this key immediately.
        synchronized (subscriptions) {
            final Object removedLocation = tableLocations.remove(locationKey);
            if (removedLocation != null) {
                final TrackedTableLocationKey trackedKey = liveLocationKeys.get(locationKey);
                Assert.neqNull(trackedKey, "trackedKey");
                // Remove this from the live set and un-manage it.
                liveLocationKeys.remove(trackedKey);
                if (subscriptions.deliverNotification(
                        Listener::handleTableLocationKeyRemoved,
                        trackedKey,
                        true)) {
                    onEmpty();
                }
                unmanage(trackedKey);
            }
        }
    }

    /**
     * Called <i>after</i> a table location has been visited by
     * {@link #handleTableLocationKeyAdded(TableLocationKey, Object)}, but before notifications have been delivered to
     * any subscriptions, if applicable. The default implementation does nothing, and may be overridden to implement
     * additional features.
     *
     * @param locationKey The {@link TableLocationKey} that was visited.
     */
    protected void visitLocationKey(@NotNull final TableLocationKey locationKey) {}

    @NotNull
    private Object observeInsert(@NotNull final TableLocationKey locationKey) {
        // NB: This must only be called while the lock on subscriptions is held.
        locationCreatedRecorder = true;

        final TrackedTableLocationKey trackedKey = toTrackedKey(locationKey);
        manage(trackedKey);

        // Add this to the live set.
        liveLocationKeys.add(trackedKey);
        return trackedKey;
    }

    /**
     * Make a new implementation-appropriate TableLocation from the supplied key.
     *
     * @param locationKey The table location key
     * @return The new TableLocation
     */
    @NotNull
    protected abstract TableLocation makeTableLocation(@NotNull final TableLocationKey locationKey);

    @Override
    public final TableLocationProvider ensureInitialized() {
        if (!isInitialized()) {
            doInitialization();
        }
        return this;
    }

    /**
     * Internal method for subclasses to call to determine if they need to call {@link #ensureInitialized()}, if doing
     * so might entail extra work (e.g. enqueueing an asynchronous job).
     *
     * @return Whether {@link #setInitialized()} has been called
     */
    protected final boolean isInitialized() {
        return initialized;
    }

    /**
     * Internal method for subclasses to call when they consider themselves to have been initialized.
     */
    protected final void setInitialized() {
        initialized = true;
    }

    /**
     * Initialization method for subclasses to override, in case simply calling {@link #refresh()} is inappropriate.
     * This is *not* guaranteed to be called only once. It should internally call {@link #setInitialized()} upon
     * successful initialization.
     */
    protected void doInitialization() {
        refresh();
        setInitialized();
    }

    @Override
    @NotNull
    public final Collection<TrackedTableLocationKey> getTableLocationKeys(
            final Predicate<TableLocationKey> filter) {
        // Lock the live set and deliver a copy to the listener after filtering.
        synchronized (liveLocationKeys) {
            return liveLocationKeys.stream()
                    .filter(tk -> filter.test(tk.getKey()))
                    .collect(Collectors.toList());
        }
    }

    @Override
    public final boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
        return tableLocations.containsKey(tableLocationKey);
    }

    @Override
    @Nullable
    public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
        Object current = tableLocations.get(tableLocationKey);
        if (current == null) {
            return null;
        }
        // See JavaDoc on tableLocations for background.
        // The intent is to create a TableLocation exactly once to replace the TableLocationKey placeholder that was
        // added in handleTableLocationKey.
        if (!(current instanceof TableLocation)) {
            final TableLocationKey immutableKey = (TableLocationKey) current;
            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (immutableKey) {
                current = tableLocations.get(immutableKey);
                if (immutableKey == current) {
                    // Make a new location, have the tracked key manage it, then replace the key with the
                    // new location in the map and return it. Note, this may contend for the lock on tableLocations
                    final TrackedTableLocationKey trackedKey = (TrackedTableLocationKey) current;
                    final TableLocation newLocation = makeTableLocation(immutableKey);
                    trackedKey.manageTableLocation(newLocation);
                    tableLocations.add(current = newLocation);
                }
            }
        }
        return (TableLocation) current;
    }

    /**
     * Remove a {@link TableLocationKey} and its corresponding {@link TableLocation} (if it was created). All
     * subscribers to this TableLocationProvider will be
     * {@link TableLocationProvider.Listener#handleTableLocationKeyRemoved(TrackedTableLocationKey) notified}. If the
     * TableLocation was created, all of its subscribers will additionally be
     * {@link TableLocation.Listener#handleUpdate() notified} that it no longer exists. This TableLocationProvider will
     * continue to update other locations and will no longer provide or request information about the removed location.
     *
     * <p>
     * In practice, there are three downstream patterns in use.
     * <ol>
     * <li>Intermediate TableLocationProviders, which will simply pass the removal on</li>
     * <li>{@link io.deephaven.engine.table.impl.SourceTable SourceTables}, which will propagate a failure notification
     * to all downstream listeners, and become {@link Table#isFailed() failed}.</li>
     * <li>{@link io.deephaven.engine.table.impl.SourcePartitionedTable SourcePartitionedTables}, which will notify
     * their downstream consumers of the removed constituent.</li>
     * </ol>
     *
     * @apiNote <em>Use with caution!</em> Implementations that call this method must provide certain guarantees: Reads
     *          should only succeed against removed locations if they will return complete, correct, consistent data.
     *          Otherwise, they should fail with a meaningful error message.
     *
     * @param locationKey The {@link TableLocationKey} to remove
     */
    public void removeTableLocationKey(@NotNull final TableLocationKey locationKey) {
        handleTableLocationKeyRemoved(locationKey, null);
    }

    private void verifyPartitionKeys(@NotNull final TableLocationKey locationKey) {
        if (partitionKeys == null) {
            partitionKeys = new ArrayList<>(locationKey.getPartitionKeys());
        } else if (!equals(partitionKeys, locationKey.getPartitionKeys())) {
            throw new TableDataException(String.format(
                    "%s has produced an inconsistent TableLocationKey with unexpected partition keys. expected=%s actual=%s.",
                    this, partitionKeys, locationKey.getPartitionKeys()));
        }
    }

    /**
     * Key definition for {@link TableLocation} or {@link TableLocationKey} lookup by {@link TableLocationKey}.
     */
    private static final class LocationKeyDefinition extends KeyedObjectKey.Basic<TableLocationKey, Object> {

        private static final KeyedObjectKey<TableLocationKey, Object> INSTANCE = new LocationKeyDefinition();

        private LocationKeyDefinition() {}

        @Override
        public TableLocationKey getKey(@NotNull final Object keyOrLocation) {
            return toKey(keyOrLocation);
        }
    }

    /**
     * Key definition for {@link TrackedTableLocationKey} lookup by {@link ImmutableTableLocationKey}.
     */
    private static final class ImmutableTableLocationKeyDefinition
            extends KeyedObjectKey.Basic<ImmutableTableLocationKey, TrackedTableLocationKey> {

        private static final KeyedObjectKey<ImmutableTableLocationKey, TrackedTableLocationKey> INSTANCE =
                new ImmutableTableLocationKeyDefinition();

        private ImmutableTableLocationKeyDefinition() {}

        @Override
        public ImmutableTableLocationKey getKey(TrackedTableLocationKey trackedTableLocationKey) {
            return trackedTableLocationKey.getKey();
        }
    }

    private static TableLocationKey toKey(@NotNull final Object keyOrLocation) {
        if (keyOrLocation instanceof TableLocation) {
            return ((TableLocation) keyOrLocation).getKey();
        }
        if (keyOrLocation instanceof TableLocationKey) {
            return ((TableLocationKey) keyOrLocation);
        }
        throw new IllegalArgumentException(
                "toKey expects a TableLocation or a TableLocationKey, instead received a " + keyOrLocation.getClass());
    }

    private static ImmutableTableLocationKey toKeyImmutable(@NotNull final Object keyOrLocation) {
        return (ImmutableTableLocationKey) toKey(keyOrLocation);
    }

    private TrackedTableLocationKey toTrackedKey(@NotNull final TableLocationKey locationKey) {
        return new TrackedTableLocationKey(locationKey.makeImmutable(), this::releaseLocationKey);
    }


    private static <T> boolean equals(Collection<T> c1, Collection<T> c2) {
        final Iterator<T> i2 = c2.iterator();
        for (T t1 : c1) {
            if (!i2.hasNext()) {
                return false;
            }
            if (!Objects.equals(t1, i2.next())) {
                return false;
            }
        }
        return !i2.hasNext();
    }

    /**
     * Called when every reference to the {@link TrackedTableLocationKey key} has been released.
     *
     * @param locationKey the key to release
     */
    private void releaseLocationKey(@NotNull final TrackedTableLocationKey locationKey) {
        // We can now remove the key from the tableLocations map
        tableLocations.remove(locationKey.getKey());
    }

    private void destroy() {
        // TODO: release all the TTLK references. Or does that happen automatically?
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
