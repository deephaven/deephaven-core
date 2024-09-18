//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.liveness.StandaloneLivenessManager;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Consumer;
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
        implements TableLocationProvider {

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

    private static class TrackedKeySupplier extends ReferenceCountedLivenessNode
            implements LiveSupplier<ImmutableTableLocationKey> {
        enum State {
            /** The key is included in the live set for new subscribers. */
            ACTIVE,
            /** The key was removed from the live list and will not be provided to new subscribers */
            INACTIVE
        }

        private final ImmutableTableLocationKey key;
        private final Consumer<TrackedKeySupplier> zeroCountConsumer;

        private TableLocation tableLocation;
        private State state;

        TrackedKeySupplier(
                final ImmutableTableLocationKey key,
                final Consumer<TrackedKeySupplier> zeroCountConsumer) {
            super(false);
            this.key = key;
            this.zeroCountConsumer = zeroCountConsumer;
            state = State.ACTIVE;
        }

        @Override
        public ImmutableTableLocationKey get() {
            return key;
        }

        /**
         * This {@link TrackedKeySupplier} should manage the given {@link TableLocation} and store a reference to it.
         *
         * @param tableLocation The {@link TableLocation} to manage.
         */
        public synchronized void setTableLocation(final TableLocation tableLocation) {
            Assert.eqNull(this.tableLocation, "this.tableLocation");
            manage(tableLocation);
            this.tableLocation = tableLocation;
        }

        /**
         * Change the state of this object to {@link State#INACTIVE}, indicates that this key is not included in the
         * live set for new subscribers.
         */
        public synchronized void deactivate() {
            this.state = State.INACTIVE;
        }

        @Override
        protected synchronized void destroy() {
            super.destroy();
            tableLocation = null;
            state = State.INACTIVE;
            zeroCountConsumer.accept(this);
        }
    }

    private final ImmutableTableKey tableKey;

    // Open transactions that are being accumulated
    private final Map<Object, Transaction> transactions = Collections.synchronizedMap(new HashMap<>());

    /**
     * Map from {@link TableLocationKey} to a {@link TrackedKeySupplier}.
     *
     * These values will not be cleared until all references to the {@link TableLocation} have been released by its
     * managers (i.e. {@link TableLocationSubscriptionBuffer subscriptions} and
     * {@link io.deephaven.engine.table.impl.sources.regioned.RegionedColumnSourceManager column source managers}).
     */
    private final KeyedObjectHashMap<TableLocationKey, TrackedKeySupplier> tableLocationKeyMap =
            new KeyedObjectHashMap<>(TableLocationKeyDefinition.INSTANCE);

    /**
     * TLP are (currently) outside of liveness scopes, but they need to manage liveness referents to prevent them from
     * going out of scope. The {@link StandaloneLivenessManager manager} will maintain the references until GC'd or the
     * referents are not needed byt the TLP.
     */
    private final StandaloneLivenessManager livenessManager;

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

        livenessManager = new StandaloneLivenessManager(false);
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

    // ------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider/SubscriptionAggregator implementation
    // ------------------------------------------------------------------------------------------------------------------

    @Override
    protected final void deliverInitialSnapshot(@NotNull final TableLocationProvider.Listener listener) {
        // Lock on the map and deliver a copy to the listener
        synchronized (tableLocationKeyMap) {
            final List<LiveSupplier<ImmutableTableLocationKey>> keySuppliers =
                    tableLocationKeyMap.values().stream()
                            .filter(iks -> iks.state == TrackedKeySupplier.State.ACTIVE)
                            .collect(Collectors.toList());
            listener.handleTableLocationKeysUpdate(keySuppliers, List.of());
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
        if (transaction.locationsAdded.isEmpty() && transaction.locationsRemoved.isEmpty()) {
            return;
        }

        final Collection<LiveSupplier<ImmutableTableLocationKey>> addedKeys =
                new ArrayList<>(transaction.locationsAdded.size());
        final Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeys =
                new ArrayList<>(transaction.locationsRemoved.size());

        synchronized (tableLocationKeyMap) {
            for (ImmutableTableLocationKey locationKey : transaction.locationsRemoved) {
                final TrackedKeySupplier trackedKey = tableLocationKeyMap.get(locationKey);
                if (trackedKey == null) {
                    // Not an error to remove a key multiple times (or a keu that was never added)
                    continue;
                }
                trackedKey.deactivate();

                // Pass this removed key to the subscribers
                removedKeys.add(trackedKey);
            }

            for (ImmutableTableLocationKey locationKey : transaction.locationsAdded) {
                locationCreatedRecorder = false;
                final TrackedKeySupplier result = tableLocationKeyMap.putIfAbsent(locationKey, this::observeInsert);
                visitLocationKey(locationKey);
                if (locationCreatedRecorder) {
                    verifyPartitionKeys(locationKey);
                    addedKeys.add(result);
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
                // Release the keys that were removed after we have delivered the notifications and the
                // subscribers have had a chance to process them
                removedKeys.forEach(livenessManager::unmanage);
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
            transaction.addLocationKey(locationKey.makeImmutable());
            return;
        }

        if (!supportsSubscriptions()) {
            tableLocationKeyMap.putIfAbsent(locationKey, this::observeInsert);
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
            final TrackedKeySupplier result = tableLocationKeyMap.putIfAbsent(locationKey, this::observeInsert);
            visitLocationKey(locationKey);
            if (locationCreatedRecorder) {
                verifyPartitionKeys(result.get());
                if (subscriptions.deliverNotification(
                        Listener::handleTableLocationKeyAdded,
                        (LiveSupplier<ImmutableTableLocationKey>) result,
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
            transaction.removeLocationKey(locationKey.makeImmutable());
            return;
        }

        if (!supportsSubscriptions()) {
            final TrackedKeySupplier trackedKey = tableLocationKeyMap.get(locationKey);
            trackedKey.deactivate();
            livenessManager.unmanage(trackedKey);
            return;
        }

        // If we're not in a transaction, we should push this key immediately.
        synchronized (subscriptions) {
            final TrackedKeySupplier trackedKey = tableLocationKeyMap.removeKey(locationKey);
            if (trackedKey != null) {
                trackedKey.deactivate();
                if (subscriptions.deliverNotification(
                        Listener::handleTableLocationKeyRemoved,
                        (LiveSupplier<ImmutableTableLocationKey>) trackedKey,
                        true)) {
                    onEmpty();
                }
                livenessManager.unmanage(trackedKey);
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
    private TrackedKeySupplier observeInsert(@NotNull final TableLocationKey locationKey) {
        // NB: This must only be called while the lock on subscriptions is held.
        locationCreatedRecorder = true;

        final TrackedKeySupplier trackedKey = toTrackedKey(locationKey);
        livenessManager.manage(trackedKey);

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
    public void getTableLocationKeys(
            final Consumer<LiveSupplier<ImmutableTableLocationKey>> consumer,
            final Predicate<ImmutableTableLocationKey> filter) {
        // Lock the live set and deliver a copy to the listener after filtering.
        synchronized (tableLocationKeyMap) {
            tableLocationKeyMap.values().stream()
                    .filter(ttlk -> ttlk.state == TrackedKeySupplier.State.ACTIVE)
                    .filter(ttlk -> filter.test(ttlk.get())).forEach(consumer);
        }
    }

    @Override
    public final boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey) {
        return tableLocationKeyMap.containsKey(tableLocationKey);
    }

    @Override
    @Nullable
    public TableLocation getTableLocationIfPresent(@NotNull final TableLocationKey tableLocationKey) {
        TrackedKeySupplier trackedKey = tableLocationKeyMap.get(tableLocationKey);
        if (trackedKey == null) {
            return null;
        }
        // See JavaDoc on tableLocations for background.
        if (trackedKey.tableLocation == null) {
            synchronized (trackedKey) {
                if (trackedKey.tableLocation == null) {
                    // Make a new location, have the tracked key manage it, then store the location in the tracked key.
                    final TableLocation newLocation = makeTableLocation(trackedKey.get());
                    trackedKey.setTableLocation(newLocation);
                }
            }
        }
        return trackedKey.tableLocation;
    }

    /**
     * Remove a {@link TableLocationKey} and its corresponding {@link TableLocation} (if it was created). All
     * subscribers to this TableLocationProvider will be
     * {@link TableLocationProvider.Listener#handleTableLocationKeyRemoved(LiveSupplier<ImmutableTableLocationKey>)
     * notified}. If the TableLocation was created, all of its subscribers will additionally be
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
     * Key definition for {@link LiveSupplier<ImmutableTableLocationKey>} lookup by {@link ImmutableTableLocationKey}.
     */
    private static final class TableLocationKeyDefinition
            extends KeyedObjectKey.Basic<TableLocationKey, TrackedKeySupplier> {

        private static final KeyedObjectKey<TableLocationKey, TrackedKeySupplier> INSTANCE =
                new TableLocationKeyDefinition();

        private TableLocationKeyDefinition() {}

        @Override
        public TableLocationKey getKey(TrackedKeySupplier immutableKeySupplier) {
            return immutableKeySupplier.get();
        }
    }

    private TrackedKeySupplier toTrackedKey(@NotNull final TableLocationKey locationKey) {
        return new TrackedKeySupplier(locationKey.makeImmutable(), this::releaseLocationKey);
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
     * Called when every reference to the {@link LiveSupplier<ImmutableTableLocationKey> key} has been released.
     *
     * @param locationKey the key to release
     */
    private void releaseLocationKey(@NotNull final TrackedKeySupplier locationKey) {
        // We can now remove the key from the tableLocations map
        tableLocationKeyMap.removeKey(locationKey.get());
    }
}
