//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.locations.*;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.*;

/**
 * Partial {@link TableLocationProvider} implementation for standalone use or as part of a {@link TableDataService}.
 * <p>
 * Presents an interface similar to {@link TableLocationProvider.Listener} for subclasses to use when communicating with
 * the parent; see {@link #handleTableLocationKeyAdded(TableLocationKey, Object).
 * <p>
 * Note that subclasses are responsible for determining when it's appropriate to call {@link #setInitialized()} and/or
 * override {@link #doInitialization()}.
 */
public abstract class AbstractTableLocationProvider
        extends SubscriptionAggregator<TableLocationProvider.Listener>
        implements TableLocationProvider {

    private static final Set<TableLocationKey> EMPTY_TABLE_LOCATION_KEYS = Collections.emptySet();

    private final ImmutableTableKey tableKey;

    // These sets represent open transactions that are being accumulated.
    private final Set<Object> transactionTokens = new HashSet<>();
    private final Map<Object, Set<TableLocationKey>> accumulatedLocationsAdded = new HashMap<>();
    private final Map<Object, Set<TableLocationKey>> accumulatedLocationsRemoved = new HashMap<>();

    private final Object transactionLock = new Object();

    /**
     * Map from {@link TableLocationKey} to itself, or to a {@link TableLocation}. The values are {@link TableLocation}s
     * if:
     * <ol>
     * <li>The location has been requested via {@link #getTableLocation(TableLocationKey)} or
     * {@link #getTableLocationIfPresent(TableLocationKey)}</li>
     * <li>The {@link TableLocationKey} <em>is</em> a {@link TableLocation}</li>
     * </ol>
     */
    private final KeyedObjectHashMap<TableLocationKey, Object> tableLocations =
            new KeyedObjectHashMap<>(LocationKeyDefinition.INSTANCE);
    @SuppressWarnings("unchecked")
    private final Collection<ImmutableTableLocationKey> unmodifiableTableLocationKeys =
            (Collection<ImmutableTableLocationKey>) (Collection<? extends TableLocationKey>) Collections
                    .unmodifiableCollection(tableLocations.keySet());

    final List<AbstractTableLocation> locationsToClear;
    final UpdateCommitter<?> locationClearCommitter;

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

        locationsToClear = new ArrayList<>();
        locationClearCommitter = new UpdateCommitter<>(this,
                ExecutionContext.getContext().getUpdateGraph(),
                (ignored) -> {
                    locationsToClear.forEach(location -> {
                        location.handleUpdate(null, System.currentTimeMillis());
                        location.clearColumnLocations();

                    });
                    locationsToClear.clear();
                });
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
        listener.handleTableLocationKeysUpdate(unmodifiableTableLocationKeys, null);
    }

    protected final void handleTableLocationKeyAdded(@NotNull final TableLocationKey locationKey) {
        handleTableLocationKeyAdded(locationKey, null);
    }

    /**
     * Internal method to begin an atomic transaction of location adds and removes.
     *
     * @param token A token to identify the transaction
     */
    protected void beginTransaction(@NotNull final Object token) {
        synchronized (transactionLock) {
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

    /**
     * Internal method to end an atomic transaction of location adds and removes.
     *
     * @param token A token to identify the transaction
     */
    protected void endTransaction(@NotNull final Object token) {
        final Set<TableLocationKey> locationsAdded;
        final Set<TableLocationKey> locationsRemoved;
        synchronized (transactionLock) {
            // Verify that this transaction is open.
            if (!transactionTokens.remove(token)) {
                throw new IllegalStateException("No transaction with token " + token + " is currently open.");
            }

            locationsAdded = accumulatedLocationsAdded.remove(token);
            locationsRemoved = accumulatedLocationsRemoved.remove(token);
        }

        final Collection<ImmutableTableLocationKey> addedImmutableKeys = new ArrayList<>(locationsAdded.size());
        final Collection<ImmutableTableLocationKey> removedImmutableKeys = new ArrayList<>(locationsRemoved.size());

        // Process the accumulated adds and removes under a lock on `tableLocations` to keep modifications atomic to
        // other holders of this lock.
        synchronized (tableLocations) {
            if (locationsAdded != EMPTY_TABLE_LOCATION_KEYS || locationsRemoved != EMPTY_TABLE_LOCATION_KEYS) {
                for (TableLocationKey locationKey : locationsAdded) {
                    locationCreatedRecorder = false;
                    final Object result = tableLocations.putIfAbsent(locationKey, this::observeInsert);
                    visitLocationKey(locationKey);
                    if (locationCreatedRecorder) {
                        verifyPartitionKeys(locationKey);
                        addedImmutableKeys.add(toKeyImmutable(result));
                    }
                }

                for (TableLocationKey locationKey : locationsRemoved) {
                    final Object removedLocation = tableLocations.remove(locationKey);
                    if (removedLocation != null) {
                        maybeClearLocationForRemoval(removedLocation);
                        removedImmutableKeys.add(toKeyImmutable(locationKey));
                    }
                }
            }
        }

        if (subscriptions != null) {
            synchronized (subscriptions) {
                // Push the notifications to the subscribers.
                if ((!addedImmutableKeys.isEmpty() || !removedImmutableKeys.isEmpty())
                        && subscriptions.deliverNotification(
                        Listener::handleTableLocationKeysUpdate,
                        addedImmutableKeys,
                        removedImmutableKeys,
                        true)) {
                    onEmpty();
                }
            }
        }
    }

    /**
     * Deliver a possibly-new key.
     *
     * @param locationKey The new key
     * @param transactionToken The token identifying the transaction
     * @apiNote This method is intended to be used by subclasses or by tightly-coupled discovery tools.
     */
    protected final void handleTableLocationKeyAdded(
            @NotNull final TableLocationKey locationKey,
            @Nullable final Object transactionToken) {

        if (!supportsSubscriptions()) {
            tableLocations.putIfAbsent(locationKey, TableLocationKey::makeImmutable);
            visitLocationKey(toKeyImmutable(locationKey));
            return;
        }

        if (transactionToken != null) {
            // When adding a location in a transaction, check for logical consistency.
            // 1. If the location was already added in this transaction, we have a problem. A transaction should not
            // add the same location twice.
            // 2. If the location was already removed in this transaction, we have a `replace` operation which is not a
            // logical error (although it may not be supported by all consumers).
            synchronized (transactionLock) {
                if (accumulatedLocationsAdded.get(transactionToken) == EMPTY_TABLE_LOCATION_KEYS) {
                    accumulatedLocationsAdded.put(transactionToken, new HashSet<>());
                }
                final Set<TableLocationKey> locationsAdded = accumulatedLocationsAdded.get(transactionToken);

                if (accumulatedLocationsAdded.containsKey(locationKey)) {
                    throw new IllegalStateException("TableLocationKey " + locationKey
                            + " was added multiple times in the same transaction.");
                }
                locationsAdded.add(locationKey);
            }
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
                        toKeyImmutable(result),
                        true)) {
                    onEmpty();
                }
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
        return locationKey.makeImmutable();
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
    public final Collection<ImmutableTableLocationKey> getTableLocationKeys() {
        // This lock is held while `endTransaction()` updates `tableLocations` with the accumulated adds/removes.
        // Locking here ensures that this call won't return while `tableLocations` (and `unmodifiableTableLocationKeys`)
        // contain a partial transaction.
        synchronized (tableLocations) {
            return unmodifiableTableLocationKeys;
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
                    // Note, this may contend for the lock on tableLocations
                    tableLocations.add(current = makeTableLocation(immutableKey));
                }
            }
        }
        return (TableLocation) current;
    }

    /**
     * Remove a {@link TableLocationKey} and its corresponding {@link TableLocation} (if it was created). All
     * subscribers to this TableLocationProvider will be
     * {@link TableLocationProvider.Listener#handleTableLocationKeyRemoved(ImmutableTableLocationKey) notified}. If the
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

    protected final void handleTableLocationKeyRemoved(@NotNull final TableLocationKey locationKey) {
        handleTableLocationKeyRemoved(locationKey, null);
    }

    /**
     * Handle a removal, optionally as part of a transaction. Notify subscribers that {@code locationKey} was removed if
     * necessary. See {@link #removeTableLocationKey(TableLocationKey)} for additional discussions of semantics.
     * 
     * @param locationKey the TableLocation that was removed
     * @param transactionToken The token identifying the transaction
     */
    protected void handleTableLocationKeyRemoved(
            @NotNull final TableLocationKey locationKey,
            @Nullable final Object transactionToken) {
        if (!supportsSubscriptions()) {
            maybeClearLocationForRemoval(tableLocations.remove(locationKey));
            return;
        }

        // When removing a location in a transaction, check for logical consistency.
        // 1. If the location was already removed in this transaction, we have a problem. A transaction should not
        // remove the same location twice.
        // 2. If the location was already added in this transaction, we have a problem. A transaction should not
        // add then remove the same location.
        if (transactionToken != null) {
            synchronized (transactionLock) {
                if (accumulatedLocationsRemoved.get(transactionToken) == EMPTY_TABLE_LOCATION_KEYS) {
                    accumulatedLocationsRemoved.put(transactionToken, new HashSet<>());
                }
                final Set<TableLocationKey> locationsRemoved = accumulatedLocationsRemoved.get(transactionToken);

                if (accumulatedLocationsRemoved.containsKey(locationKey)) {
                    throw new IllegalStateException("TableLocationKey " + locationKey
                            + " was removed multiple times in the same transaction.");
                } else if (accumulatedLocationsAdded.containsKey(locationKey)) {
                    throw new IllegalStateException("TableLocationKey " + locationKey
                            + " was removed after being added in the same transaction.");
                }
                locationsRemoved.add(locationKey);
                return;
            }
        }

        // If we're not in a transaction, we should push this key immediately.
        synchronized (subscriptions) {
            final Object removedLocation = tableLocations.remove(locationKey);
            if (removedLocation != null) {
                maybeClearLocationForRemoval(removedLocation);
                if (subscriptions.deliverNotification(
                        Listener::handleTableLocationKeyRemoved,
                        locationKey.makeImmutable(),
                        true)) {
                    onEmpty();
                }
            }
        }
    }

    private void maybeClearLocationForRemoval(@Nullable final Object removedLocation) {
        if (removedLocation instanceof AbstractTableLocation) {
            locationsToClear.add((AbstractTableLocation) removedLocation);
            locationClearCommitter.maybeActivate();
        }
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
}
