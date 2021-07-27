package io.deephaven.db.v2.locations.impl;

import io.deephaven.db.v2.locations.*;
import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;

/**
 * Partial {@link TableLocationProvider} implementation for standalone use or as part of a {@link TableDataService}.
 * <p>
 * Presents an interface similar to {@link TableLocationProvider.Listener} for subclasses to use when
 * communicating with the parent; see {@link #handleTableLocationKey(TableLocationKey)}.
 * <p>
 * Note that subclasses are responsible for determining when it's appropriate to call {@link #setInitialized()}
 * and/or override {@link #doInitialization()}.
 */
public abstract class AbstractTableLocationProvider
        extends SubscriptionAggregator<TableLocationProvider.Listener>
        implements TableLocationProvider {

    private final ImmutableTableKey tableKey;

    /**
     * Map from {@link TableLocationKey} to itself, or to a {@link TableLocation}. The values are {@link TableLocation}s
     * if:
     * <ol>
     *     <li>The location has been requested via {@link #getTableLocation(TableLocationKey)} or
     *     {@link #getTableLocationIfPresent(TableLocationKey)}</li>
     *     <li>The {@link TableLocationKey} <em>is</em> a {@link TableLocation}</li>
     * </ol>
     */
    private final KeyedObjectHashMap<TableLocationKey, Object> tableLocations = new KeyedObjectHashMap<>(LocationKeyDefinition.INSTANCE);
    @SuppressWarnings("unchecked")
    private final Collection<ImmutableTableLocationKey> unmodifiableTableLocationKeys =
            (Collection<ImmutableTableLocationKey>) (Collection<? extends TableLocationKey>) Collections.unmodifiableCollection(tableLocations.keySet());

    private volatile boolean initialized;

    private boolean locationCreatedRecorder;

    /**
     * Construct a provider as part of a service.
     *
     * @param tableKey              A key that will be used by this provider
     * @param supportsSubscriptions Whether this provider should support subscriptions
     */
    protected AbstractTableLocationProvider(@NotNull final TableKey tableKey, final boolean supportsSubscriptions) {
        super(supportsSubscriptions);
        this.tableKey = tableKey.makeImmutable();
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

    //------------------------------------------------------------------------------------------------------------------
    // TableLocationProvider/SubscriptionAggregator implementation
    //------------------------------------------------------------------------------------------------------------------

    @Override
    protected final void deliverInitialSnapshot(@NotNull final TableLocationProvider.Listener listener) {
        unmodifiableTableLocationKeys.forEach(listener::handleTableLocationKey);
    }

    /**
     * Deliver a possibly-new key.
     *
     * @param locationKey The new key
     * @apiNote This method is intended to be used by subclasses or by tightly-coupled discovery tools.
     */
    protected final void handleTableLocationKey(@NotNull final TableLocationKey locationKey) {
        if (!supportsSubscriptions()) {
            tableLocations.putIfAbsent(locationKey, TableLocationKey::makeImmutable);
            return;
        }
        synchronized (subscriptions) {
            // Since we're holding the lock on subscriptions, the following code is overly complicated - we could
            // certainly just deliver the notification in observeInsert. That said, I'm happier with this approach,
            // as it minimizes lock duration for tableLocations, exemplifies correct use of putIfAbsent, and keeps
            // observeInsert out of the business of subscription processing.
            locationCreatedRecorder = false;
            final Object result = tableLocations.putIfAbsent(locationKey, this::observeInsert);
            if (locationCreatedRecorder && subscriptions.deliverNotification(Listener::handleTableLocationKey, toKeyImmutable(result), true)) {
                onEmpty();
            }
        }
    }

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
        return unmodifiableTableLocationKeys;
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
        if (current instanceof TableLocation) {
            return (TableLocation) current;
        }
        synchronized (tableLocations) {
            current = tableLocations.get(tableLocationKey);
            if (current instanceof TableLocation) {
                return (TableLocation) current;
            }
            final TableLocation result = makeTableLocation((TableLocationKey) current);
            tableLocations.add(result);
            return result;
        }
    }

    /**
     * Key definition for {@link TableLocation} or {@link TableLocationKey} lookup by {@link TableLocationKey}.
     */
    private static final class LocationKeyDefinition extends KeyedObjectKey.Basic<TableLocationKey, Object> {

        private static final KeyedObjectKey<TableLocationKey, Object> INSTANCE = new LocationKeyDefinition();

        private LocationKeyDefinition() {
        }

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
        throw new IllegalArgumentException("toKey expects a TableLocation or a TableLocationKey, instead received a " + keyOrLocation.getClass());
    }

    private static ImmutableTableLocationKey toKeyImmutable(@NotNull final Object keyOrLocation) {
        return (ImmutableTableLocationKey) toKey(keyOrLocation);
    }
}
