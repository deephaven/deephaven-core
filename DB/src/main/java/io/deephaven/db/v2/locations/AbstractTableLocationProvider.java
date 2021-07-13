package io.deephaven.db.v2.locations;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;

/**
 * Partial {@link TableLocationProvider} implementation for use standalone or as part of {@link TableDataService}
 * implementations.
 * <p>
 * It implements an interface similar to TableLocationProvider.Listener for implementation classes to use when
 * communicating with the parent.
 * <p>
 * Note that implementations are responsible for determining when it's appropriate to call {@link #setInitialized()}
 * and/or override {@link #doInitialization()}.
 */
public abstract class AbstractTableLocationProvider
        extends SubscriptionAggregator<TableLocationProvider.Listener>
        implements TableLocationProvider {

    private final ImmutableTableKey tableKey;

    private final KeyedObjectHashMap<TableLocationKey, TableLocation> tableLocations = new KeyedObjectHashMap<>(LocationKeyDefinition.INSTANCE);
    @SuppressWarnings("unchecked")
    private final Collection<ImmutableTableLocationKey> unmodifiableTableLocationKeys =
            (Collection<ImmutableTableLocationKey>) (Collection<? extends TableLocationKey>)  Collections.unmodifiableCollection(tableLocations.keySet());

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
        return getClass().getName() + '[' + tableKey + ']';
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
     */
    public final void handleTableLocationKey(@NotNull final TableLocationKey locationKey) {
        if (supportsSubscriptions()) {
            synchronized (subscriptions) {
                // Since we're holding the lock on subscriptions, the following code is overly complicated - we could
                // certainly just deliver the notification in observeTableLocationCreation. That said, I'm happier with
                // this approach, as it minimizes lock duration for tableLocations, exemplifies correct use of
                // putIfAbsent, and keeps observeTableLocationCreation out of the business of subscription processing.
                locationCreatedRecorder = false;
                final TableLocation tableLocation = tableLocations.putIfAbsent(locationKey, this::observeTableLocationCreation);
                if (locationCreatedRecorder && subscriptions.deliverNotification(Listener::handleTableLocationKey, tableLocation.getKey(), true)) {
                    onEmpty();
                }
            }
        } else {
            tableLocations.putIfAbsent(locationKey, PlaceholderTableLocation::new);
        }
    }

    @NotNull
    private TableLocation observeTableLocationCreation(@NotNull final TableLocationKey locationKey) {
        // NB: This must only be called while the lock on subscriptions is held.
        locationCreatedRecorder = true;
        return new PlaceholderTableLocation(locationKey);
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
        final TableLocation current = tableLocations.get(tableLocationKey);
        if (current instanceof PlaceholderTableLocation) {
            return tableLocations.putIfAbsent(current.getKey(), this::makeTableLocation);
        }
        return current;
    }

    //------------------------------------------------------------------------------------------------------------------
    // Default key definition implementation
    //------------------------------------------------------------------------------------------------------------------

    private static final class LocationKeyDefinition extends KeyedObjectKey.Basic<TableLocationKey, TableLocation> {

        private static final KeyedObjectKey<TableLocationKey, TableLocation> INSTANCE = new LocationKeyDefinition();

        private LocationKeyDefinition() {
        }

        @Override
        public TableLocationKey getKey(@NotNull final TableLocation tableLocation) {
            return tableLocation.getKey();
        }
    }

    //------------------------------------------------------------------------------------------------------------------
    // Placeholder TableLocation implementation (for deferred instantiation)
    //------------------------------------------------------------------------------------------------------------------

    private final class PlaceholderTableLocation implements TableLocation {

        private final ImmutableTableLocationKey key;

        private PlaceholderTableLocation(@NotNull final TableLocationKey key) {
            this.key = key.makeImmutable();
        }

        @NotNull
        @Override
        public ImmutableTableKey getTableKey() {
            return AbstractTableLocationProvider.this.getKey();
        }

        @NotNull
        @Override
        public ImmutableTableLocationKey getKey() {
            return key;
        }

        @Override
        public boolean supportsSubscriptions() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void subscribe(@NotNull Listener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unsubscribe(@NotNull Listener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void refresh() {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public ColumnLocation getColumnLocation(@NotNull CharSequence name) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public Object getStateLock() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getSize() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLastModifiedTimeMillis() {
            throw new UnsupportedOperationException();
        }
    }
}
