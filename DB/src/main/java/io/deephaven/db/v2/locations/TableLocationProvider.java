package io.deephaven.db.v2.locations;

import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * Discovery utility for {@link TableLocation}s for a given table.
 */
public interface TableLocationProvider extends NamedImplementation {

    /**
     * Get the {@link TableKey} associated with this provider.
     *
     * @return The associated {@link TableKey}
     */
    ImmutableTableKey getKey();

    /**
     * Listener interface for anything that wants to know about new table location keys.
     */
    interface Listener extends BasicTableDataListener {

        /**
         * Notify the listener of a {@link TableLocationKey} encountered while initiating or maintaining the location
         * subscription. This should occur at most once per location, but the order of delivery is <i>not</i>
         * guaranteed.
         *
         * @param tableLocationKey The new table location key
         */
        void handleTableLocationKey(@NotNull ImmutableTableLocationKey tableLocationKey);
    }

    /**
     * Does this provider support subscriptions? That is, can this provider ever have ticking data?
     *
     * @return True if this provider supports subscriptions.
     */
    boolean supportsSubscriptions();

    /**
     * <p>
     * Subscribe to pushed location additions. Subscribing more than once with the same listener without an intervening
     * unsubscribe is an error, and may result in undefined behavior.
     * <p>
     * This is a possibly asynchronous operation - listener will receive 0 or more handleTableLocationKey callbacks,
     * followed by 0 or 1 handleException callbacks during invocation and continuing after completion, on a thread
     * determined by the implementation. As noted in {@link Listener#handleException(TableDataException)}, an exception
     * callback signifies that the subscription is no longer valid, and no unsubscribe is required in that case. Callers
     * <b>must not</b> hold any lock that prevents notification delivery while subscribing. Callers <b>must</b> guard
     * against duplicate notifications.
     * <p>
     * This method only guarantees eventually consistent state. To force a state update, use refresh() after
     * subscription completes.
     *
     * @param listener A listener.
     */
    void subscribe(@NotNull Listener listener);

    /**
     * Unsubscribe from pushed location additions.
     *
     * @param listener The listener to forget about.
     */
    void unsubscribe(@NotNull Listener listener);

    /**
     * Initialize or refresh state information about the list of existing locations.
     */
    void refresh();

    /**
     * Ensure that this location provider is initialized. Mainly useful in cases where it cannot be otherwise guaranteed
     * that {@link #refresh()} or {@link #subscribe(Listener)} has been called prior to calls to the various table
     * location fetch methods.
     *
     * @return this, to allow method chaining
     */
    TableLocationProvider ensureInitialized();

    /**
     * Get this provider's currently known location keys. The locations specified by the keys returned may have null
     * size - that is, they may not "exist" for application purposes. {@link #getTableLocation(TableLocationKey)} is
     * guaranteed to succeed for all results.
     *
     * @return A collection of keys for locations available from this provider
     */
    @NotNull
    Collection<ImmutableTableLocationKey> getTableLocationKeys();

    /**
     * Check if this provider knows the supplied location key.
     *
     * @param tableLocationKey The key to test for
     * @return Whether the key is known to this provider
     */
    boolean hasTableLocationKey(@NotNull final TableLocationKey tableLocationKey);

    /**
     * @param tableLocationKey A {@link TableLocationKey} specifying the location to get
     * @return The {@link TableLocation} matching the given key
     */
    @NotNull
    default TableLocation getTableLocation(@NotNull TableLocationKey tableLocationKey) {
        final TableLocation tableLocation = getTableLocationIfPresent(tableLocationKey);
        if (tableLocation == null) {
            throw new TableDataException(this + ": Unknown table location " + tableLocationKey);
        }
        return tableLocation;
    }

    /**
     * @param tableLocationKey A {@link TableLocationKey} specifying the location to get
     * @return The {@link TableLocation} matching the given key if present, else null
     */
    @Nullable
    TableLocation getTableLocationIfPresent(@NotNull TableLocationKey tableLocationKey);

    /**
     * Allow TableLocationProvider instances to have names.
     */
    default String getName() {
        return getImplementationName() + ':' + System.identityHashCode(this);
    }
}
