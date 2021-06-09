package io.deephaven.db.v2.locations;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;

/**
 * Discovery utility for table locations for a given table.
 */
public interface TableLocationProvider extends TableKey {

    /**
     * Listener interface for anything that wants to know about new/updated table locations.
     */
    interface Listener extends BasicTableDataListener {

        /**
         * Notify the listener of a table location encountered while initiating or maintaining the location
         * subscription.  This should occur at most once per location, but the order of delivery is <i>not</i>
         * guaranteed.
         *
         * @param tableLocation The table location
         */
        void handleTableLocation(@NotNull TableLocation tableLocation);
    }

    /**
     * Does this provider support subscriptions? That is, can this provider ever have ticking data?
     *
     * @return True if this provider supports subscriptions.
     */
    boolean supportsSubscriptions();

    /**
     * <p>Subscribe to pushed location additions. Subscribing more than once with the same listener without an
     * intervening unsubscribe is an error, and may result in undefined behavior.
     * <p>This is a possibly asynchronous operation - listener will receive 0 or more handleTableLocation callbacks,
     * followed by 0 or 1 handleException callbacks during invocation and continuing after completion, on a thread
     * determined by the implementation. As noted in {@link Listener#handleException(TableDataException)}, an exception
     * callback signifies that the subscription is no longer valid, and no unsubscribe is required in that case.
     * Callers <b>must not</b> hold any lock that prevents notification delivery while subscribing.
     * Callers <b>must</b> guard against duplicate notifications.
     * <p>This method only guarantees eventually consistent state.  To force a state update, use refresh() after
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
     * Ensure that this location provider is initialized. Mainly useful in cases where it cannot be otherwise
     * guaranteed that {@link #refresh()} or {@link #subscribe(Listener)} has been called prior to calls to
     * the various table location fetch methods.
     *
     * @return this, to allow method chaining
     */
    TableLocationProvider ensureInitialized();

    /**
     * Get this provider's currently available locations.  Locations returned may have null size - that is, they may not
     * "exist" for application purposes.
     *
     * @return A collection of locations available from this provider
     */
    @NotNull
    Collection<TableLocation> getTableLocations();

    /**
     * @param internalPartition The internal partition
     * @param columnPartition   The column partition
     * @return The TableLocation matching the given partition names
     */
    @NotNull
    default TableLocation getTableLocation(@NotNull final String internalPartition, @NotNull final String columnPartition) {
        return getTableLocation(new TableLocationLookupKey.Immutable(internalPartition, columnPartition));
    }

    /**
     * @param tableLocationKey A key specifying the location to get
     * @return The TableLocation matching the given key
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
     * @param tableLocationKey A key specifying the location to get
     * @return The TableLocation matching the given key if present, else null
     */
    @Nullable
    TableLocation getTableLocationIfPresent(@NotNull TableLocationKey tableLocationKey);

    /**
     * allow TableLocationProvider instances to have names.
     */
    default String getName() {
        return getImplementationName() + ':' + System.identityHashCode(this);
    }
}
