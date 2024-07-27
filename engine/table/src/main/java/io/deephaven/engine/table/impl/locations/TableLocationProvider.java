//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

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
     * ShiftObliviousListener interface for anything that wants to know about new table location keys.
     */
    interface Listener extends BasicTableDataListener {

        /**
         * Begin a transaction that collects location key additions and removals to be processed atomically.
         *
         * @param token A token to identify the transaction.
         */
        void beginTransaction(@NotNull Object token);

        /**
         * Begin a transaction that collects location key additions and removals to be processed atomically. Uses
         * {@code this} as the token.
         */
        default void beginTransaction() {
            beginTransaction(this);
        }

        /**
         * End the transaction and process the location changes.
         *
         * @param token A token to identify the transaction.
         */
        void endTransaction(@NotNull Object token);

        /**
         * End the transaction and process the location changes. Uses {@code this} as the token.
         */
        default void endTransaction() {
            endTransaction(this);
        }

        /**
         * <p>
         * Notify the listener of a {@link TableLocationKey} encountered while initiating or maintaining the location
         * subscription. This should occur at most once per location, but the order of delivery is <i>not</i>
         * guaranteed.
         * </p>
         * <p>
         * If transactionToken is {@code null}, the key will be added to the pending additions immediately.
         * </p>
         * @param tableLocationKey The new table location key.
         * @param transactionToken The token identifying the transaction.
         */
        void handleTableLocationKeyAdded(
                @NotNull ImmutableTableLocationKey tableLocationKey,
                @Nullable Object transactionToken);

        /**
         * Notify the listener of a {@link TableLocationKey} encountered while initiating or maintaining the location
         * subscription. This should occur at most once per location, but the order of delivery is <i>not</i>
         * guaranteed. Uses {@code this} as the token.
         *
         * @param tableLocationKey The new table location key.
         */
        default void handleTableLocationKeyAdded(@NotNull ImmutableTableLocationKey tableLocationKey) {
            handleTableLocationKeyAdded(tableLocationKey, this);
        }

        /**
         * <p>
         * Notify the listener of a {@link TableLocationKey} that has been removed.
         * </p>
         * <p>
         * If transactionToken is {@code null}, the key will be added to the pending removals immediately.
         * </p>
         *
         * @param tableLocationKey The table location key that was removed.
         * @param transactionToken The token identifying the transaction.
         */
        void handleTableLocationKeyRemoved(
                @NotNull ImmutableTableLocationKey tableLocationKey,
                @Nullable Object transactionToken);

        /**
         * Notify the listener of a {@link TableLocationKey} that has been removed. Uses {@code this} as the token.
         *
         * @param tableLocationKey The table location key that was removed.
         */
        @SuppressWarnings("unused")
        default void handleTableLocationKeyRemoved(@NotNull ImmutableTableLocationKey tableLocationKey) {
            handleTableLocationKeyRemoved(tableLocationKey, this);
        }
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
     * This method only guarantees eventually consistent state. To force a state update, use run() after subscription
     * completes.
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
     * @return this, to allow method chaining.
     */
    TableLocationProvider ensureInitialized();

    /**
     * Get this provider's currently known location keys. The locations specified by the keys returned may have null
     * size - that is, they may not "exist" for application purposes. {@link #getTableLocation(TableLocationKey)} is
     * guaranteed to succeed for all results.
     *
     * @return A collection of keys for locations available from this provider.
     */
    @NotNull
    Collection<ImmutableTableLocationKey> getTableLocationKeys();

    /**
     * Check if this provider knows the supplied location key.
     *
     * @param tableLocationKey The key to test.
     * @return Whether the key is known to this provider.
     */
    boolean hasTableLocationKey(@NotNull TableLocationKey tableLocationKey);

    /**
     * @param tableLocationKey A {@link TableLocationKey} specifying the location to get.
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
     * @param tableLocationKey A {@link TableLocationKey} specifying the location to get.
     * @return The {@link TableLocation} matching the given key if present, else null.
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
