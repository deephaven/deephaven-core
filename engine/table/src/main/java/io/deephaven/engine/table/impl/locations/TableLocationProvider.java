//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.liveness.LiveSupplier;
import io.deephaven.engine.table.impl.TableUpdateMode;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.type.NamedImplementation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;

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
     * Get the {@link TableUpdateMode update guarantees} of this provider describing how this provider will add or
     * remove table locations.
     */
    @NotNull
    TableUpdateMode getUpdateMode();

    /**
     * Get the location {@link TableUpdateMode update guarantees} of this provider describing how individual locations
     * will add or remove rows.
     */
    @NotNull
    TableUpdateMode getLocationUpdateMode();

    /**
     * ShiftObliviousListener interface for anything that wants to know about new table location keys.
     */
    interface Listener extends BasicTableDataListener {
        /**
         * Notify the listener of a {@link LiveSupplier<ImmutableTableLocationKey>} encountered while initiating or
         * maintaining the location subscription. This should occur at most once per location, but the order of delivery
         * is <i>not</i> guaranteed. This addition is not part of any transaction, and is equivalent to
         * {@code handleTableLocationKeyAdded(tableLocationKey, null);} by default.
         *
         * @param tableLocationKey The new table location key.
         */
        void handleTableLocationKeyAdded(@NotNull LiveSupplier<ImmutableTableLocationKey> tableLocationKey);

        /**
         * Notify the listener of a {@link LiveSupplier<ImmutableTableLocationKey>} that has been removed. This removal
         * is not part of any transaction, and is equivalent to
         * {@code handleTableLocationKeyRemoved(tableLocationKey, null);} by default.
         *
         * @param tableLocationKey The table location key that was removed.
         */
        @SuppressWarnings("unused")
        void handleTableLocationKeyRemoved(@NotNull LiveSupplier<ImmutableTableLocationKey> tableLocationKey);

        /**
         * <p>
         * Notify the listener of collections of {@link TableLocationKey TableLocationKeys} added or removed while
         * initiating or maintaining the location subscription. Addition or removal should occur at most once per
         * location, but the order of delivery is <i>not</i> guaranteed.
         * </p>
         *
         * @param addedKeys Collection of table location keys that were added.
         * @param removedKeys Collection of table location keys that were removed.
         */
        default void handleTableLocationKeysUpdate(
                @NotNull Collection<LiveSupplier<ImmutableTableLocationKey>> addedKeys,
                @NotNull Collection<LiveSupplier<ImmutableTableLocationKey>> removedKeys) {
            removedKeys.forEach(this::handleTableLocationKeyRemoved);
            addedKeys.forEach(this::handleTableLocationKeyAdded);
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
     * @return this, to allow method chaining.
     */
    TableLocationProvider ensureInitialized();

    /**
     * Get this provider's currently known location keys. The locations specified by the keys returned may have null
     * size - that is, they may not "exist" for application purposes. {@link #getTableLocation(TableLocationKey)} is
     * guaranteed to succeed for all results as long as the associated {@link LiveSupplier} is retained by the caller.
     */
    @TestUseOnly
    default Collection<ImmutableTableLocationKey> getTableLocationKeys() {
        final List<ImmutableTableLocationKey> keys = new ArrayList<>();
        getTableLocationKeys(trackedKey -> keys.add(trackedKey.get()));
        return keys;
    }

    /**
     * Get this provider's currently known location keys. The locations specified by the keys returned may have null
     * size - that is, they may not "exist" for application purposes. {@link #getTableLocation(TableLocationKey)} is
     * guaranteed to succeed for all results as long as the associated {@link LiveSupplier} is retained by the caller.
     *
     * @param consumer A consumer to receive the location keys
     */
    default void getTableLocationKeys(Consumer<LiveSupplier<ImmutableTableLocationKey>> consumer) {
        getTableLocationKeys(consumer, key -> true);
    }

    /**
     * Get this provider's currently known location keys. The locations specified by the keys returned may have null
     * size - that is, they may not "exist" for application purposes. {@link #getTableLocation(TableLocationKey)} is
     * guaranteed to succeed for all results as long as the associated {@link LiveSupplier} is retained by the caller.
     *
     * @param consumer A consumer to receive the location keys
     * @param filter A filter to apply to the location keys before the consumer is called
     */
    void getTableLocationKeys(
            Consumer<LiveSupplier<ImmutableTableLocationKey>> consumer,
            Predicate<ImmutableTableLocationKey> filter);

    /**
     * Check if this provider knows the supplied location key.
     *
     * @param tableLocationKey The key to test.
     * @return Whether the key is known to this provider.
     */
    boolean hasTableLocationKey(@NotNull TableLocationKey tableLocationKey);

    /**
     * Get the {@link TableLocation} associated with the given key. Callers should ensure that they retain the
     * {@link LiveSupplier} returned by {@link #getTableLocationKeys(Consumer, Predicate)} for the key they are
     * interested in, as the location may be removed if the supplier is no longer live.
     *
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
     * Get the {@link TableLocation} associated with the given key if it exists. Callers should ensure that they retain
     * the {@link LiveSupplier} returned by {@link #getTableLocationKeys(Consumer, Predicate)} for the key they are
     * interested in, as the location may be removed if the supplier is no longer live.
     *
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
