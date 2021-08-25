package io.deephaven.util.datastructures;

import io.deephaven.base.Procedure;
import io.deephaven.base.reference.SimpleReference;
import io.deephaven.base.reference.WeakReferenceWrapper;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.Array;
import java.util.Arrays;

/**
 * <p>
 * Array-backed set that stores generic listener objects, only enforcing hard reachability on listeners that are
 * SubstitutionWeakReferences.
 * <p>
 * <p>
 * All public operations (except clear and isEmpty) are O(n). All comparisons are based on object reference equality.
 * <p>
 * <p>
 * Requires external synchronization for thread safe usage, except where otherwise noted.
 */
public class SubscriptionSet<LISTENER_TYPE> {

    /**
     * Subscription entry, allowing state associated with a listener.
     */
    public class Entry {

        private final SimpleReference<LISTENER_TYPE> listenerReference;

        private boolean active;

        private Entry(final LISTENER_TYPE listener) {
            this.listenerReference =
                    WeakReferenceWrapper.maybeCreateWeakReference(Require.neqNull(listener, "listener"));
        }

        private LISTENER_TYPE getListener() {
            return listenerReference.get();
        }

        private boolean isActive() {
            return active;
        }

        /**
         * Activate this subscription entry. Must hold the lock on the enclosing subscription set.
         */
        public void activate() {
            Assert.holdsLock(SubscriptionSet.this, "SubscriptionSet.this");
            active = true;
        }
    }

    private Entry subscriptions[];
    private int size;

    public SubscriptionSet() {
        clear();
    }

    /**
     * Remove all listeners.
     */
    public final void clear() {
        // noinspection unchecked
        subscriptions = (Entry[]) Array.newInstance(Entry.class, 1);
        size = 0;
    }

    /**
     * Check if this set is empty, without cleaning up existing subscriptions.
     *
     * @return Whether this set is empty
     */
    public final boolean isEmpty() {
        return size == 0;
    }

    /**
     * Clean up any GC'd subscriptions.
     *
     * @return Whether this operation caused the set to become <b>empty</b>
     */
    public final boolean collect() {
        final int initialSize = size;
        for (int si = 0; si < size;) {
            if (subscriptions[si].getListener() == null) {
                removeAt(si);
                continue; // si is not incremented in this case - we'll reconsider the same slot if necessary.
            }
            ++si;
        }
        return initialSize > 0 && size == 0;
    }

    /**
     * Make an entry for a listener, in order to pass it to {@link #add(Object, Entry)}. May be called without holding
     * any locks.
     *
     * @param listener The listener
     * @return A new entry for the listener
     */
    public final Entry makeEntryFor(final LISTENER_TYPE listener) {
        return new Entry(listener);
    }

    /**
     * Add a listener to the set, if it's not already present. Clean up any GC'd subscriptions. See
     * {@link #makeEntryFor(Object)}.
     *
     * @param listener The listener to be added
     * @param entry An entry for the listener to be added
     * @return Whether this operation caused the set to become <b>non-empty</b>
     */
    public final boolean add(final @NotNull LISTENER_TYPE listener, final @NotNull Entry entry) {
        Require.eq(listener, "listener", entry.getListener(), "entry.getListener()");

        final int initialSize = size;
        boolean found = false;
        for (int si = 0; si < size;) {
            final Entry currentEntry = subscriptions[si];
            final LISTENER_TYPE currentListener = currentEntry.getListener();
            if (currentListener == null) {
                removeAt(si);
                continue; // si is not incremented in this case - we'll reconsider the same slot if necessary.
            }
            if (currentEntry == entry || currentListener == listener) {
                found = true;
            }
            ++si;
        }
        if (found) {
            return false; // We definitely had at least one subscription, since we found listener.
        }
        if (size == subscriptions.length) {
            subscriptions = Arrays.copyOf(subscriptions, size << 1);
        }
        subscriptions[size++] = entry;
        return initialSize == 0;
    }

    /**
     * Remove a listener from the set, if it's present. Clean up any GC'd subscriptions.
     *
     * @param listener The listener to remove
     * @return Whether this operation caused the set to become <b>empty</b>
     */
    public final boolean remove(final LISTENER_TYPE listener) {
        final int initialSize = size;
        for (int si = 0; si < size;) {
            final LISTENER_TYPE currentListener = subscriptions[si].getListener();
            if (currentListener == null || currentListener == listener) {
                removeAt(si);
                continue; // si is not incremented in this case - we'll reconsider the same slot if necessary.
            }
            ++si;
        }
        return initialSize > 0 && size == 0;
    }

    /**
     * Dispatch a nullary notification to all subscribers. Clean up any GC'd subscriptions.
     *
     * @param procedure The notification procedure to invoke
     * @param activeOnly Whether to restrict this notification to active subscriptions only
     * @return Whether this operation caused the set to become <b>empty</b>
     */
    public final boolean deliverNotification(@NotNull final Procedure.Unary<LISTENER_TYPE> procedure,
            final boolean activeOnly) {
        final int initialSize = size;
        for (int si = 0; si < size;) {
            final Entry currentEntry = subscriptions[si];
            final LISTENER_TYPE currentListener = currentEntry.getListener();
            if (currentListener == null) {
                removeAt(si);
                continue; // si is not incremented in this case - we'll reconsider the same slot if necessary.
            }
            if (!activeOnly || currentEntry.isActive()) {
                procedure.call(currentListener);
            }
            ++si;
        }
        return initialSize > 0 && size == 0;
    }

    /**
     * Dispatch a unary notification to all subscribers. Clean up any GC'd subscriptions.
     *
     * @param procedure The notification procedure to invoke
     * @param notification The notification to deliver
     * @param activeOnly Whether to restrict this notification to active subscriptions only
     * @return Whether this operation caused the set to become <b>empty</b>
     */
    public final <NOTIFICATION_TYPE> boolean deliverNotification(
            @NotNull final Procedure.Binary<LISTENER_TYPE, NOTIFICATION_TYPE> procedure,
            @Nullable final NOTIFICATION_TYPE notification,
            final boolean activeOnly) {
        final int initialSize = size;
        for (int si = 0; si < size;) {
            final Entry currentEntry = subscriptions[si];
            final LISTENER_TYPE currentListener = currentEntry.getListener();
            if (currentListener == null) {
                removeAt(si);
                continue; // si is not incremented in this case - we'll reconsider the same slot if necessary.
            }
            if (!activeOnly || currentEntry.isActive()) {
                procedure.call(currentListener, notification);
            }
            ++si;
        }
        return initialSize > 0 && size == 0;
    }

    private void removeAt(final int subscriptionIndex) {
        final int lastSubscriptionIndex = --size;
        subscriptions[subscriptionIndex] = subscriptions[lastSubscriptionIndex];
        subscriptions[lastSubscriptionIndex] = null;
    }
}
