package io.deephaven.util.datastructures;

import gnu.trove.impl.PrimeFinder;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.function.Consumer;

/**
 * An open-addressed identity hash set that only maintains weak references to its members. Only
 * supports {@link #add} and {@link #clear}. See {@link Synchronized} variant for concurrent usage.
 */
public class WeakIdentityHashSet<TYPE> {

    private static final double LOAD_FACTOR = 0.5;
    private static final int NULL_SLOT = -1;
    private static final WeakReference<?> DELETED_REFERENCE = new DeletedReference();

    private WeakReference<? extends TYPE>[] storage;
    private int usedSlots; // The number of slots that *may* contain a valid reference (not exactly
                           // "size")
    private int freeSlots;
    private int rehashThreshold;

    @SuppressWarnings("WeakerAccess")
    public WeakIdentityHashSet() {
        this(1, false);
    }

    private WeakIdentityHashSet(final int initialCapacity, final boolean exact) {
        initialize(initialCapacity, exact);
    }

    /**
     * Initialize the set with the new capacity.
     *
     * @param capacity The new capacity
     * @param exact Whether the capacity is the exact requirement, or should be adjusted for load
     *        factor and primeness
     */
    private void initialize(final int capacity, final boolean exact) {
        storage = null;
        usedSlots = 0;
        freeSlots =
            exact ? capacity : PrimeFinder.nextPrime((int) Math.ceil(capacity / LOAD_FACTOR));
        rehashThreshold = Math.min(freeSlots - 1, (int) Math.floor(freeSlots * LOAD_FACTOR));
    }

    /**
     * After an insert, update the usedSlots and freeSlots, and trigger a rehash if necessary.
     */
    private void updateAccountingForInsert(final boolean usedFreeSlot) {
        if (usedFreeSlot) {
            --freeSlots;
        }

        // Follows the same policies as io.deephaven.base.hash.KHash, as we may very well want to
        // take advantage of the
        // guarantees they provide in the future.
        if (++usedSlots > rehashThreshold || freeSlots == 1) {
            // If we've grown beyond our maximum usedSlots, double capacity.
            // If we've exhausted the free spots, rehash to the same capacity, freeing up removed
            // slots.
            final int newCapacity =
                usedSlots > rehashThreshold ? PrimeFinder.nextPrime(storage.length << 1)
                    : storage.length;
            rehash(newCapacity);
        }
    }

    /**
     * Mark a slot deleted (either because we removed the value there, or because it was found to
     * have a cleared reference). This allows us to update usedSlots in order to avoid rehash calls
     * in some cases.
     *
     * @param slot The slot index to mark
     */
    private void markSlotDeleted(final int slot) {
        // noinspection unchecked
        storage[slot] = (WeakReference<TYPE>) DELETED_REFERENCE;
        --usedSlots;
    }

    /**
     * Rehashes the set.
     *
     * @param newCapacity The new capacity to rehash to
     */
    private void rehash(final int newCapacity) {
        final WeakIdentityHashSet<TYPE> other = new WeakIdentityHashSet<>(newCapacity, true);
        for (WeakReference<? extends TYPE> valueReference : storage) {
            final TYPE value;
            if (valueReference != null && valueReference != DELETED_REFERENCE
                && (value = valueReference.get()) != null) {
                other.add(value, valueReference);
            }
        }
        storage = other.storage;
        usedSlots = other.usedSlots;
        freeSlots = other.freeSlots;
        rehashThreshold = other.rehashThreshold;
    }

    /**
     * Clear the set.
     */
    public void clear() {
        initialize(1, false);
    }

    /**
     * Add a value to the set if its not already present.
     *
     * @param value The value to add
     * @return True if the value was added to the set
     */
    public boolean add(@NotNull final TYPE value) {
        return add(value, null);
    }

    /**
     * Add a value to the set if its not already present.
     *
     * @param value The value to add
     * @param valueReference A re-usable WeakReference to value if already available, else null
     * @return True if the value was added to the set
     */
    public boolean add(@NotNull final TYPE value,
        @Nullable final WeakReference<? extends TYPE> valueReference) {
        if (storage == null) {
            // noinspection unchecked
            storage = new WeakReference[freeSlots]; // usedSlots == 0, freeSlots == capacity
        }

        final int length = storage.length;
        final int hash = System.identityHashCode(value);
        final int probe = 1 + (hash % (length - 2));

        int candidateSlot = hash % length;
        boolean foundDeletedSlot = false;
        int firstDeletedSlot = NULL_SLOT;
        while (true) {
            final WeakReference<? extends TYPE> candidateReference = storage[candidateSlot];
            if (candidateReference == null) {
                storage[foundDeletedSlot ? firstDeletedSlot : candidateSlot] =
                    valueReference == null ? new WeakReference<>(value) : valueReference;
                updateAccountingForInsert(!foundDeletedSlot);
                return true;
            }

            if (candidateReference == DELETED_REFERENCE) {
                if (!foundDeletedSlot) {
                    foundDeletedSlot = true;
                    firstDeletedSlot = candidateSlot;
                }
            } else {
                final TYPE candidate = candidateReference.get();
                if (candidate == value) {
                    return false;
                }
                if (candidate == null) {
                    markSlotDeleted(candidateSlot);
                    if (!foundDeletedSlot) {
                        foundDeletedSlot = true;
                        firstDeletedSlot = candidateSlot;
                    }
                }
            }

            if ((candidateSlot -= probe) < 0) {
                candidateSlot += length;
            }
        }
    }

    /**
     * Invoke an action on each member of the set.
     *
     * @param action The action to invoke
     */
    public void forEach(@NotNull final Consumer<? super TYPE> action) {
        if (storage == null) {
            return;
        }
        for (int slotIndex = 0, usedSlotsConsumed = 0; slotIndex < storage.length
            && usedSlotsConsumed < usedSlots; ++slotIndex) {
            final WeakReference<? extends TYPE> memberReference = storage[slotIndex];
            if (memberReference == null || memberReference == DELETED_REFERENCE) {
                continue;
            }

            final TYPE member = memberReference.get();
            if (member == null) {
                markSlotDeleted(slotIndex);
            } else {
                ++usedSlotsConsumed;
                action.accept(member);
            }
        }
    }

    /**
     * Thread-safe implementation.
     */
    public static class Synchronized<TYPE> extends WeakIdentityHashSet<TYPE> {

        @Override
        public synchronized void clear() {
            super.clear();
        }

        @Override
        public synchronized boolean add(@NotNull final TYPE value) {
            return super.add(value);
        }

        @Override
        public synchronized boolean add(@NotNull final TYPE value,
            @Nullable final WeakReference<? extends TYPE> valueReference) {
            return super.add(value, valueReference);
        }

        public synchronized void forEach(@NotNull final Consumer<? super TYPE> action) {
            super.forEach(action);
        }
    }

    /**
     * Special WeakReference sub-class for the {@link #DELETED_REFERENCE}.
     */
    private static final class DeletedReference extends WeakReference<Object> {

        private DeletedReference() {
            super(new Object());
            clear();
        }

        @Override
        public final Object get() {
            throw new IllegalStateException("DeletedReference cannot be dereferenced");
        }
    }
}
