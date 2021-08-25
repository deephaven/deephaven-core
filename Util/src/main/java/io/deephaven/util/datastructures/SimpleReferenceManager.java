package io.deephaven.util.datastructures;

import io.deephaven.base.reference.SimpleReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A helper for manging a list of References. It hides the internal management of expired references
 * and provides for iteration over the valid ones
 */
public final class SimpleReferenceManager<T, R extends SimpleReference<T>> {

    private final Function<T, R> referenceFactory;
    private final Collection<R> references;

    /**
     * Create a SimpleReferenceManager, with {@link CopyOnWriteArrayList} as backing structure.
     *
     * @param referenceFactory Factory to create references for added referents; should always make
     *        a unique reference
     */
    public SimpleReferenceManager(@NotNull final Function<T, R> referenceFactory) {
        this(referenceFactory, true);
    }

    /**
     * Create a SimpleReferenceManager, with either {@link ArrayList} or
     * {@link CopyOnWriteArrayList} as backing structure.
     *
     * @param referenceFactory Factory to create references for added referents; should always make
     *        a unique reference
     * @param concurrent Use CopyOnWriteArrayList for internal storage if true, else ArrayList
     */
    public SimpleReferenceManager(@NotNull final Function<T, R> referenceFactory,
        final boolean concurrent) {
        this.referenceFactory = referenceFactory;
        references = concurrent ? new CopyOnWriteArrayList<>() : new ArrayList<>();
    }

    /**
     * Add the specified item to the list.
     *
     * @param item the item to add.
     */
    public R add(@NotNull final T item) {
        final R ref = referenceFactory.apply(item);
        references.add(ref);
        return ref;
    }

    /**
     * Remove item from the list if present according to reference equality ({@code ==}), and also
     * any cleared references.
     *
     * @param item the item to remove.
     * @return The item if it was removed, else null
     */
    public T remove(@NotNull final T item) {
        return removeIf((final T found) -> found == item) ? item : null;
    }

    /**
     * Remove items in the collection from the list, and also any cleared references.
     *
     * @param items the items to remove.
     */
    public void removeAll(@NotNull final Collection<T> items) {
        removeIf(items::contains);
    }

    /**
     * Retrieve all encountered items that satisfy a filter, while also removing any cleared
     * references.
     *
     * @param filter The filter to decide if a valid item should be removed
     * @return Whether we succeeded in removing anything
     */
    public boolean removeIf(@NotNull final Predicate<T> filter) {
        if (references.isEmpty()) {
            return false;
        }
        Deque<R> collected = null;
        boolean found = false;
        try {
            for (final R ref : references) {
                final T item = ref.get();
                boolean accepted = false;
                if (item == null || (accepted = filter.test(item))) {
                    ref.clear();
                    (collected = maybeMakeRemovalDeque(collected)).add(ref);
                }
                found |= accepted;
            }
        } finally {
            maybeDoRemoves(collected);
        }
        return found;
    }

    /**
     * Execute the provided procedure on each reference, item pair whose item is still reachable,
     * while removing any cleared references.
     *
     * @param consumer The function to call with each reachable pair
     */
    public void forEach(@NotNull final BiConsumer<R, T> consumer) {
        if (references.isEmpty()) {
            return;
        }
        Deque<R> collected = null;
        try {
            for (final R ref : references) {
                final T item = ref.get();
                if (item != null) {
                    consumer.accept(ref, item);
                } else {
                    (collected = maybeMakeRemovalDeque(collected)).add(ref);
                }
            }
        } finally {
            maybeDoRemoves(collected);
        }
    }

    /**
     * Retrieve the first valid item that satisfies a filter. Remove any encountered cleared
     * references as a side effect.
     *
     * @param filter The filter to decide if a valid item should be returned
     * @return The first valid item that passed the filter, or null if no such item exists
     */
    @SuppressWarnings("unused") // NB: Not used, yet. Needed in order to replace some instances of
                                // WeakReferenceManager.
    public T getFirstItem(@NotNull final Predicate<T> filter) {
        if (references.isEmpty()) {
            return null;
        }
        Deque<R> collected = null;
        try {
            for (final R ref : references) {
                final T item = ref.get();
                if (item != null) {
                    if (filter.test(item)) {
                        return item;
                    }
                } else {
                    (collected = maybeMakeRemovalDeque(collected)).add(ref);
                }
            }
        } finally {
            maybeDoRemoves(collected);
        }
        return null;
    }

    /**
     * Retrieve the first valid reference whose item satisfies a filter. Remove any encountered
     * cleared references as a side effect.
     *
     * @param filter The filter to decide if a valid item should be returned
     * @return The first valid item that passed the filter, or null if no such item exists
     */
    public R getFirstReference(@NotNull final Predicate<T> filter) {
        if (references.isEmpty()) {
            return null;
        }
        Deque<R> collected = null;
        try {
            for (final R ref : references) {
                final T item = ref.get();
                if (item != null) {
                    if (filter.test(item)) {
                        return ref;
                    }
                } else {
                    (collected = maybeMakeRemovalDeque(collected)).add(ref);
                }
            }
        } finally {
            maybeDoRemoves(collected);
        }
        return null;
    }

    /**
     * Return true if the list is empty. Does not check for cleared references.
     *
     * @return true if the list is empty.
     */
    public boolean isEmpty() {
        return references.isEmpty();
    }

    /**
     * Clear the list of references.
     */
    public void clear() {
        references.clear();
    }

    private Deque<R> maybeMakeRemovalDeque(@Nullable final Deque<R> current) {
        if (current != null) {
            return current;
        }
        // This is very unusual, in that contains only checks the head of the deque and removes it
        // if it matches.
        // This is particular to the use case in question, because the deque is used as an ordered
        // subset of an ordered
        // data structure for removing items by identity match. Don't try to use this deque for
        // anything else and expect
        // you'll like the results.
        return new ArrayDeque<R>() {
            @Override
            public boolean contains(final Object object) {
                if (peekFirst() == object) {
                    pollFirst();
                    return true;
                }
                return false;
            }
        };
    }

    private void maybeDoRemoves(@Nullable final Deque<R> toRemove) {
        if (toRemove == null || toRemove.isEmpty()) {
            return;
        }
        if (toRemove.size() == 1) {
            references.remove(toRemove.peekFirst());
        } else {
            references.removeAll(toRemove);
        }
    }
}
