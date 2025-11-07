//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

import java.util.Collection;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A helper for managing a list of weakly-reachable items. Implementations hide the internal management of cleared
 * references and provide for iteration over the valid (still reachable) items.
 */
public interface WeakReferenceManager<T> {

    /**
     * Add {@code item} to this WeakReferenceManager.
     *
     * @param item The item to add
     */
    void add(T item);

    /**
     * Remove {@code item} from this WeakReferenceManager, along with any cleared references. Comparison is strictly by
     * reference equality, so {@code item} must be the same object as one previously {@link #add(Object) added}.
     *
     * @param item The item to remove
     */
    void remove(T item);

    /**
     * Remove all items in {@code items} from this WeakReferenceManager, along with any cleared references. Comparison
     * is strictly by reference equality.
     *
     * @param items The items to remove
     */
    void removeAll(@NotNull Collection<T> items);

    /**
     * Invoke {@code consumer.accept(item)} for each item previously added to this WeakReferenceManager whose reference
     * has not been cleared (i.e. because it was garbage-collected). Removes cleared references as a side effect. This
     * overload is the same as {@code forEachValidReference(consumer, false)}; that is, it does not parallelize.
     *
     * @param consumer The consumer to call for each valid item
     */
    default void forEachValidReference(@NotNull final Consumer<T> consumer) {
        forEachValidReference(consumer, false);
    }

    /**
     * Invoke {@code consumer.accept(item)} for each item previously added to this WeakReferenceManager whose reference
     * has not been cleared (i.e. because it was garbage-collected). Removes cleared references as a side effect.
     *
     * @param consumer The consumer to call for each valid item
     * @param parallel Whether to attempt to invoke {@code consumer} in parallel if the implementation supports this
     */
    void forEachValidReference(@NotNull Consumer<T> consumer, boolean parallel);

    /**
     * Retrieve the first valid item for which {@code test.test(item)} returns {@code true}. Any cleared references
     * encountered are removed as a side effect.
     *
     * @param test The test to decide if a valid item should be returned
     * @return The first valid item that passed {@code test}, or {@code null} if no such item existed
     */
    T getFirst(@NotNull Predicate<T> test);

    /**
     * Check whether this WeakReferenceManager is empty. Does not check for cleared references, and so a {@code true}
     * return does not guarantee that items will be encountered by subsequent methods.
     *
     * @return Whether this WeakReferenceManager is empty
     */
    boolean isEmpty();

    /**
     * Clear all items from this WeakReferenceManager.
     */
    void clear();
}
