package io.deephaven.base;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * A helper for manging a list of WeakReferences. It hides the internal management of expired
 * references and provides for iteration over the valid ones
 */
public class WeakReferenceManager<T> {
    private final List<WeakReference<T>> refs;

    /**
     * Create a WeakReferenceManager, with {@link CopyOnWriteArrayList} as backing structure.
     */
    public WeakReferenceManager() {
        this(true);
    }

    /**
     * Create a WeakReferenceManager, with either {@link ArrayList} or {@link CopyOnWriteArrayList}
     * as backing structure.
     *
     * @param useCowList Use CopyOnWriteArrayList if true, else ArrayList.
     */
    public WeakReferenceManager(boolean useCowList) {
        refs = useCowList ? new CopyOnWriteArrayList<>() : new ArrayList<>();
    }

    /**
     * Add the specified item to the list.
     *
     * @param item the item to add.
     */
    public void add(T item) {
        refs.add(new WeakReference<>(item));
    }

    /**
     * Remove item from the list if present, and also any expired references.
     *
     * @param item the item to remove.
     */
    public void remove(T item) {
        refs.removeIf((l) -> (l.get() == null) || (l.get() == item));
    }

    /**
     * Remove items in the collection from the list, and also any expired references.
     *
     * @param items the items to remove.
     */
    public void removeAll(Collection<T> items) {
        refs.removeIf(l -> l.get() == null || items.contains(l.get()));
    }

    /**
     * Execute the provided procedure on each listener that has not been GC'd. If a listener was
     * GC'd the reference will be removed from the internal list of refs.
     *
     * @param proc The procedure to call with each valid listener
     */
    public void forEachValidReference(Consumer<T> proc) {
        if (!refs.isEmpty()) {
            ArrayList<WeakReference<T>> expiredRefs = new ArrayList<>();

            try {
                for (WeakReference<T> ref : refs) {
                    T item = ref.get();
                    if (item != null) {
                        proc.accept(item);
                    } else {
                        expiredRefs.add(ref);
                    }
                }
            } finally {
                refs.removeAll(expiredRefs);
            }
        }
    }

    /**
     * Retrieve the first valid ref that satisfies the test
     *
     * @param test The test to decide if a valid ref should be returned
     * @return The first valid ref that passed test
     */
    public T getFirst(Predicate<T> test) {
        if (!refs.isEmpty()) {
            ArrayList<WeakReference<T>> expiredRefs = new ArrayList<>();

            try {
                for (WeakReference<T> ref : refs) {
                    T item = ref.get();
                    if (item != null) {
                        if (test.test(item)) {
                            return item;
                        }
                    } else {
                        expiredRefs.add(ref);
                    }
                }
            } finally {
                refs.removeAll(expiredRefs);
            }
        }

        return null;
    }

    /**
     * Return true if the list is empty. Does not check for expired references.
     *
     * @return true if the list is empty.
     */
    public boolean isEmpty() {
        return refs.isEmpty();
    }

    /**
     * Clear the list of references.
     */
    public void clear() {
        refs.clear();
    }
}
