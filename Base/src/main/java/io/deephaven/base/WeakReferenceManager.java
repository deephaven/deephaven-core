//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * A helper for manging a list of WeakReferences. It hides the internal management of expired references and provides
 * for iteration over the valid ones
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
     * Create a WeakReferenceManager, with either {@link ArrayList} or {@link CopyOnWriteArrayList} as backing
     * structure.
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
     * Execute the provided procedure on each listener that has not been GC'd. If a listener was GC'd the reference will
     * be removed from the internal list of refs.
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

    private class IteratorImpl implements Iterator<T> {

        private final Iterator<WeakReference<T>> refsIterator;

        private T next;
        private List<WeakReference<T>> expiredRefs;

        private IteratorImpl() {
            this.refsIterator = refs.iterator();
        }

        private void accumulateExpiredRef(@NotNull final WeakReference<T> ref) {
            if (expiredRefs == null) {
                expiredRefs = new ArrayList<>();
            }
            expiredRefs.add(ref);
        }

        private void maybeRemoveExpiredRefs() {
            if (expiredRefs != null) {
                refs.removeAll(expiredRefs);
                expiredRefs = null;
            }
        }

        @Override
        public boolean hasNext() {
            boolean exhausted;
            while ((exhausted = next == null) && refsIterator.hasNext()) {
                final WeakReference<T> ref = refsIterator.next();
                final T item = ref.get();
                if (item != null) {
                    next = item;
                } else {
                    accumulateExpiredRef(ref);
                }
            }
            if (exhausted) {
                maybeRemoveExpiredRefs();
            }
            return !exhausted;
        }

        @Override
        public T next() {
            if (hasNext()) {
                final T result = next;
                next = null;
                return result;
            }
            throw new NoSuchElementException();
        }
    }

    public Iterator<T> iterator() {
        return new IteratorImpl();
    }

    public Stream<T> stream() {
        if (refs.isEmpty()) {
            return Stream.empty();
        }
        final List<WeakReference<T>> expiredRefs = new ArrayList<>();
        return refs.stream().map(ref -> {
            final T item = ref.get();
            if (item == null) {
                expiredRefs.add(ref);
            }
            return item;
        }).filter(Objects::nonNull).onClose(() -> refs.removeAll(expiredRefs));
    }

    public Stream<T> parallelStream() {
        if (refs.isEmpty()) {
            return Stream.empty();
        }
        final List<WeakReference<T>> expiredRefs = Collections.synchronizedList(new ArrayList<>());
        return refs.parallelStream().map(ref -> {
            final T item = ref.get();
            if (item == null) {
                expiredRefs.add(ref);
            }
            return item;
        }).filter(Objects::nonNull).onClose(() -> refs.removeAll(expiredRefs));
    }
}
