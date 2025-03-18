//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Implementation of {@link WeakReferenceManager} backed by an {@link ArrayList} (when not concurrent) or a
 * {@link CopyOnWriteArrayList} (when concurrent). Users should note the potential for poor performance when adding a
 * large number of items to a concurrent ArrayWeakReferenceManager.
 */
public class ArrayWeakReferenceManager<T> implements WeakReferenceManager<T> {

    private final List<WeakReference<T>> refs;

    /**
     * Construct a WeakReferenceManager as with {@code new ArrayWeakReferenceManager(true)}.
     */
    public ArrayWeakReferenceManager() {
        this(true);
    }

    /**
     * Construct a WeakReferenceManager.
     *
     * @param concurrent Use {@link CopyOnWriteArrayList} if {@code true}, else {@link ArrayList}.
     */
    public ArrayWeakReferenceManager(final boolean concurrent) {
        refs = concurrent ? new CopyOnWriteArrayList<>() : new ArrayList<>();
    }

    @Override
    public void add(final T item) {
        refs.add(new WeakReference<>(item));
    }

    @Override
    public void remove(final T item) {
        if (refs.isEmpty()) {
            return;
        }
        refs.removeIf((ref) -> ref.get() == null || ref.get() == item);
    }

    @Override
    public void removeAll(@NotNull final Collection<T> items) {
        if (refs.isEmpty()) {
            return;
        }
        final Set<T> itemsIdentitySet = Collections.newSetFromMap(new IdentityHashMap<>(items.size()));
        itemsIdentitySet.addAll(items);
        refs.removeIf(ref -> ref.get() == null || itemsIdentitySet.contains(ref.get()));
    }

    @Override
    public void forEachValidReference(@NotNull final Consumer<T> consumer, final boolean parallel) {
        if (refs.isEmpty()) {
            return;
        }
        if (parallel) {
            parallelStream().forEach(consumer);
        } else {
            iterator().forEachRemaining(consumer);
        }
    }

    @Override
    public T getFirst(@NotNull final Predicate<T> test) {
        return stream().filter(test).findFirst().orElse(null);
    }

    @Override
    public boolean isEmpty() {
        return refs.isEmpty();
    }

    @Override
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

    private Iterator<T> iterator() {
        return new IteratorImpl();
    }

    private Stream<T> stream() {
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

    private Stream<T> parallelStream() {
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
