//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Intrusive doubly-linked list implementation of {@link WeakReferenceManager}. This implementation is internally
 * synchronized to ensure that it is always safe for concurrent use, but operations may block.
 */
public class LinkedWeakReferenceManager<T> implements WeakReferenceManager<T> {

    private static final class Node<T> extends WeakReference<T> implements IntrusiveDoublyLinkedNode<Node<T>> {

        private Node<T> next;
        private Node<T> prev;

        public Node(final T referent) {
            super(referent);
            next = prev = this;
        }

        @Override
        public @NotNull Node<T> getNext() {
            return next;
        }

        @Override
        public void setNext(@NotNull final Node<T> other) {
            next = other;
        }

        @Override
        public @NotNull Node<T> getPrev() {
            return prev;
        }

        @Override
        public void setPrev(@NotNull final Node<T> other) {
            prev = other;
        }
    }

    private final IntrusiveDoublyLinkedQueue<Node<T>> refs;

    /**
     * Create a LinkedWeakReferenceManager.
     */
    public LinkedWeakReferenceManager() {
        refs = new IntrusiveDoublyLinkedQueue<>(IntrusiveDoublyLinkedNode.Adapter.<Node<T>>getInstance());
    }

    @Override
    public synchronized void add(final T item) {
        refs.offer(new Node<>(item));
    }

    @Override
    public synchronized void remove(final T item) {
        if (refs.isEmpty()) {
            return;
        }
        for (final Iterator<T> iterator = iterator(); iterator.hasNext();) {
            if (item == iterator.next()) {
                iterator.remove();
            }
        }
    }

    @Override
    public synchronized void removeAll(@NotNull final Collection<T> items) {
        if (refs.isEmpty()) {
            return;
        }
        final Set<T> itemsIdentitySet = Collections.newSetFromMap(new IdentityHashMap<>(items.size()));
        itemsIdentitySet.addAll(items);
        for (final Iterator<T> iterator = iterator(); iterator.hasNext();) {
            if (itemsIdentitySet.contains(iterator.next())) {
                iterator.remove();
            }
        }
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
    public synchronized T getFirst(@NotNull final Predicate<T> test) {
        return stream().filter(test).findFirst().orElse(null);
    }

    @Override
    public synchronized boolean isEmpty() {
        return refs.isEmpty();
    }

    /**
     * Clear the list of references.
     */
    @Override
    public synchronized void clear() {
        refs.clear();
    }

    private class IteratorImpl implements Iterator<T> {

        private final Iterator<Node<T>> refsIterator;

        private T next;

        private IteratorImpl() {
            this.refsIterator = refs.iterator();
        }

        @Override
        public boolean hasNext() {
            boolean exhausted;
            while ((exhausted = next == null) && refsIterator.hasNext()) {
                final Node<T> ref = refsIterator.next();
                final T item = ref.get();
                if (item != null) {
                    next = item;
                } else {
                    refsIterator.remove();
                }
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

        @Override
        public void remove() {
            refsIterator.remove();
        }
    }

    private Iterator<T> iterator() {
        return new IteratorImpl();
    }

    private Stream<T> stream() {
        if (refs.isEmpty()) {
            return Stream.empty();
        }
        return StreamSupport.stream(Spliterators.spliterator(iterator(), refs.size(), Spliterator.NONNULL), false);
    }

    private Stream<T> parallelStream() {
        if (refs.isEmpty()) {
            return Stream.empty();
        }
        return StreamSupport.stream(Spliterators.spliterator(iterator(), refs.size(), Spliterator.NONNULL), true);
    }
}
