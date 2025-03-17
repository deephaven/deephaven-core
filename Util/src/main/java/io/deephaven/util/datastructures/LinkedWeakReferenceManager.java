//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures;

import io.deephaven.base.WeakReferenceManager;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedNode;
import io.deephaven.util.datastructures.linked.IntrusiveDoublyLinkedQueue;
import org.jetbrains.annotations.NotNull;

import java.lang.ref.WeakReference;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A helper for manging a list of weakly-reachable references. It hides the internal management of expired references
 * and provides for iteration over the valid ones. This implementation uses synchronization to ensure that it is always
 * safe for concurrent use, but operations may block. Usages of {@link #iterator()}, {@link #stream()}, or
 * {@link #parallelStream()} must be synchronized on the LinkedWeakReferenceManager.
 */
public class LinkedWeakReferenceManager<T> implements WeakReferenceManager<T> {

    private static final class Node<T> extends WeakReference<T> implements IntrusiveDoublyLinkedNode<Node<T>> {

        private Node<T> next;
        private Node<T> prev;

        public Node(final T referent) {
            super(referent);
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

    /**
     * Add the specified item to the list.
     *
     * @param item The item to add
     */
    public synchronized void add(final T item) {
        refs.offer(new Node<>(item));
    }

    /**
     * Remove item from the list if present, and also any expired references.
     *
     * @param item The item to remove
     */
    public synchronized void remove(final T item) {
        for (final Iterator<Node<T>> refsIterator = refs.iterator(); refsIterator.hasNext();) {
            final Node<T> ref = refsIterator.next();
            if (ref.get() == null || ref.get() == item) {
                refsIterator.remove();
            }
        }
    }

    /**
     * Remove items in the collection from the list, and also any expired references.
     *
     * @param items The items to remove
     */
    public synchronized void removeAll(@NotNull final Collection<T> items) {
        for (final Iterator<Node<T>> refsIterator = refs.iterator(); refsIterator.hasNext();) {
            final Node<T> ref = refsIterator.next();
            if (ref.get() == null || items.contains(ref.get())) {
                refsIterator.remove();
            }
        }
    }

    /**
     * Execute the provided procedure on each item that has not been GC'd. If an item was GC'd the reference will be
     * removed from the internal list of refs.
     *
     * @param proc The procedure to call with each valid item
     */
    public synchronized void forEachValidReference(@NotNull final Consumer<T> proc) {
        for (final Iterator<Node<T>> refsIterator = refs.iterator(); refsIterator.hasNext();) {
            final Node<T> ref = refsIterator.next();
            final T item = ref.get();
            if (item != null) {
                proc.accept(item);
            } else {
                refsIterator.remove();
            }
        }
    }

    /**
     * Retrieve the first valid ref that satisfies the test
     *
     * @param test The test to decide if a valid ref should be returned
     * @return The first valid ref that passed {@code test}
     */
    public synchronized T getFirst(@NotNull final Predicate<T> test) {
        if (!refs.isEmpty()) {
            for (final Iterator<Node<T>> refsIterator = refs.iterator(); refsIterator.hasNext();) {
                final Node<T> ref = refsIterator.next();
                final T item = ref.get();
                if (item != null) {
                    if (test.test(item)) {
                        return item;
                    }
                } else {
                    refsIterator.remove();
                }
            }
        }

        return null;
    }

    /**
     * Return true if the list is empty. Does not check for expired references.
     *
     * @return true if the list is empty
     */
    public synchronized boolean isEmpty() {
        return refs.isEmpty();
    }

    /**
     * Clear the list of references.
     */
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
    }

    public Iterator<T> iterator() {
        return new IteratorImpl();
    }

    public synchronized Stream<T> stream() {
        if (refs.isEmpty()) {
            return Stream.empty();
        }
        return StreamSupport.stream(Spliterators.spliterator(iterator(), refs.size(), Spliterator.NONNULL), false);
    }

    public synchronized Stream<T> parallelStream() {
        if (refs.isEmpty()) {
            return Stream.empty();
        }
        return StreamSupport.stream(Spliterators.spliterator(iterator(), refs.size(), Spliterator.NONNULL), true);
    }
}
