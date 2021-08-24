package io.deephaven.util.datastructures.linked;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * A simple queue based on circular intrusive doubly linked nodes (for O(1) random removal).
 */
public class IntrusiveDoublyLinkedQueue<VALUE_TYPE>
    extends IntrusiveDoublyLinkedStructureBase<VALUE_TYPE> implements Iterable<VALUE_TYPE> {

    /**
     * The head of the queue, or null if the queue is empty
     */
    private VALUE_TYPE head;

    /**
     * The size of the queue.
     */
    private int size;

    /**
     * Construct a new queue
     *
     * @param adapter The adapter for updating a node's next and previous nodes.
     */
    public IntrusiveDoublyLinkedQueue(@NotNull final Adapter<VALUE_TYPE> adapter) {
        super(adapter);
    }

    /**
     * Test if the queue is empty.
     *
     * @return Whether the queue is empty
     */
    public final boolean isEmpty() {
        return head == null;
    }

    /**
     * Get the size of this queue.
     *
     * @return The size of the queue
     */
    public final int size() {
        return size;
    }

    /**
     * Move all nodes from {@code other} to the front of this queue in O(1) time.
     *
     * @param other The queue to transfer from
     */
    public final void transferBeforeHeadFrom(
        @NotNull final IntrusiveDoublyLinkedQueue<VALUE_TYPE> other) {
        transferFrom(other, true);
    }

    /**
     * Move all nodes from {@code other} to the back of this queue in O(1) time.
     *
     * @param other The queue to transfer from
     */
    public final void transferAfterTailFrom(
        @NotNull final IntrusiveDoublyLinkedQueue<VALUE_TYPE> other) {
        transferFrom(other, false);
    }

    /**
     * Move all nodes from {@code other} to this queue in O(1) time.
     *
     * @param other The queue to transfer from
     * @param front Whether to add {@code other}'s elements at the front (instead of the back) of
     *        this queue
     */
    private void transferFrom(@NotNull final IntrusiveDoublyLinkedQueue<VALUE_TYPE> other,
        final boolean front) {
        if (!compatible(other)) {
            throw new UnsupportedOperationException(
                this + ": Attempted to transfer from incompatible queue " + other);
        }
        if (other.isEmpty()) {
            return;
        }
        if (isEmpty()) {
            head = other.head;
        } else {
            final VALUE_TYPE tail = getPrev(head);
            final VALUE_TYPE otherTail = getPrev(other.head);
            setNext(tail, other.head);
            setPrev(other.head, tail);
            setNext(otherTail, head);
            setPrev(head, otherTail);
            if (front) {
                head = other.head;
            }
        }
        size += other.size;
        other.head = null;
        other.size = 0;
    }

    /**
     * Add a node at the end of the queue.
     *
     * @param node The node to add
     * @return true
     */
    @SuppressWarnings("UnusedReturnValue")
    public final boolean offer(@NotNull final VALUE_TYPE node) {
        if (isEmpty()) {
            head = node;
        } else {
            linkBefore(node, head);
        }
        ++size;
        return true;
    }

    /**
     * Add a node at the the specified offset into the queue. This is necessarily an O(n) operation.
     *
     * @param node The node to add
     * @param offset The offset (in [0, size()] to add the node at
     */
    public final void insert(@NotNull final VALUE_TYPE node, final int offset) {
        if (offset < 0 || offset > size) {
            throw new IllegalArgumentException(
                "Invalid offset " + offset + ", must be in [0, size(" + size + ")]");
        }

        if (offset == size) {
            offer(node);
            return; // NB: offer(VALUE_TYPE) already updates size
        }

        VALUE_TYPE curr = head;
        for (int currOffset = 0; currOffset < offset; ++currOffset) {
            curr = getNext(curr);
        }
        linkBefore(node, curr);
        if (curr == head) {
            head = node;
        }
        ++size;
    }

    /**
     * Get the node at the front of the queue without modifying the queue.
     *
     * @return The current head of the queue, or null if the queue is empty
     */
    @SuppressWarnings("unused")
    public @Nullable final VALUE_TYPE peek() {
        return head;
    }

    /**
     * Remove and get the node at the front of the queue.
     *
     * @return The node at the head of the queue (now removed)
     */
    public @Nullable final VALUE_TYPE poll() {
        if (isEmpty()) {
            return null;
        }
        final VALUE_TYPE node = head;
        head = isLinked(head) ? getNext(head) : null;
        --size;
        return unlink(node);
    }

    /**
     * Remove and get the node at the front of the queue.
     *
     * @return The node at the head of the queue (now removed)
     * @throws NoSuchElementException If the queue is empty
     */
    public @NotNull final VALUE_TYPE remove() {
        final VALUE_TYPE result = poll();
        if (result == null) {
            throw new NoSuchElementException();
        }
        // NB: Size is decremented in poll().
        return result;
    }

    /**
     * Remove a node from the queue.
     *
     * @param node The node to remove
     * @return Whether the node was in the queue (and now removed)
     */
    public final boolean remove(@NotNull final VALUE_TYPE node) {
        if (head == node) {
            if (isLinked(node)) {
                head = getNext(node);
                unlink(node);
            } else {
                head = null;
            }
        } else if (isLinked(node)) {
            unlink(node);
        } else {
            return false;
        }
        --size;
        return true;
    }

    /**
     * Remove and unlink all nodes in the queue.
     */
    public final void clear() {
        // noinspection StatementWithEmptyBody
        while (null != poll());
    }

    /**
     * Remove all nodes in the queue, without unlinking anything. This is suitable for nodes that
     * will be discarded.
     */
    public final void clearFast() {
        head = null;
        size = 0;
    }

    /**
     * Determine if a node is currently in the queue. Assumes that the node's prev/next pointers are
     * only used in this queue.
     *
     * @param node The node
     * @return Whether the node is currently in the queue
     */
    public final boolean contains(@NotNull final VALUE_TYPE node) {
        return head == node || isLinked(node);
    }

    private class IteratorImpl implements Iterator<VALUE_TYPE> {

        private VALUE_TYPE next = head;
        private VALUE_TYPE current = null;

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public VALUE_TYPE next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            current = next;
            final VALUE_TYPE currentNext = getNext(current);
            next = currentNext == head ? null : currentNext;
            return current;
        }

        @Override
        public void remove() {
            if (current == null) {
                throw new IllegalStateException();
            }
            IntrusiveDoublyLinkedQueue.this.remove(current);
            current = null;
        }
    }

    @NotNull
    @Override
    public Iterator<VALUE_TYPE> iterator() {
        return new IteratorImpl();
    }

    @Override
    public Spliterator<VALUE_TYPE> spliterator() {
        return Spliterators.spliterator(iterator(), size(),
            Spliterator.ORDERED | Spliterator.NONNULL); // Implicitly | SIZED | SUBSIZED, too.

    }

    @NotNull
    public Stream<VALUE_TYPE> stream() {
        return StreamSupport.stream(spliterator(), false);
    }
}
