package io.deephaven.util.datastructures.linked;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Singly-linked queue. Supports basic queue operations, but not extended Collection methods that would be required by
 * actually implementing java.lang.Queue.
 */
public class IntrusiveSinglyLinkedQueue<VALUE_TYPE> {
    VALUE_TYPE head = null;
    VALUE_TYPE tail = null;
    private long count = 0;

    public boolean offer(@NotNull VALUE_TYPE item) {
        setNext(item, null);
        if (isEmpty()) {
            head = tail = item;
        } else {
            setNext(tail, item);
            tail = item;
        }
        count++;
        return true;
    }

    public VALUE_TYPE poll() {
        if (isEmpty()) {
            return null;
        }
        VALUE_TYPE retval = head;
        head = getNext(head);
        // if now empty, change tail also (head==tail==null)
        if (tail == retval) {
            tail = head;
        }
        count--;
        return retval;
    }

    public VALUE_TYPE peek() {
        if (isEmpty()) {
            return null;
        }
        return head;
    }

    public boolean isEmpty() {
        return head == null;
    }

    public long size() {
        return count;
    }

    /**
     * Adapter interface for nodes with intrusively-stored next nodes.
     * 
     * @param <NODE_TYPE>
     */
    public interface Adapter<NODE_TYPE> {

        /**
         * Get the next node after the input node.
         * 
         * @param node The input node
         * @return The input node's next node
         */
        @Nullable
        NODE_TYPE getNext(@NotNull NODE_TYPE node);

        /**
         * Set the input node's next node.
         * 
         * @param node The input node
         * @param other The input node's new next node
         */
        void setNext(@NotNull NODE_TYPE node, @Nullable NODE_TYPE other);
    }

    /**
     * The adapter for updating a node's next node.
     */
    @NotNull
    private final Adapter<VALUE_TYPE> adapter;

    /**
     * Constructor, for sub-class use only.
     * 
     * @param adapter The adapter for updating a node's next node.
     */
    public IntrusiveSinglyLinkedQueue(@NotNull final Adapter<VALUE_TYPE> adapter) {
        this.adapter = adapter;
    }

    /**
     * Get the next node after the input node.
     * 
     * @param node The input node
     * @return The input node's next node
     */
    @Nullable
    private VALUE_TYPE getNext(@NotNull final VALUE_TYPE node) {
        return adapter.getNext(node);
    }

    /**
     * Set the input node's next node.
     * 
     * @param node The input node
     * @param other The input node's new next node
     */
    private void setNext(@NotNull final VALUE_TYPE node, @Nullable final VALUE_TYPE other) {
        adapter.setNext(node, other);
    }
}
