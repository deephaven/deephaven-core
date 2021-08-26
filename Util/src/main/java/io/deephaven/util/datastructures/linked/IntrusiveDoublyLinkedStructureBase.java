package io.deephaven.util.datastructures.linked;

import org.jetbrains.annotations.NotNull;

/**
 * Circular doubly-linked structure base. Intended as a utility for building other structures.
 *
 * Note that "unlinked" nodes should have themselves as their next and previous nodes.
 */
@SuppressWarnings({"WeakerAccess", "unused", "UnusedReturnValue"})
public abstract class IntrusiveDoublyLinkedStructureBase<VALUE_TYPE> {

    /**
     * Adapter interface for nodes with intrusively-stored previous and next nodes.
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
        @NotNull
        NODE_TYPE getNext(@NotNull NODE_TYPE node);

        /**
         * Set the input node's next node.
         * 
         * @param node The input node
         * @param other The input node's new next node
         */
        void setNext(@NotNull NODE_TYPE node, @NotNull NODE_TYPE other);

        /**
         * Get the previous node before the input node.
         * 
         * @param node The input node
         * @return The input node's previous node
         */
        @NotNull
        NODE_TYPE getPrev(@NotNull NODE_TYPE node);

        /**
         * Set the input node's previous node.
         * 
         * @param node The input node
         * @param other The input node's new previous node
         */
        void setPrev(@NotNull NODE_TYPE node, @NotNull NODE_TYPE other);
    }

    /**
     * The adapter for updating a node's next and previous nodes.
     */
    private @NotNull final Adapter<VALUE_TYPE> adapter;

    /**
     * Constructor, for sub-class use only.
     * 
     * @param adapter The adapter for updating a node's next and previous nodes.
     */
    protected IntrusiveDoublyLinkedStructureBase(@NotNull final Adapter<VALUE_TYPE> adapter) {
        this.adapter = adapter;
    }

    /**
     * Get the next node after the input node.
     * 
     * @param node The input node
     * @return The input node's next node
     */
    protected @NotNull final VALUE_TYPE getNext(@NotNull final VALUE_TYPE node) {
        return adapter.getNext(node);
    }

    /**
     * Set the input node's next node.
     * 
     * @param node The input node
     * @param other The input node's new next node
     */
    protected final void setNext(@NotNull final VALUE_TYPE node, @NotNull final VALUE_TYPE other) {
        adapter.setNext(node, other);
    }

    /**
     * Get the previous node before the input node.
     * 
     * @param node The input node
     * @return The input node's previous node
     */
    protected @NotNull final VALUE_TYPE getPrev(@NotNull final VALUE_TYPE node) {
        return adapter.getPrev(node);
    }

    /**
     * Set the input node's previous node.
     * 
     * @param node The input node
     * @param other The input node's new previous node
     */
    protected final void setPrev(@NotNull final VALUE_TYPE node, @NotNull final VALUE_TYPE other) {
        adapter.setPrev(node, other);
    }

    /**
     * Test if a node is part of a structure.
     * 
     * @param node The node to test
     * @return Whether the node is part of a structure (i.e. if its next node is not itself)
     */
    protected final boolean isLinked(@NotNull final VALUE_TYPE node) {
        return adapter.getNext(node) != node;
    }

    /**
     * Insert a node before another node.
     * 
     * @param node The node to insert
     * @param other The node to insert before
     * @return node
     */
    protected @NotNull final VALUE_TYPE linkBefore(@NotNull final VALUE_TYPE node, @NotNull final VALUE_TYPE other) {
        setNext(node, other);
        setPrev(node, getPrev(other));
        setNext(getPrev(other), node);
        setPrev(other, node);
        return node;
    }

    /**
     * Insert a node after another node.
     * 
     * @param node The node to insert
     * @param other The node to insert after
     * @return node
     */
    protected @NotNull final VALUE_TYPE linkAfter(@NotNull final VALUE_TYPE node, @NotNull final VALUE_TYPE other) {
        setPrev(node, other);
        setNext(node, getNext(other));
        setPrev(getNext(other), node);
        setNext(other, node);
        return node;
    }

    /**
     * Remove a node from a structure.
     * 
     * @param node The node to remove
     * @return node
     */
    protected @NotNull final VALUE_TYPE unlink(@NotNull final VALUE_TYPE node) {
        setNext(getPrev(node), getNext(node));
        setPrev(getNext(node), getPrev(node));
        setNext(node, node);
        setPrev(node, node);
        return node;
    }

    /**
     * Is the other data structure compatible with this one? This is true if and only if it's the same class, with the
     * same adapter instance.
     *
     * @param other The other data structure
     * @return Whether other is compatible
     */
    protected final boolean compatible(@NotNull final IntrusiveDoublyLinkedStructureBase<VALUE_TYPE> other) {
        return getClass() == other.getClass() && adapter == other.adapter;
    }
}
