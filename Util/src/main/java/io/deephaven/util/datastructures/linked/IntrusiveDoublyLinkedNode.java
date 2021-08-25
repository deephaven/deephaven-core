package io.deephaven.util.datastructures.linked;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for allowing sub-interfaces to enforce a common interface for intrusive doubly-linked
 * nodes.
 */
public interface IntrusiveDoublyLinkedNode<NODE_TYPE extends IntrusiveDoublyLinkedNode<NODE_TYPE>> {

    /**
     * Get the next node after this node.
     *
     * @return This node's next node
     */
    @NotNull
    NODE_TYPE getNext();

    /**
     * Set this node's next node.
     *
     * @param other This node's new next node
     */
    void setNext(@NotNull NODE_TYPE other);

    /**
     * Get the previous node before this node.
     *
     * @return This node's previous node
     */
    @NotNull
    NODE_TYPE getPrev();

    /**
     * Set this node's previous node.
     *
     * @param other This node's new previous node
     */
    void setPrev(@NotNull NODE_TYPE other);

    /**
     * Basic implementation for classes that can simply extend it rather than implement the
     * interface directly.
     */
    class Impl<NODE_TYPE extends Impl<NODE_TYPE>> implements IntrusiveDoublyLinkedNode<NODE_TYPE> {

        private NODE_TYPE next;
        private NODE_TYPE prev;

        protected Impl() {
            // noinspection unchecked
            next = prev = (NODE_TYPE) this;
        }

        @NotNull
        @Override
        public NODE_TYPE getNext() {
            return next;
        }

        @Override
        public void setNext(@NotNull final NODE_TYPE other) {
            next = other;
        }

        @NotNull
        @Override
        public NODE_TYPE getPrev() {
            return prev;
        }

        @Override
        public void setPrev(@NotNull final NODE_TYPE other) {
            prev = other;
        }
    }

    /**
     * Generic {@link IntrusiveDoublyLinkedStructureBase.Adapter} usable with any implementing
     * class.
     */
    class Adapter<NODE_TYPE extends IntrusiveDoublyLinkedNode<NODE_TYPE>>
        implements IntrusiveDoublyLinkedStructureBase.Adapter<NODE_TYPE> {

        private static final IntrusiveDoublyLinkedStructureBase.Adapter<?> INSTANCE =
            new Adapter<>();

        public static <NODE_TYPE extends IntrusiveDoublyLinkedNode<NODE_TYPE>> IntrusiveDoublyLinkedStructureBase.Adapter<NODE_TYPE> getInstance() {
            // noinspection unchecked
            return (IntrusiveDoublyLinkedStructureBase.Adapter<NODE_TYPE>) INSTANCE;
        }

        private Adapter() {}

        @NotNull
        @Override
        public NODE_TYPE getNext(@NotNull final NODE_TYPE node) {
            return node.getNext();
        }

        @Override
        public void setNext(@NotNull final NODE_TYPE node, @NotNull final NODE_TYPE other) {
            node.setNext(other);
        }

        @NotNull
        @Override
        public NODE_TYPE getPrev(@NotNull final NODE_TYPE node) {
            return node.getPrev();
        }

        @Override
        public void setPrev(@NotNull final NODE_TYPE node, @NotNull final NODE_TYPE other) {
            node.setPrev(other);
        }
    }
}
