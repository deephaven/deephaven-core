package io.deephaven.util.datastructures;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * <p>
 * Re-usable data structure for a segmented stack of pooled elements which tries to strike a balance
 * between GC-sensitivity and element reuse.
 * <p>
 * The pool is safe for multi-threaded use, but not highly-concurrent.
 */
public class SegmentedSoftPool<ELEMENT_TYPE> {

    /**
     * The capacity of each segment of the pool.
     */
    private final int segmentCapacity;

    /**
     * The creation procedure for new elements when the pool is exhausted.
     */
    private final Supplier<ELEMENT_TYPE> creationProcedure;

    /**
     * The cleanup procedure for elements returned to the pool.
     */
    private final Consumer<ELEMENT_TYPE> cleanupProcedure;

    /**
     * A reference to the current segment in use.
     */
    private Reference<Segment<ELEMENT_TYPE>> currentSegmentReference;

    /**
     * Create a new pool with the supplied creation and cleanup procedures.
     *
     * @param segmentCapacity The capacity of each segment of this pool. This is the unit of cleanup
     *        for the garbage collector.
     * @param creationProcedure Creation procedure for new elements. If null, all elements must
     *        supplied via {@link #give(java.lang.Object)}.
     * @param cleanupProcedure Cleanup procedure for returned elements. If null, no cleanup will be
     *        performed in {@link #give(java.lang.Object)}.
     */
    public SegmentedSoftPool(final int segmentCapacity,
        @Nullable final Supplier<ELEMENT_TYPE> creationProcedure,
        @Nullable final Consumer<ELEMENT_TYPE> cleanupProcedure) {
        this.segmentCapacity = Require.gtZero(segmentCapacity, "segmentCapacity");
        this.creationProcedure = creationProcedure;
        this.cleanupProcedure = cleanupProcedure;
    }

    /**
     * Take an element from the pool, or make a new one if the pool is exhausted and a creation
     * procedure was supplied at pool construction time. The element belongs to the caller, and the
     * caller may keep it rather than return it to the pool if desired.
     *
     * @return An element from the pool, possibly newly-constructed
     */
    public final ELEMENT_TYPE take() {
        synchronized (this) {
            Segment<ELEMENT_TYPE> currentSegment = getCurrentSegment();
            if (currentSegment != null) {
                if (currentSegment.empty()) {
                    final Segment<ELEMENT_TYPE> previousSegment = currentSegment.getPrev();
                    if (previousSegment != null) {
                        Assert.assertion(previousSegment.full(), "previousSegment.full()");
                        updateCurrentSegment(currentSegment = previousSegment);
                        return currentSegment.take();
                    }
                } else {
                    return currentSegment.take();
                }
            }
        }
        return maybeCreateElement();
    }

    /**
     * Give an element to the pool. Neither the caller nor any other thread may interact with the
     * element again until it has been returned by a subsequent call to {@link #take()}. The element
     * will be cleaned if a cleanup procedure was provided at pool construction time.
     *
     * @param element The element to give to the pool
     */
    public final void give(@NotNull final ELEMENT_TYPE element) {
        maybeCleanElement(Require.neqNull(element, "element"));
        synchronized (this) {
            Segment<ELEMENT_TYPE> currentSegment = getCurrentSegment();
            if (currentSegment == null) {
                updateCurrentSegment(currentSegment = new Segment<>(segmentCapacity, null));
            } else if (currentSegment.full()) {
                Segment<ELEMENT_TYPE> nextSegment = currentSegment.getNext();
                if (nextSegment == null) {
                    nextSegment = new Segment<>(segmentCapacity, currentSegment);
                    currentSegment.setNext(nextSegment);
                } else {
                    Assert.assertion(nextSegment.empty(), "nextSegment.empty()");
                }
                updateCurrentSegment(currentSegment = nextSegment);
            }
            currentSegment.give(element);
        }
    }

    /**
     * Get the current segment, or null if there isn't one.
     *
     * @return The current segment, or null if there is no such segment
     */
    private Segment<ELEMENT_TYPE> getCurrentSegment() {
        return currentSegmentReference == null ? null : currentSegmentReference.get();
    }

    /**
     * Update the current segment reference.
     *
     * @param newCurrentSegment The new current segment
     */
    private void updateCurrentSegment(@NotNull final Segment<ELEMENT_TYPE> newCurrentSegment) {
        currentSegmentReference = newCurrentSegment.getSelfReference();
    }

    /**
     * Create a new element if a creation procedure was specified.
     *
     * @return The new element, if one was made
     */
    private ELEMENT_TYPE maybeCreateElement() {
        if (creationProcedure == null) {
            throw new UnsupportedOperationException(
                "Pool exhausted and no creation procedure supplied");
        }
        return creationProcedure.get();
    }

    /**
     * Clean the element if a cleanup procedure was specified.
     *
     * @param element The element to cleanup
     */
    private void maybeCleanElement(@NotNull final ELEMENT_TYPE element) {
        if (cleanupProcedure != null) {
            cleanupProcedure.accept(element);
        }
    }

    /**
     * A Segment holds a very simple array-backed stack of available elements. It refers softly to
     * the previous segment (if such exists and has not been collected), and strongly to the next
     * segment (if such exists). The main pool structure only keeps a hard reference to the segment
     * while operating on it - otherwise it
     */
    private static class Segment<ELEMENT_TYPE> extends SoftReference<Segment<ELEMENT_TYPE>> {

        /**
         * Storage slots for available elements in this segment of the pool.
         */
        private final ELEMENT_TYPE[] storage;

        /**
         * A re-usable reference to this segment, for when it is the one currently in use to process
         * incoming requests to take elements from or give elements to the pool.
         */
        private final Reference<Segment<ELEMENT_TYPE>> selfReference;

        /**
         * The number of available elements in this segment.
         */
        private int available;

        /**
         * The next segment, if one exists.
         */
        private Segment<ELEMENT_TYPE> next;

        /**
         * Construct a new segment with the given capacity and previous segment.
         *
         * @param capacity The segment capacity
         * @param previous The previous segment, if such exists
         */
        private Segment(final int capacity, @Nullable final Segment<ELEMENT_TYPE> previous) {
            super(previous);
            // noinspection unchecked
            storage = (ELEMENT_TYPE[]) new Object[capacity];
            selfReference = new SoftReference<>(this);
        }

        /**
         * Test if this segment has no available elements.
         *
         * @return Whether a call to {@link #take()} will succeed.
         */
        private boolean empty() {
            return available == 0;
        }

        /**
         * Test if this segment is at capacity.
         *
         * @return Whether a call to {@link #give(java.lang.Object)} will succeed
         */
        private boolean full() {
            return available == storage.length;
        }

        /**
         * Take an element from this segment, which must not empty.
         *
         * @return The taken element
         */
        private ELEMENT_TYPE take() {
            return storage[--available];
        }

        /**
         * Give an element, already cleared if necessary, to this segment, which must not be full.
         *
         * @param element The element to give
         */
        private void give(@NotNull final ELEMENT_TYPE element) {
            storage[available++] = element;
        }

        /**
         * Get a re-usable reference to this segment.
         *
         * @return A re-usable reference to this segment
         */
        private Reference<Segment<ELEMENT_TYPE>> getSelfReference() {
            return selfReference;
        }

        /**
         * Get the next segment.
         *
         * @return The next segment, or null if none such exists
         */
        private Segment<ELEMENT_TYPE> getNext() {
            return next;
        }

        /**
         * Set the next segment. There must not currently be a next segment.
         *
         * @param other The new next segment
         */
        private void setNext(@NotNull final Segment<ELEMENT_TYPE> other) {
            Assert.eqNull(next, "next");
            next = Require.neqNull(other, "other");
        }

        /**
         * Get the previous segment. This may return null either because there was no previous
         * segment, or because all previous segments have been garbage collected.
         *
         * @return The previous segment, or null if none such exists
         */
        private Segment<ELEMENT_TYPE> getPrev() {
            return get();
        }
    }
}
