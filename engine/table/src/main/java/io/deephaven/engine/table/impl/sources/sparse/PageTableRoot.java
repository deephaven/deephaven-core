//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.Nullable;

import java.util.function.ToLongFunction;

/**
 * The top level of an adaptive sparse source's page table. Its children are {@link PageTableLevel}s, held in fields of
 * that class, so that reading one needs no type check.
 *
 * @param <LEAF> the type of the children of the {@link PageTableLevel}s
 */
public final class PageTableRoot<LEAF> {
    private static final int NO_CHILD = -1;
    private static final int MANY_CHILDREN = -2;

    private final int capacity;
    /** The recycler the array of children is borrowed from, as for previous values, or null to allocate it. */
    private final SoftRecycler<PageTableLevel<?>[]> recycler;
    private int oneIndex = NO_CHILD;
    private PageTableLevel<?> oneChild;
    private PageTableLevel<?>[] children;

    /**
     * @param capacity the number of children the level addresses
     * @param recycler the recycler to borrow the array of children from, whose arrays have {@code capacity} entries, or
     *        null to allocate it
     */
    public PageTableRoot(final int capacity, @Nullable final SoftRecycler<PageTableLevel<?>[]> recycler) {
        this.capacity = capacity;
        this.recycler = recycler;
    }

    /**
     * @return the child at {@code index}, or null if there is none
     */
    @SuppressWarnings("unchecked")
    public PageTableLevel<LEAF> get(final int index) {
        if (oneIndex == index) {
            return (PageTableLevel<LEAF>) oneChild;
        }
        return children == null ? null : (PageTableLevel<LEAF>) children[index];
    }

    /**
     * Make {@code child} the child at {@code index}; a null child clears it.
     */
    public void set(final int index, final PageTableLevel<LEAF> child) {
        if (oneIndex == index || oneIndex == NO_CHILD) {
            oneIndex = index;
            oneChild = child;
            return;
        }
        if (children == null) {
            if (child == null) {
                // a level with one child holds no other child to clear
                return;
            }
            children = recycler == null ? new PageTableLevel<?>[capacity] : recycler.borrowItem();
            children[oneIndex] = oneChild;
            oneChild = null;
            oneIndex = MANY_CHILDREN;
        }
        children[index] = child;
    }

    /**
     * Return the array of children, whose entries must all be null, to its recycler.
     */
    public void recycleArray() {
        if (children != null) {
            recycler.returnItem(children);
            children = null;
        }
    }

    /**
     * Release every child.
     */
    public void clear() {
        oneIndex = NO_CHILD;
        oneChild = null;
        children = null;
    }

    /**
     * @param childSize the estimated size of a child, in bytes
     * @return the estimated size of this level and its children, in bytes
     */
    @SuppressWarnings("unchecked")
    public long estimateSize(final ToLongFunction<PageTableLevel<LEAF>> childSize) {
        if (children == null) {
            return oneChild == null ? 0 : childSize.applyAsLong((PageTableLevel<LEAF>) oneChild);
        }
        long size = (long) children.length * Long.BYTES;
        for (final PageTableLevel<?> child : children) {
            if (child != null) {
                size += childSize.applyAsLong((PageTableLevel<LEAF>) child);
            }
        }
        return size;
    }
}
