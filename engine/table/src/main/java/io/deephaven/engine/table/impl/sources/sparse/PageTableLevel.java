//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.util.SoftRecycler;
import org.jetbrains.annotations.Nullable;

import java.util.function.ToLongFunction;

/**
 * An upper level of an adaptive sparse source's page table, holding the children for one range of row keys. Like the
 * levels of the sparse array sources, a level holds its first child without allocating an array of children.
 *
 * <p>
 * The children are kept as {@code Object}s, so that every source shares this code; each source casts a child to its own
 * level class where it reads one. The level above a {@code PageTableLevel} is a {@link PageTableRoot}, whose children
 * are held as {@code PageTableLevel}s and so need no such cast.
 *
 * @param <CHILD> the type of the children
 */
public final class PageTableLevel<CHILD> {
    private static final int NO_CHILD = -1;
    private static final int MANY_CHILDREN = -2;

    private final int capacity;
    /** The recycler the array of children is borrowed from, as for previous values, or null to allocate it. */
    private final SoftRecycler<Object[]> recycler;
    private int oneIndex = NO_CHILD;
    private Object oneChild;
    private Object[] children;

    /**
     * @param capacity the number of children the level addresses
     * @param recycler the recycler to borrow the array of children from, whose arrays have {@code capacity} entries, or
     *        null to allocate it
     */
    public PageTableLevel(final int capacity, @Nullable final SoftRecycler<Object[]> recycler) {
        this.capacity = capacity;
        this.recycler = recycler;
    }

    /**
     * @return the child at {@code index}, or null if there is none
     */
    @SuppressWarnings("unchecked")
    public CHILD get(final int index) {
        if (oneIndex == index) {
            return (CHILD) oneChild;
        }
        return children == null ? null : (CHILD) children[index];
    }

    /**
     * Make {@code child} the child at {@code index}; a null child clears it.
     */
    public void set(final int index, final CHILD child) {
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
            children = recycler == null ? new Object[capacity] : recycler.borrowItem();
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
    public long estimateSize(final ToLongFunction<CHILD> childSize) {
        if (children == null) {
            return oneChild == null ? 0 : childSize.applyAsLong((CHILD) oneChild);
        }
        long size = (long) children.length * Long.BYTES;
        for (final Object child : children) {
            if (child != null) {
                size += childSize.applyAsLong((CHILD) child);
            }
        }
        return size;
    }
}
