//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.BLOCK2_SIZE;

/**
 * The limits and helpers that the page table levels of every adaptive sparse source share. A level holds its children
 * either in a window, an array indexed by child index less the index of its first entry, or ranked, densely in child
 * index order. These methods decide how large a window or a ranked level may be, and move children between the forms;
 * none depends on the type of the children.
 */
public final class PageTableLevels {

    private PageTableLevels() {}

    /** The index a level records for its only child when it does not hold exactly one. */
    public static final int NO_CHILD = -1;
    /**
     * The fewest entries {@link #maxWindow(int)} allows: a window may always grow to cover this many entries, even when
     * it holds fewer than {@code MIN_WINDOW / WINDOW_ENTRIES_PER_CHILD} children.
     */
    public static final int MIN_WINDOW = 64;
    /** The most entries a window holds for each child it holds, when that is more than {@link #MIN_WINDOW}. */
    public static final int WINDOW_ENTRIES_PER_CHILD = 8;
    /**
     * The most children a ranked middle or bottom page table level holds. More children span at most
     * {@link #WINDOW_ENTRIES_PER_CHILD} entries each, so a window always holds them.
     */
    public static final int MAX_RANKED = BLOCK2_SIZE / WINDOW_ENTRIES_PER_CHILD;

    /**
     * @return the most entries a window holding {@code count} children may have; a level whose children span more ranks
     *         them instead
     */
    public static int maxWindow(final int count) {
        return Math.max(MIN_WINDOW, WINDOW_ENTRIES_PER_CHILD * count);
    }

    /**
     * @return the length of a window of at most {@code limit} entries for {@code count} children spanning
     *         {@code needed} entries, in place of a window of {@code length} entries: at least twice {@code length}
     *         when the children allow it, so that a window that grows a few entries at a time is copied a logarithmic
     *         number of times
     */
    public static int windowLength(final int needed, final int length, final int count, final int limit) {
        return Math.min(limit, Math.min(maxWindow(count), Math.max(needed, 2 * length)));
    }

    /**
     * @return the first index of a window of {@code newLength} entries, of at most {@code limit}, covering the indices
     *         from {@code low} to {@code high}, exclusive, extended below them if {@code down}, else above them
     */
    public static int windowBase(final int low, final int high, final boolean down, final int newLength,
            final int limit) {
        final int newBase = down ? Math.max(0, high - newLength) : low;
        return Math.min(newBase, limit - newLength);
    }

    /**
     * @return the length of the arrays for {@code size} children of a ranked level whose arrays have length
     *         {@code capacity}: the same if they fit, else at least double, so that a level that grows a few children
     *         at a time is copied a logarithmic number of times
     */
    public static int grownCapacity(final int capacity, final int size) {
        return size <= capacity ? capacity : Math.max(size, Math.min(MAX_RANKED, capacity * 2));
    }

    /**
     * @return the position of the first child held in {@code window}, or -1 if it holds none
     */
    public static int firstHeld(final Object[] window) {
        for (int ii = 0; ii < window.length; ++ii) {
            if (window[ii] != null) {
                return ii;
            }
        }
        return -1;
    }

    /**
     * @return the position of the last child held in {@code window}, or -1 if it holds none
     */
    public static int lastHeld(final Object[] window) {
        for (int ii = window.length - 1; ii >= 0; --ii) {
            if (window[ii] != null) {
                return ii;
            }
        }
        return -1;
    }

    /**
     * @return the number of children held in {@code window}
     */
    public static int countHeld(final Object[] window) {
        int count = 0;
        for (final Object child : window) {
            if (child != null) {
                ++count;
            }
        }
        return count;
    }

    /**
     * Rank the children held in a window together with a new child index outside the window. The indices of the
     * children held and the new index go to {@code rankedIndices}, ascending, and each child held goes to the same
     * position of {@code rankedChildren}; the position of the new index is left null.
     *
     * @param window the children indexed by window, at least one held
     * @param base the index of the first entry of {@code window}
     * @param first the position of the first child held in {@code window}
     * @param last the position of the last child held in {@code window}
     * @param index the new child index, below {@code base} or at least {@code base + window.length}
     * @param rankedIndices receives the ranked indices; at least one longer than the number of children held
     * @param rankedChildren receives the children held at their ranked positions; at least one longer than the number
     *        of children held
     * @return the number of indices ranked, which is one more than the number of children held
     */
    public static <T> int rankWindow(final T[] window, final int base, final int first, final int last,
            final int index, final int[] rankedIndices, final T[] rankedChildren) {
        int ri = 0;
        if (index < base + first) {
            rankedIndices[ri++] = index;
        }
        for (int ii = first; ii <= last; ++ii) {
            if (window[ii] != null) {
                rankedIndices[ri] = base + ii;
                rankedChildren[ri++] = window[ii];
            }
        }
        if (index > base + last) {
            rankedIndices[ri++] = index;
        }
        return ri;
    }

    /**
     * Copy each of {@code children[0, count)}, ranked, to the entry of {@code window} for its index,
     * {@code indices[ii] - base}.
     *
     * @return {@code window}
     */
    public static <T> T[] unrank(final T[] children, final int[] indices, final int count, final T[] window,
            final int base) {
        for (int ii = 0; ii < count; ++ii) {
            window[indices[ii] - base] = children[ii];
        }
        return window;
    }

    /**
     * Copy the entry at each position {@code pp} below {@code count} of {@code from} to position {@code positions[pp]}
     * of {@code to}.
     *
     * @return {@code to}
     */
    public static <T> T[] scatter(final T[] from, final T[] to, final int[] positions, final int count) {
        for (int pp = 0; pp < count; ++pp) {
            to[positions[pp]] = from[pp];
        }
        return to;
    }

    /**
     * Copy the entry at position {@code positions[pp]} of {@code from} to each position {@code pp} below {@code count}
     * of {@code to}.
     *
     * @return {@code to}
     */
    public static <T> T[] gather(final T[] from, final T[] to, final int[] positions, final int count) {
        for (int pp = 0; pp < count; ++pp) {
            to[pp] = from[positions[pp]];
        }
        return to;
    }
}
