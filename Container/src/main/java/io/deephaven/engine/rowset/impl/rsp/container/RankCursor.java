package io.deephaven.engine.rowset.impl.rsp.container;

import static io.deephaven.engine.rowset.impl.rsp.container.ContainerUtil.toIntUnsigned;

/**
 * Rank navigation within a single container that resumes from where the previous query left off.
 *
 * <p>
 * {@link Container#select(int)}, {@link Container#find(short)} and {@link Container#getShortRangeIterator(int)} on a
 * {@link BitmapContainer} count words from the start of the bitmap on every call, and on a {@link RunContainer} count
 * runs from the first run, so a loop asking about consecutive ranks or values in one container does work proportional
 * to the square of the number of queries. This cursor remembers the word or run it last stopped at, together with the
 * number of values before it. A query at or beyond that point continues from there; one behind it starts over.
 * Ascending queries, the common case for bulk operations, cost the container's length once in total.
 *
 * <p>
 * For the other container kinds these queries are already cheap, and the cursor delegates to the container.
 *
 * <p>
 * The cursor's position describes the container as it was when {@link #reset} was called. A container mutated since
 * must be {@link #reset} again before the cursor is used. Cursors are not thread safe.
 */
public final class RankCursor {
    private Container container;
    /** The bitmap words when {@code container} is a {@link BitmapContainer}, null otherwise. */
    private long[] bitmap;
    /** The runs when {@code container} is a {@link RunContainer}, null otherwise. */
    private RunContainer runs;
    /** The word (bitmap) or run (runs) the cursor is positioned at; may equal their count when past the end. */
    private int index;
    /** The number of values in the words or runs before {@code index}. */
    private int cardBefore;

    /**
     * @return whether queries on {@code c} through a cursor save work over the container's own methods
     */
    public static boolean benefits(final Container c) {
        return c instanceof BitmapContainer || c instanceof RunContainer;
    }

    /**
     * Point the cursor at the start of {@code c}.
     */
    public void reset(final Container c) {
        container = c;
        index = 0;
        cardBefore = 0;
        if (c instanceof BitmapContainer) {
            bitmap = ((BitmapContainer) c).bitmap;
            runs = null;
        } else if (c instanceof RunContainer) {
            runs = (RunContainer) c;
            bitmap = null;
        } else {
            bitmap = null;
            runs = null;
        }
    }

    /**
     * @return the container the cursor was last {@link #reset} to, or null
     */
    public Container container() {
        return container;
    }

    /**
     * As {@link Container#select(int)}.
     */
    public short select(final int rank) {
        if (bitmap != null) {
            seekBitmapWordForRank(rank);
            if (index >= bitmap.length) {
                throw new IllegalArgumentException("Insufficient cardinality.");
            }
            return (short) (index * 64 + ContainerUtil.select(bitmap[index], rank - cardBefore));
        }
        if (runs != null) {
            seekRunForRank(rank);
            if (index >= runs.numberOfRuns()) {
                throw new IllegalArgumentException(
                        "Cannot select " + rank + " since cardinality is " + runs.getCardinality());
            }
            return (short) (runs.getValueAsInt(index) + rank - cardBefore);
        }
        return container.select(rank);
    }

    /**
     * As {@link Container#find(short)}.
     */
    public int find(final short x) {
        if (bitmap != null) {
            return bitmapFind(x);
        }
        if (runs != null) {
            return runFind(x);
        }
        return container.find(x);
    }

    /**
     * As {@link Container#getShortRangeIterator(int)}. Prefer {@link Container#getShortRangeIterator(int, RankCursor)}
     * from a call site that sees one container type: the iterator is then created at a single site inside the
     * container's own method, where the JIT can inline and scalar-replace it, rather than at one of several sites here.
     */
    public SearchRangeIterator getShortRangeIterator(final int rank) {
        if (bitmap != null) {
            return new BitmapContainerRangeIterator(bitmap, bitmapWordForRank(rank), rank - cardBefore);
        }
        if (runs != null) {
            return new RunContainerRangeIterator(runs, runForRank(rank), rank - cardBefore);
        }
        return container.getShortRangeIterator(rank);
    }

    /**
     * For a cursor on {@code c}, the bitmap word holding the value of {@code rank}, or the word count when the bitmap
     * holds fewer values than that; {@link #cardBefore()} then gives the number of values before that word.
     */
    int bitmapWordForRank(final BitmapContainer c, final int rank) {
        if (c != container) {
            reset(c);
        }
        return bitmapWordForRank(rank);
    }

    /**
     * For a cursor on {@code c}, the run holding the value of {@code rank}, or the run count when the container holds
     * fewer values than that; {@link #cardBefore()} then gives the number of values before that run.
     */
    int runForRank(final RunContainer c, final int rank) {
        if (c != container) {
            reset(c);
        }
        return runForRank(rank);
    }

    /**
     * @return the number of values before the word or run most recently located
     */
    int cardBefore() {
        return cardBefore;
    }

    private int bitmapWordForRank(final int rank) {
        seekBitmapWordForRank(rank);
        return index;
    }

    private int runForRank(final int rank) {
        seekRunForRank(rank);
        return index;
    }

    /**
     * Position the cursor at the word holding the value of {@code rank}, or past the last word when the bitmap holds
     * fewer values than that.
     */
    private void seekBitmapWordForRank(final int rank) {
        if (rank < cardBefore) {
            index = 0;
            cardBefore = 0;
        }
        while (index < bitmap.length) {
            final int bits = Long.bitCount(bitmap[index]);
            if (cardBefore + bits > rank) {
                return;
            }
            cardBefore += bits;
            ++index;
        }
    }

    private int bitmapFind(final short x) {
        final int value = toIntUnsigned(x);
        final int target = value >>> 6;
        final int bit = value & 63;
        if (target < index) {
            index = 0;
            cardBefore = 0;
        }
        while (index < target) {
            cardBefore += Long.bitCount(bitmap[index]);
            ++index;
        }
        final long word = bitmap[target];
        final int pos = cardBefore + Long.bitCount(word & ((1L << bit) - 1));
        return ((word >>> bit) & 1L) != 0 ? pos : ~pos;
    }

    /**
     * Position the cursor at the run holding the value of {@code rank}, or past the last run when the container holds
     * fewer values than that.
     */
    private void seekRunForRank(final int rank) {
        if (rank < cardBefore) {
            index = 0;
            cardBefore = 0;
        }
        final int numRuns = runs.numberOfRuns();
        while (index < numRuns) {
            final int size = runs.getLengthAsInt(index) + 1;
            if (cardBefore + size > rank) {
                return;
            }
            cardBefore += size;
            ++index;
        }
    }

    private int runFind(final short x) {
        final int target = toIntUnsigned(x);
        final int numRuns = runs.numberOfRuns();
        // Values before the run at index are all at most the last value of the run before it; a target beyond that
        // value can be resolved from the current position, anything else needs a fresh start.
        if (index > 0 && target <= runs.getValueAsInt(index - 1) + runs.getLengthAsInt(index - 1)) {
            index = 0;
            cardBefore = 0;
        }
        while (index < numRuns) {
            final int start = runs.getValueAsInt(index);
            if (target < start) {
                return ~cardBefore;
            }
            final int length = runs.getLengthAsInt(index);
            if (target <= start + length) {
                return cardBefore + target - start;
            }
            cardBefore += length + 1;
            ++index;
        }
        return ~cardBefore;
    }
}
