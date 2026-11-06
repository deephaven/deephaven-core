//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterAdaptiveSparseArraySource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftCallback;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.sparse.IntBitmapLeaf;
import io.deephaven.engine.table.impl.sources.sparse.IntCompactLeaf;
import io.deephaven.engine.table.impl.sources.sparse.IntSortedLeaf;
import io.deephaven.engine.table.impl.sources.sparse.ChildPositions;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.util.SoftRecycler;
import io.deephaven.util.SoftRecyclerWindow;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
// region boxing imports
import static io.deephaven.util.type.TypeUtils.unbox;
// endregion boxing imports

/**
 * A sparse in-memory column source of ints whose leaves adapt to the number of values they hold.
 * <p>
 * Row keys are addressed through three page table levels above leaves that each cover
 * {@link io.deephaven.engine.table.impl.sources.sparse.SparseConstants#BLOCK_SIZE} consecutive row keys. A leaf is one
 * of:
 * <ul>
 * <li>a {@link IntSortedLeaf}, for at most {@link #DEFAULT_SORTED_THRESHOLD} values</li>
 * <li>a {@link IntBitmapLeaf}, for at most {@link #DEFAULT_COMPACT_THRESHOLD} values</li>
 * <li>a full block of {@code BLOCK_SIZE} values</li>
 * </ul>
 * A leaf takes the next representation when a write would take it past its threshold, and never returns to a smaller
 * one.
 * <p>
 * Each page table level adapts how it holds its children to how many it has and how far apart they are. The top and
 * middle levels hold them in a window, an array indexed by child index less the index of its first entry, while the
 * window has at most eight entries for each child or 64 in all, and rank them otherwise: the top level by a sorted
 * array of their indices, and the middle level by positions ranked among the indices it holds, sorted indices for a few
 * children and then a bitmap with the number of indices before each word, following the leaves' own tiers. The bottom
 * level holds one leaf in its own fields, and more the same way as the middle level, in a window or ranked. A window of
 * one entry and a bottom level of one leaf record the child's index, so that finding the child is one comparison. Each
 * level is held directly by its parent, or by the source for the top level, so that a read follows one reference per
 * level. A level's window or positions and its arrays are fixed but for appending positions and setting children; any
 * other change makes a new level, which replaces the old one in its parent, so that a concurrent reader finds each
 * child where the level it read says.
 * <p>
 * When tracking previous values, the first write to a row in a cycle records the row's value in a second page table of
 * previous values. Each leaf of that table holds the recorded offsets of its leaf as a sorted or bitmap leaf, following
 * the same thresholds, or, once more offsets are recorded than a compact leaf holds, a full block that snapshots the
 * whole leaf. A row without a recorded value has not changed this cycle, so its previous value is its current value.
 * The previous values are discarded when the cycle completes. The snapshot blocks and the arrays of the previous value
 * page table come from recyclers shared by the sources of this type; completing the cycle walks that page table,
 * returning each snapshot block, and each array indexed by child index with every entry null. A ranked level holds only
 * its children and a window has at most 64 entries or eight for each child, so the walk costs at most 64 entries for
 * each level and eight for each child the cycle recorded.
 * <p>
 * For parallel population, {@link #prepareForParallelPopulation(RowSequence, boolean)} allocates the page table above
 * each leaf to be written and reserves the leaf's position, for current and previous values, so that writers of
 * different leaves change no shared structure. When the writers' divisions are aligned to leaves, each writer builds
 * and records its own leaves; a bottom level gets an array of compact leaves only when one of its leaves may be
 * compact, and a writer makes a leaf with no room for a compact leaf a full block from its first chunk. Otherwise it
 * also builds each leaf with every offset to be written, holding null, and records the previous values, so that writers
 * only store values.
 */
public class IntegerAdaptiveSparseArraySource extends SparseArrayColumnSource<Integer>
        implements MutableColumnSourceGetDefaults.ForInt /* MIXIN_IMPLS */ {

    /** The largest number of values a sorted leaf holds. */
    public static final int DEFAULT_SORTED_THRESHOLD = 16;
    /** The largest number of values a sorted or bitmap leaf holds. */
    public static final int DEFAULT_COMPACT_THRESHOLD = 128;

    /**
     * Prepare reads the row keys in pieces of 64 leaves of key space, which bounds the chunk of ranges each piece
     * builds while sharing the cost of building it among many leaves.
     */
    private static final long PREPARE_PIECE_MASK = (64L << LOG_BLOCK_SIZE) - 1;

    // region recycler
    /** Blocks for previous value snapshots, which are not null-filled when borrowed. */
    private static final SoftRecycler<int[]> snapshotRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.INT_RECYCLER_CAPACITY,
                    SparseArrayColumnSourceConfiguration.INT_RECYCLER_MAXIMUM_CAPACITY,
                    () -> new int[BLOCK_SIZE], null));
    // endregion recycler
    // The arrays of the previous value page table, which are returned with every entry null.
    private static final SoftRecycler<int[][]> blocksArrayRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.INT_RECYCLER_CAPACITY2,
                    SparseArrayColumnSourceConfiguration.INT_RECYCLER_MAXIMUM_CAPACITY2,
                    () -> new int[BLOCK2_SIZE][], null));
    private static final SoftRecycler<IntCompactLeaf[]> compactsArrayRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.INT_RECYCLER_CAPACITY2,
                    SparseArrayColumnSourceConfiguration.INT_RECYCLER_MAXIMUM_CAPACITY2,
                    () -> new IntCompactLeaf[BLOCK2_SIZE], null));
    private static final SoftRecycler<Level2[]> level2ArrayRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.INT_RECYCLER_CAPACITY1,
                    SparseArrayColumnSourceConfiguration.INT_RECYCLER_MAXIMUM_CAPACITY1,
                    () -> new Level2[BLOCK1_SIZE], null));

    private final int sortedThreshold;
    private final int compactThreshold;
    private final int recordThreshold;
    /** The current values; a change that a level cannot make in place publishes a new level here. */
    private Level0 root = Level0.empty();

    /**
     * Commits the previous values at the end of each cycle in which a value changed; null when not tracking previous
     * values.
     */
    private transient UpdateCommitter<IntegerAdaptiveSparseArraySource> prevFlusher;
    /**
     * The previous values recorded this cycle, or null if none have been recorded.
     */
    private transient Level0 prevRoot;

    /**
     * The blocks, block2 structures and block1 structures to release at the end of this cycle, as passed to
     * {@link #clearBlocks(RowSet, RowSet, RowSet, boolean)}.
     */
    private transient RowSet blocksToClear;
    private transient RowSet blocks2ToClear;
    private transient RowSet blocks1ToClear;
    private transient boolean emptyResult;

    /**
     * The clock step of the last call to {@link #prepareForParallelPopulation(RowSequence, boolean)}.
     */
    private transient long prepareClockStep = -1;
    /**
     * Whether that call built the leaves to be written and recorded their previous values.
     */
    private transient boolean preparedLeaves;

    /**
     * Whether the values are known never to change, as for the result of a static select.
     */
    private boolean immutable = false;

    // region constructor
    /**
     * Make a source with the default thresholds, whose leaves may be compact.
     */
    public IntegerAdaptiveSparseArraySource() {
        this(DEFAULT_SORTED_THRESHOLD, DEFAULT_COMPACT_THRESHOLD);
    }

    /**
     * @return a source whose leaves are all full blocks
     */
    public static IntegerAdaptiveSparseArraySource fullBlocks() {
        return new IntegerAdaptiveSparseArraySource(DEFAULT_SORTED_THRESHOLD, 0);
    }

    /**
     * @param sortedThreshold the largest number of values a sorted leaf holds
     * @param compactThreshold the largest number of values a sorted or bitmap leaf holds, from 0, when every leaf is a
     *        full block, to the block size
     */
    public IntegerAdaptiveSparseArraySource(final int sortedThreshold, final int compactThreshold) {
        super(int.class);
        this.sortedThreshold = checkSortedThreshold(sortedThreshold, compactThreshold);
        this.compactThreshold = compactThreshold;
        this.recordThreshold = compactThreshold > 0 ? compactThreshold : DEFAULT_COMPACT_THRESHOLD;
    }
    // endregion constructor

    static int checkSortedThreshold(final int sortedThreshold, final int compactThreshold) {
        Require.inRange(compactThreshold, "compactThreshold", BLOCK_SIZE + 1, "BLOCK_SIZE + 1");
        Require.geqZero(compactThreshold, "compactThreshold");
        Require.gtZero(sortedThreshold, "sortedThreshold");
        return sortedThreshold;
    }

    // region page table

    /**
     * @return the length of the arrays for {@code size} children of a ranked level whose arrays have length
     *         {@code capacity}: the same if they fit, else at least double, so that a level that grows a few children
     *         at a time is copied a logarithmic number of times
     */
    private static int grownCapacity(final int capacity, final int size) {
        return size <= capacity ? capacity : Math.max(size, Math.min(MAX_RANKED, capacity * 2));
    }

    /**
     * Copy the entry at each position {@code pp} below {@code count} of {@code from} to position {@code positions[pp]}
     * of {@code to}.
     *
     * @return {@code to}
     */
    private static <T> T[] scatter(final T[] from, final T[] to, final int[] positions, final int count) {
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
    private static <T> T[] gather(final T[] from, final T[] to, final int[] positions, final int count) {
        for (int pp = 0; pp < count; ++pp) {
            to[pp] = from[positions[pp]];
        }
        return to;
    }

    private static final int NO_CHILD = -1;
    /**
     * The fewest entries {@link #maxWindow(int)} allows: a window may always grow to cover this many entries, even when
     * it holds fewer than {@code MIN_WINDOW / WINDOW_ENTRIES_PER_CHILD} children.
     */
    private static final int MIN_WINDOW = 64;
    /** The most entries a window holds for each child it holds, when that is more than {@link #MIN_WINDOW}. */
    private static final int WINDOW_ENTRIES_PER_CHILD = 8;
    /**
     * The most children a ranked middle or bottom page table level holds. More children span at most
     * {@link #WINDOW_ENTRIES_PER_CHILD} entries each, so a window always holds them.
     */
    private static final int MAX_RANKED = BLOCK2_SIZE / WINDOW_ENTRIES_PER_CHILD;

    /**
     * @return the most entries a window holding {@code count} children may have; a level whose children span more ranks
     *         them instead
     */
    private static int maxWindow(final int count) {
        return Math.max(MIN_WINDOW, WINDOW_ENTRIES_PER_CHILD * count);
    }

    /**
     * @return the length of a window of at most {@code limit} entries for {@code count} children spanning
     *         {@code needed} entries, in place of a window of {@code length} entries: at least twice {@code length}
     *         when the children allow it, so that a window that grows a few entries at a time is copied a logarithmic
     *         number of times
     */
    private static int windowLength(final int needed, final int length, final int count, final int limit) {
        return Math.min(limit, Math.min(maxWindow(count), Math.max(needed, 2 * length)));
    }

    /**
     * @return the first index of a window of {@code newLength} entries, of at most {@code limit}, covering the indices
     *         from {@code low} to {@code high}, exclusive, extended below them if {@code down}, else above them
     */
    private static int windowBase(final int low, final int high, final boolean down, final int newLength,
            final int limit) {
        final int newBase = down ? Math.max(0, high - newLength) : low;
        return Math.min(newBase, limit - newLength);
    }

    /**
     * The root page table level, containing {@link Level1} children. A row key's index at this level is its bits from
     * {@code BLOCK0_SHIFT} up. While the children are dense enough, as {@link #maxWindow(int)} allows, the level holds
     * them in an array indexed by child index less the index of its first entry, so that the array covers only the
     * window of indices in use; otherwise it holds them densely, at the positions of their indices in a sorted array.
     * <p>
     * A window of one entry also records its index, so that finding the only child of a level is one comparison. The
     * window of a level is fixed, and a child is set or cleared in place; a ranked level appends an index past its last
     * one in place, once its arrays have room. Anything else makes a new level, which the source publishes as its root,
     * so that a reader that loaded a level finds each child where the level says.
     */
    private static final class Level0 {
        /** For a window of one entry, its index; otherwise {@link #NO_CHILD}. */
        private final int onlyIndex;
        /** The index of the first entry of the window, when the children are indexed by window. */
        private final int base;
        /** The ascending indices of the children, when ranked; null when the children are indexed by window. */
        private final int[] indices;
        private final Level1[] children;
        /** The number of children of a ranked level. */
        private int size;

        /**
         * Make a level that indexes its children by window.
         *
         * @param base the index of the first entry of the window
         * @param children the window: the child for each index from {@code base}, or null where there is none
         */
        private Level0(final int base, final Level1[] children) {
            this.onlyIndex = children.length == 1 ? base : NO_CHILD;
            this.base = base;
            this.indices = null;
            this.children = children;
        }

        /**
         * Make a ranked level, which holds its children densely in the order of their indices.
         *
         * @param indices the ascending indices of the children in its first {@code size} entries; the entries past them
         *        are room to append
         * @param children the children, each at the position of its index in {@code indices}; the entries past the
         *        first {@code size} are room to append
         * @param size the number of children
         */
        private Level0(final int[] indices, final Level1[] children, final int size) {
            this.onlyIndex = NO_CHILD;
            this.base = 0;
            this.indices = indices;
            this.children = children;
            this.size = size;
        }

        private static Level0 empty() {
            return new Level0(0, new Level1[0]);
        }

        /**
         * @return the entry of the children array for {@code index}, or -1 if it has none
         */
        private int slot(final int index) {
            if (indices == null) {
                final int slot = index - base;
                return slot >= 0 && slot < children.length ? slot : -1;
            }
            int low = 0;
            int length = size;
            // narrow to the last position whose index is at most the target; the number of steps depends only on the
            // size, and each step chooses its half with a select of two values, which the JIT can compile to a
            // conditional move rather than a branch on the comparison
            while (length > 1) {
                final int half = length >>> 1;
                low = indices[low + half] <= index ? low + half : low;
                length -= half;
            }
            return length > 0 && indices[low] == index ? low : -1;
        }

        private Level1 get(final int index) {
            if (index == onlyIndex) {
                return children[0];
            }
            final int slot = slot(index);
            return slot < 0 ? null : children[slot];
        }

        /** Make {@code child} the child at {@code index}, which this level covers. */
        private void set(final int index, final Level1 child) {
            children[slot(index)] = child;
        }

        /**
         * @return this level, or a new level holding the same children, that has an entry for {@code index}
         */
        private Level0 cover(final int index) {
            if (slot(index) >= 0) {
                return this;
            }
            return indices == null ? coverOutsideWindow(index) : coverRanked(index);
        }

        private Level0 coverOutsideWindow(final int index) {
            int first = -1;
            int last = -1;
            int count = 1;
            for (int ii = 0; ii < children.length; ++ii) {
                if (children[ii] != null) {
                    if (first < 0) {
                        first = ii;
                    }
                    last = ii;
                    ++count;
                }
            }
            final int low = first < 0 ? index : Math.min(base + first, index);
            final int high = first < 0 ? index + 1 : Math.max(base + last + 1, index + 1);
            if (high - low <= maxWindow(count)) {
                final int newLength = windowLength(high - low, children.length, count, BLOCK0_SIZE);
                final boolean down = first >= 0 && index < base + first;
                final int newBase = windowBase(low, high, down, newLength, BLOCK0_SIZE);
                final Level1[] window = new Level1[newLength];
                if (first >= 0) {
                    System.arraycopy(children, first, window, base + first - newBase, last - first + 1);
                }
                return new Level0(newBase, window);
            }
            // the indices span too wide a range to hold efficiently in a window, so switch to ranked children; the
            // new index is outside the window, so it ranks before or after the children held
            final int[] rankedIndices = new int[2 * count];
            final Level1[] rankedChildren = new Level1[2 * count];
            int ri = 0;
            if (index < base + first) {
                rankedIndices[ri++] = index;
            }
            for (int ii = first; ii <= last; ++ii) {
                if (children[ii] != null) {
                    rankedIndices[ri] = base + ii;
                    rankedChildren[ri++] = children[ii];
                }
            }
            if (index > base + last) {
                rankedIndices[ri++] = index;
            }
            return new Level0(rankedIndices, rankedChildren, ri);
        }

        private Level0 coverRanked(final int index) {
            final int oldSize = size;
            final int low = Math.min(indices[0], index);
            final int high = Math.max(indices[oldSize - 1], index) + 1;
            if (high - low <= maxWindow(oldSize + 1)) {
                final Level1[] window = new Level1[high - low];
                for (int ii = 0; ii < oldSize; ++ii) {
                    window[indices[ii] - low] = children[ii];
                }
                return new Level0(low, window);
            }
            if (index > indices[oldSize - 1] && oldSize < indices.length) {
                indices[oldSize] = index;
                size = oldSize + 1;
                return this;
            }
            final int capacity = oldSize < indices.length ? indices.length : indices.length * 2;
            final Level0 grown = new Level0(new int[capacity], new Level1[capacity], oldSize + 1);
            int gi = 0;
            for (int ii = 0; ii < oldSize; ++ii) {
                if (gi == ii && index < indices[ii]) {
                    grown.indices[gi++] = index;
                }
                grown.indices[gi] = indices[ii];
                grown.children[gi++] = children[ii];
            }
            if (gi == oldSize) {
                grown.indices[gi] = index;
            }
            return grown;
        }

        /**
         * @return this level, or a new level, without the children at {@code removed[0, count)}, ascending
         */
        private Level0 clear(final int[] removed, final int count) {
            if (indices == null) {
                for (int ii = 0; ii < count; ++ii) {
                    final int slot = removed[ii] - base;
                    if (slot >= 0 && slot < children.length) {
                        children[slot] = null;
                    }
                }
                return this;
            }
            final int[] held = Arrays.copyOf(indices, size);
            final int[] kept = new int[held.length];
            final int[] keptFrom = new int[held.length];
            final int keptCount = ChildPositions.keep(held, removed, count, kept, keptFrom);
            if (keptCount == held.length) {
                return this;
            }
            if (keptCount == 0) {
                return empty();
            }
            return new Level0(kept, gather(children, new Level1[keptCount], keptFrom, keptCount), keptCount);
        }

        private long estimateSize() {
            long estimate = (indices == null ? 0 : (long) indices.length * Integer.BYTES)
                    + (long) children.length * Long.BYTES;
            for (final Level1 child : children) {
                if (child != null) {
                    estimate += child.estimateSize();
                }
            }
            return estimate;
        }
    }

    /**
     * The middle page table level, containing {@link Level2} children. While the children are dense enough, as
     * {@link #maxWindow(int)} allows, the level holds them in an array indexed by child index less the index of its
     * first entry, so that the array covers only the window of indices in use; otherwise it holds them densely, in
     * child index order, at the positions its {@link ChildPositions} report.
     * <p>
     * A window of one entry also records its index, so that finding the only child of a level is one comparison. The
     * window of a level is fixed, and a child is set or cleared in place; a ranked level appends positions past its
     * last one in place, once its array has room. Anything else makes a new level, which the source publishes in its
     * parent, so that a reader that loaded a level finds each child where the level says.
     */
    private static final class Level1 {
        /**
         * Whether this level holds previous values, whose array covering every child index is borrowed from a recycler
         * and returned when the cycle completes.
         */
        private final boolean recycled;
        /** For a window of one entry, its index; otherwise {@link #NO_CHILD}. */
        private final int onlyIndex;
        /** The index of the first entry of the window, when the children are indexed by window. */
        private final int base;
        /** The positions of the children, when ranked; null when the children are indexed by window. */
        private final ChildPositions positions;
        private final Level2[] children;

        private Level1(final boolean recycled, final int base, final ChildPositions positions,
                final Level2[] children) {
            this.recycled = recycled;
            this.onlyIndex = positions == null && children.length == 1 ? base : NO_CHILD;
            this.base = base;
            this.positions = positions;
            this.children = children;
        }

        private static Level1 empty(final boolean recycled) {
            return new Level1(recycled, 0, null, new Level2[0]);
        }

        private Level2[] newWindow(final int length) {
            return recycled && length == BLOCK1_SIZE ? level2ArrayRecycler.borrowItem() : new Level2[length];
        }

        private Level2 get(final int index) {
            if (index == onlyIndex) {
                return children[0];
            }
            if (positions == null) {
                final int slot = index - base;
                return slot >= 0 && slot < children.length ? children[slot] : null;
            }
            final int slot = positions.find(index);
            // a position appended after this level was replaced may be past its array
            return slot < 0 || slot >= children.length ? null : children[slot];
        }

        /** Make {@code child} the child at {@code index}, which this level covers. */
        private void set(final int index, final Level2 child) {
            children[positions == null ? index - base : positions.find(index)] = child;
        }

        /**
         * @return this level, or a new level holding the same children, that has an entry for {@code index}
         */
        private Level1 cover(final int index) {
            if (positions == null) {
                final int slot = index - base;
                return slot >= 0 && slot < children.length ? this : coverOutsideWindow(index);
            }
            return positions.find(index) >= 0 ? this : coverRanked(index);
        }

        private Level1 coverOutsideWindow(final int index) {
            int first = -1;
            int last = -1;
            int count = 1;
            for (int ii = 0; ii < children.length; ++ii) {
                if (children[ii] != null) {
                    if (first < 0) {
                        first = ii;
                    }
                    last = ii;
                    ++count;
                }
            }
            final int low = first < 0 ? index : Math.min(base + first, index);
            final int high = first < 0 ? index + 1 : Math.max(base + last + 1, index + 1);
            if (high - low <= maxWindow(count)) {
                final int newLength = windowLength(high - low, children.length, count, BLOCK1_SIZE);
                final boolean down = first >= 0 && index < base + first;
                final int newBase = windowBase(low, high, down, newLength, BLOCK1_SIZE);
                final Level2[] window = newWindow(newLength);
                if (first >= 0) {
                    System.arraycopy(children, first, window, base + first - newBase, last - first + 1);
                }
                return new Level1(recycled, newBase, null, window);
            }
            // the indices span too wide a range to hold efficiently in a window, so switch to ranked children; the
            // new index is outside the window, so it ranks before or after the children held
            final int[] rankedIndices = new int[count];
            final Level2[] rankedChildren = new Level2[grownCapacity(count, count)];
            int ri = 0;
            if (index < base + first) {
                rankedIndices[ri++] = index;
            }
            for (int ii = first; ii <= last; ++ii) {
                if (children[ii] != null) {
                    rankedIndices[ri] = base + ii;
                    rankedChildren[ri++] = children[ii];
                }
            }
            if (index > base + last) {
                rankedIndices[ri++] = index;
            }
            return new Level1(recycled, 0, ChildPositions.of(rankedIndices, ri), rankedChildren);
        }

        private Level1 coverRanked(final int index) {
            final int[] held = positions.indices();
            final int newSize = held.length + 1;
            final int low = Math.min(held[0], index);
            final int high = Math.max(held[held.length - 1], index) + 1;
            if (high - low <= maxWindow(newSize)) {
                final Level2[] window = newWindow(high - low);
                for (int ii = 0; ii < held.length; ++ii) {
                    window[held[ii] - low] = children[ii];
                }
                return new Level1(recycled, low, null, window);
            }
            final int[] added = {index};
            if (index > held[held.length - 1]) {
                Level1 level = this;
                if (newSize > children.length) {
                    // grow the array first, so that a position appended in place is within the array readers find
                    level = new Level1(recycled, 0, positions,
                            Arrays.copyOf(children, grownCapacity(children.length, newSize)));
                }
                final ChildPositions appended = level.positions.append(added, 1);
                return appended == level.positions ? level : new Level1(recycled, 0, appended, level.children);
            }
            final int[] movedTo = new int[held.length];
            final int[] merged = ChildPositions.merge(held, added, 1, movedTo);
            return new Level1(recycled, 0, ChildPositions.of(merged, newSize),
                    scatter(children, new Level2[grownCapacity(children.length, newSize)], movedTo, held.length));
        }

        /**
         * @return this level, or a new level, without the children at {@code removed[0, count)}, ascending
         */
        private Level1 clear(final int[] removed, final int count) {
            if (positions == null) {
                for (int ii = 0; ii < count; ++ii) {
                    final int slot = removed[ii] - base;
                    if (slot >= 0 && slot < children.length) {
                        children[slot] = null;
                    }
                }
                return this;
            }
            final int[] held = positions.indices();
            final int[] kept = new int[held.length];
            final int[] keptFrom = new int[held.length];
            final int keptCount = ChildPositions.keep(held, removed, count, kept, keptFrom);
            if (keptCount == held.length) {
                return this;
            }
            if (keptCount == 0) {
                return empty(recycled);
            }
            return new Level1(recycled, 0, ChildPositions.of(kept, keptCount),
                    gather(children, new Level2[keptCount], keptFrom, keptCount));
        }

        /**
         * Release this previous value level and its children when the cycle completes: return their snapshot blocks,
         * and their arrays covering every child index, with every entry null, to the recyclers. The level is discarded
         * with any other array.
         */
        private void recyclePrevious() {
            for (int ii = 0; ii < children.length; ++ii) {
                if (children[ii] != null) {
                    children[ii].recyclePrevious();
                }
            }
            if (positions == null && children.length == BLOCK1_SIZE) {
                Arrays.fill(children, null);
                level2ArrayRecycler.returnItem(children);
            }
        }

        private long estimateSize() {
            long estimate = (positions == null ? 0 : positions.estimateSize()) + (long) children.length * Long.BYTES;
            for (final Level2 child : children) {
                if (child != null) {
                    estimate += child.estimateSize();
                }
            }
            return estimate;
        }
    }

    /**
     * The bottom page table level: its leaves. A leaf is either a full block or a compact leaf, kept in separate arrays
     * so that reading a full block needs no test of the leaf's type. A level adapts how it holds its leaves to how many
     * it has and how far apart they are: one leaf in its own fields, with no arrays, so that reading it follows no
     * reference beyond the level; leaves dense enough for a window, as {@link #maxWindow(int)} allows, in arrays
     * indexed by leaf index less the index of their first entry, up to a window of every leaf index of the level; and
     * sparser leaves densely, in leaf index order, at the positions its {@link ChildPositions} report.
     * <p>
     * The window or positions and the arrays of a level are fixed, except that appending positions past the last one
     * changes the positions in place, once the arrays have room for them, and a leaf is set at its entry. Anything
     * else, such as widening a window, moving a leaf or growing an array, makes a new level, which the source publishes
     * in its parent, so that a reader that loaded a level finds each leaf where the level says. A writer therefore uses
     * the level that its last change returned.
     * <p>
     * Only one thread changes which leaves a level holds at a time; a parallel population sets only the leaves at
     * entries its preparation reserved, in arrays it allocated.
     */
    private static final class Level2 {
        /**
         * Whether this level holds previous values, whose arrays covering every leaf index are borrowed from recyclers
         * and returned when the cycle completes.
         */
        private final boolean recycled;
        /** The positions of the leaves, when ranked; null when the arrays are a window. */
        private final ChildPositions positions;
        /** The leaf index of the first entry of a window; 0 when ranked. */
        private final int base;
        /** The length of the arrays of leaves, or of the window of a level of one leaf. */
        private final int capacity;
        /** The full blocks, or null if no leaf may be one. */
        private final int[][] blocks;
        /** The compact leaves, or null if no leaf may be one. */
        private final IntCompactLeaf[] compacts;
        /** For a level holding one leaf in its own fields, the index of the leaf; otherwise {@link #NO_CHILD}. */
        private final int onlyIndex;
        /** The leaf of a level holding one leaf in its own fields, if it is a full block. */
        private int[] onlyBlock;
        /** The leaf of a level holding one leaf in its own fields, if it is a compact leaf. */
        private IntCompactLeaf onlyCompact;

        private Level2(
                final boolean recycled,
                final ChildPositions positions,
                final int base,
                final int capacity,
                final int[][] blocks,
                final IntCompactLeaf[] compacts) {
            this.recycled = recycled;
            this.positions = positions;
            this.base = base;
            this.capacity = capacity;
            this.blocks = blocks;
            this.compacts = compacts;
            this.onlyIndex = NO_CHILD;
        }

        /** Make a level holding only the leaf at {@code index}, which is not yet set, in its own fields. */
        private Level2(final boolean recycled, final int index) {
            this.recycled = recycled;
            this.positions = null;
            this.base = index;
            this.capacity = 1;
            this.blocks = null;
            this.compacts = null;
            this.onlyIndex = index;
        }

        private static Level2 empty(final boolean recycled) {
            return new Level2(recycled, null, 0, 0, null, null);
        }

        /**
         * @return the position of the leaf at {@code index}, or -1 if it has none
         */
        private int slot(final int index) {
            if (index == onlyIndex) {
                return 0;
            }
            if (positions == null) {
                final int slot = index - base;
                return slot >= 0 && slot < capacity ? slot : -1;
            }
            return positions.find(index);
        }

        /**
         * @return the full block at position {@code slot}, or null if there is none
         */
        private int[] blockAt(final int slot) {
            final int[][] localBlocks = blocks;
            if (localBlocks == null) {
                // a level of one leaf holds it at position 0; any other level with no array holds no full block
                return slot == 0 ? onlyBlock : null;
            }
            // a position appended after this level was replaced may be past its arrays
            return slot < 0 || slot >= localBlocks.length ? null : localBlocks[slot];
        }

        /**
         * @return the compact leaf at position {@code slot}, or null if there is none
         */
        private IntCompactLeaf compactAt(final int slot) {
            final IntCompactLeaf[] localCompacts = compacts;
            if (localCompacts == null) {
                // a level of one leaf holds it at position 0; any other level with no array holds no compact leaf
                return slot == 0 ? onlyCompact : null;
            }
            return slot < 0 || slot >= localCompacts.length ? null : localCompacts[slot];
        }

        private int[] getBlock(final int index) {
            return blockAt(slot(index));
        }

        private IntCompactLeaf getCompact(final int index) {
            return compactAt(slot(index));
        }

        /**
         * Make {@code block} the leaf at {@code index}, which has a position and room for a full block, replacing any
         * compact leaf there.
         */
        private void putBlock(final int index, final int[] block) {
            if (index == onlyIndex) {
                onlyBlock = block;
                onlyCompact = null;
                return;
            }
            final int slot = slot(index);
            blocks[slot] = block;
            if (compacts != null) {
                compacts[slot] = null;
            }
        }

        /**
         * Make {@code compact} the leaf at {@code index}, which has a position and room for a compact leaf, and is not
         * a full block.
         */
        private void putCompact(final int index, final IntCompactLeaf compact) {
            if (index == onlyIndex) {
                onlyCompact = compact;
                return;
            }
            compacts[slot(index)] = compact;
        }

        private boolean isDirect() {
            return positions == null && capacity == BLOCK2_SIZE;
        }

        private int[][] newBlocks(final boolean window, final int length) {
            return recycled && window && length == BLOCK2_SIZE ? blocksArrayRecycler.borrowItem() : new int[length][];
        }

        private IntCompactLeaf[] newCompacts(final boolean window, final int length) {
            return recycled && window && length == BLOCK2_SIZE
                    ? compactsArrayRecycler.borrowItem()
                    : new IntCompactLeaf[length];
        }

        /**
         * @return a new level holding the leaf of this level of one leaf in a window of one entry with arrays
         */
        private Level2 toArrays() {
            return new Level2(recycled, null, onlyIndex, 1,
                    onlyBlock == null ? null : new int[][] {onlyBlock},
                    onlyCompact == null ? null : new IntCompactLeaf[] {onlyCompact});
        }

        /**
         * Give each of {@code indices[0, count)}, ascending and distinct, a position, so that setting its leaf writes
         * only its own entry. Positions after the existing ones of a ranked level are appended; otherwise the leaves
         * are moved once, into new arrays.
         *
         * @return this level, or a new level holding the same leaves, with the positions
         */
        private Level2 reserve(final int[] indices, final int count) {
            if (isDirect()) {
                return this;
            }
            int absent = 0;
            for (int ii = 0; ii < count; ++ii) {
                if (slot(indices[ii]) < 0) {
                    ++absent;
                }
            }
            if (absent == 0) {
                return this;
            }
            if (onlyIndex != NO_CHILD) {
                return toArrays().reserve(indices, count);
            }
            final int[] absentIndices;
            if (absent == count) {
                absentIndices = indices;
            } else {
                absentIndices = new int[absent];
                int next = 0;
                for (int ii = 0; ii < count; ++ii) {
                    if (slot(indices[ii]) < 0) {
                        absentIndices[next++] = indices[ii];
                    }
                }
            }
            return positions == null ? reserveInWindow(absentIndices, absent) : reserveRanked(absentIndices, absent);
        }

        /**
         * @return whether the window entry at {@code slot} holds a leaf
         */
        private boolean holds(final int slot) {
            return (blocks != null && blocks[slot] != null) || (compacts != null && compacts[slot] != null);
        }

        /**
         * @return a new level holding the leaves of this window and positions for {@code indices[0, count)}, ascending
         *         and outside the window
         */
        private Level2 reserveInWindow(final int[] indices, final int count) {
            int first = -1;
            int last = -1;
            int held = 0;
            for (int ii = 0; ii < capacity; ++ii) {
                if (holds(ii)) {
                    if (first < 0) {
                        first = ii;
                    }
                    last = ii;
                    ++held;
                }
            }
            if (held == 0 && count == 1) {
                return new Level2(recycled, indices[0]);
            }
            final int newCount = held + count;
            final int low = first < 0 ? indices[0] : Math.min(base + first, indices[0]);
            final int high = first < 0 ? indices[count - 1] + 1 : Math.max(base + last + 1, indices[count - 1] + 1);
            if (high - low <= maxWindow(newCount)) {
                final int newLength = windowLength(high - low, capacity, newCount, BLOCK2_SIZE);
                final boolean down = first >= 0 && indices[0] < base + first;
                final int newBase = windowBase(low, high, down, newLength, BLOCK2_SIZE);
                final int[][] newBlocks = blocks == null ? null : newBlocks(true, newLength);
                final IntCompactLeaf[] newCompacts = compacts == null ? null : newCompacts(true, newLength);
                if (first >= 0) {
                    if (newBlocks != null) {
                        System.arraycopy(blocks, first, newBlocks, base + first - newBase, last - first + 1);
                    }
                    if (newCompacts != null) {
                        System.arraycopy(compacts, first, newCompacts, base + first - newBase, last - first + 1);
                    }
                }
                return new Level2(recycled, null, newBase, newLength, newBlocks, newCompacts);
            }
            // rank the leaves held and the new ones
            final int[] heldIndices = new int[held];
            final int[] heldSlots = new int[held];
            int hi = 0;
            for (int ii = first; hi < held; ++ii) {
                if (holds(ii)) {
                    heldIndices[hi] = base + ii;
                    heldSlots[hi++] = ii;
                }
            }
            final int[] movedTo = new int[held];
            final int[] merged = ChildPositions.merge(heldIndices, indices, count, movedTo);
            final int newCapacity = grownCapacity(newCount, newCount);
            final int[][] newBlocks = blocks == null ? null : new int[newCapacity][];
            final IntCompactLeaf[] newCompacts = compacts == null ? null : new IntCompactLeaf[newCapacity];
            for (int ii = 0; ii < held; ++ii) {
                if (newBlocks != null) {
                    newBlocks[movedTo[ii]] = blocks[heldSlots[ii]];
                }
                if (newCompacts != null) {
                    newCompacts[movedTo[ii]] = compacts[heldSlots[ii]];
                }
            }
            return new Level2(recycled, ChildPositions.of(merged, newCount), 0, newCapacity, newBlocks,
                    newCompacts);
        }

        /**
         * @return this level, or a new level holding the same leaves, with positions for {@code indices[0, count)},
         *         ascending and none held
         */
        private Level2 reserveRanked(final int[] indices, final int count) {
            final int[] held = positions.indices();
            final int newSize = held.length + count;
            final int low = held.length == 0 ? indices[0] : Math.min(held[0], indices[0]);
            final int high = held.length == 0
                    ? indices[count - 1] + 1
                    : Math.max(held[held.length - 1], indices[count - 1]) + 1;
            if (held.length == 0 && count == 1) {
                return new Level2(recycled, indices[0]);
            }
            if (high - low <= maxWindow(newSize)) {
                final int newLength = high - low;
                final int[][] newBlocks = blocks == null ? null : newBlocks(true, newLength);
                final IntCompactLeaf[] newCompacts = compacts == null ? null : newCompacts(true, newLength);
                for (int ii = 0; ii < held.length; ++ii) {
                    if (newBlocks != null && ii < blocks.length) {
                        newBlocks[held[ii] - low] = blocks[ii];
                    }
                    if (newCompacts != null && ii < compacts.length) {
                        newCompacts[held[ii] - low] = compacts[ii];
                    }
                }
                return new Level2(recycled, null, low, newLength, newBlocks, newCompacts);
            }
            if (held.length > 0 && indices[0] > held[held.length - 1]) {
                return append(indices, count, newSize);
            }
            return merge(held, indices, count, newSize);
        }

        /**
         * @return this level, or a new level, holding {@code indices[0, count)}, each past the last index held, for
         *         {@code newSize} leaves in all
         */
        private Level2 append(final int[] indices, final int count, final int newSize) {
            Level2 level = this;
            if (newSize > capacity) {
                // grow the arrays first, so that a position appended in place is within the arrays readers find
                final int newCapacity = grownCapacity(capacity, newSize);
                level = new Level2(recycled, positions, 0, newCapacity,
                        blocks == null ? null : Arrays.copyOf(blocks, newCapacity),
                        compacts == null ? null : Arrays.copyOf(compacts, newCapacity));
            }
            final ChildPositions appended = level.positions.append(indices, count);
            return appended == level.positions
                    ? level
                    : new Level2(recycled, appended, 0, level.capacity, level.blocks, level.compacts);
        }

        /**
         * @return a new level holding {@code indices[0, count)}, none held, among {@code held}, the indices held, for
         *         {@code newSize} leaves in all, with each leaf held moved once into new arrays
         */
        private Level2 merge(final int[] held, final int[] indices, final int count, final int newSize) {
            final int[] movedTo = new int[held.length];
            final int[] merged = ChildPositions.merge(held, indices, count, movedTo);
            final int newCapacity = grownCapacity(capacity, newSize);
            return new Level2(recycled, ChildPositions.of(merged, newSize), 0, newCapacity,
                    blocks == null ? null : scatter(blocks, new int[newCapacity][], movedTo, held.length),
                    compacts == null
                            ? null
                            : scatter(compacts, new IntCompactLeaf[newCapacity], movedTo, held.length));
        }

        /**
         * @return this level, or a new level holding the same leaves, with a position for {@code index}
         */
        private Level2 reserveOne(final int index) {
            return slot(index) >= 0 ? this : reserve(new int[] {index}, 1);
        }

        /**
         * @return this level, or a new level holding the same leaves, with an array of full blocks
         */
        private Level2 withBlocks() {
            if (blocks != null || onlyIndex != NO_CHILD) {
                return this;
            }
            return new Level2(recycled, positions, base, capacity, newBlocks(positions == null, capacity), compacts);
        }

        /**
         * @return this level, or a new level holding the same leaves, with an array of compact leaves
         */
        private Level2 withCompacts() {
            if (compacts != null || onlyIndex != NO_CHILD) {
                return this;
            }
            return new Level2(recycled, positions, base, capacity, blocks, newCompacts(positions == null, capacity));
        }

        /**
         * @param mayHoldBlocks whether a leaf may become a full block
         * @param mayHoldCompacts whether a leaf may become a compact leaf
         * @return this level, or a new level holding the same leaves, with the arrays that setting the leaves at the
         *         reserved positions may need, so that doing so writes only their own entries
         */
        private Level2 withArrays(final boolean mayHoldBlocks, final boolean mayHoldCompacts) {
            final Level2 level = mayHoldBlocks ? withBlocks() : this;
            return mayHoldCompacts ? level.withCompacts() : level;
        }

        /**
         * @return whether making a leaf at a reserved position a compact leaf writes only its own entry
         */
        private boolean hasRoomForCompact() {
            return compacts != null || onlyIndex != NO_CHILD;
        }

        /**
         * @return this level, or a new level, without the leaves at {@code indices[0, count)}, ascending
         */
        private Level2 clear(final int[] indices, final int count) {
            if (onlyIndex != NO_CHILD) {
                for (int ii = 0; ii < count; ++ii) {
                    if (indices[ii] == onlyIndex) {
                        return empty(recycled);
                    }
                }
                return this;
            }
            if (positions == null) {
                for (int ii = 0; ii < count; ++ii) {
                    final int slot = indices[ii] - base;
                    if (slot < 0 || slot >= capacity) {
                        continue;
                    }
                    if (blocks != null) {
                        blocks[slot] = null;
                    }
                    if (compacts != null) {
                        compacts[slot] = null;
                    }
                }
                return this;
            }
            final int[] held = positions.indices();
            final int[] kept = new int[held.length];
            final int[] keptFrom = new int[held.length];
            final int keptCount = ChildPositions.keep(held, indices, count, kept, keptFrom);
            if (keptCount == held.length) {
                return this;
            }
            if (keptCount == 0) {
                return empty(recycled);
            }
            // the remaining leaves move into new arrays, so that a reader of this level finds each where it was
            return new Level2(recycled, ChildPositions.of(kept, keptCount), 0, keptCount,
                    blocks == null ? null : gather(blocks, new int[keptCount][], keptFrom, keptCount),
                    compacts == null ? null : gather(compacts, new IntCompactLeaf[keptCount], keptFrom, keptCount));
        }

        /**
         * Release this previous value level when the cycle completes: return its snapshot blocks, and its arrays
         * covering every leaf index, with every entry null, to their recyclers. The level is discarded with any other
         * arrays.
         */
        private void recyclePrevious() {
            if (onlyBlock != null) {
                snapshotRecycler.returnItem(onlyBlock);
            }
            if (blocks != null) {
                for (final int[] snapshot : blocks) {
                    if (snapshot != null) {
                        snapshotRecycler.returnItem(snapshot);
                    }
                }
            }
            if (!isDirect()) {
                return;
            }
            if (blocks != null) {
                Arrays.fill(blocks, null);
                blocksArrayRecycler.returnItem(blocks);
            }
            if (compacts != null) {
                Arrays.fill(compacts, null);
                compactsArrayRecycler.returnItem(compacts);
            }
        }

        private long estimateSize() {
            long estimate = positions == null ? 0 : positions.estimateSize();
            if (onlyBlock != null) {
                estimate += (long) BLOCK_SIZE * Integer.BYTES;
            }
            if (onlyCompact != null) {
                estimate += onlyCompact.estimateSize();
            }
            if (blocks != null) {
                estimate += (long) blocks.length * Long.BYTES;
                for (final int[] block : blocks) {
                    if (block != null) {
                        estimate += (long) BLOCK_SIZE * Integer.BYTES;
                    }
                }
            }
            if (compacts != null) {
                estimate += (long) compacts.length * Long.BYTES;
                for (final IntCompactLeaf compact : compacts) {
                    if (compact != null) {
                        estimate += compact.estimateSize();
                    }
                }
            }
            return estimate;
        }
    }

    private static int block0(final long key) {
        return (int) (key >> BLOCK0_SHIFT) & BLOCK0_MASK;
    }

    private static int block1(final long key) {
        return (int) (key >> BLOCK1_SHIFT) & BLOCK1_MASK;
    }

    private static int block2(final long key) {
        return (int) (key >> BLOCK2_SHIFT) & BLOCK2_MASK;
    }

    /**
     * @return the bottom page table level covering {@code key}, or null if none has been allocated
     */
    private Level2 getLevel2(final long key) {
        return getLevel2(root, key);
    }

    /**
     * @return the bottom page table level of {@code root} covering {@code key}, or null if none has been allocated
     */
    private static Level2 getLevel2(final Level0 root, final long key) {
        final Level1 level1 = root.get(block0(key));
        return level1 == null ? null : level1.get(block1(key));
    }

    /**
     * @return the bottom page table level covering {@code key}, allocating the levels as needed
     */
    private Level2 ensureLevel2(final long key) {
        return ensureLevel2(false, key);
    }

    /**
     * @return the bottom level of the previous value page table covering {@code key}, allocating as needed
     */
    private Level2 ensurePrevLevel2(final long key) {
        if (prevRoot == null) {
            prevRoot = Level0.empty();
            prevFlusher.maybeActivate();
        }
        return ensureLevel2(true, key);
    }

    /**
     * @return the bottom level of the previous value page table if {@code prev}, else the current one, covering
     *         {@code key}, allocating the levels as needed
     */
    private Level2 ensureLevel2(final boolean prev, final long key) {
        Level0 level0 = prev ? prevRoot : root;
        final int block0 = block0(key);
        Level1 level1 = level0.get(block0);
        if (level1 == null) {
            final Level0 covering = level0.cover(block0);
            if (covering != level0) {
                level0 = covering;
                if (prev) {
                    prevRoot = covering;
                } else {
                    root = covering;
                }
            }
            level1 = Level1.empty(prev);
            level0.set(block0, level1);
        }
        final int block1 = block1(key);
        Level2 level2 = level1.get(block1);
        if (level2 != null) {
            return level2;
        }
        final Level1 covering = level1.cover(block1);
        if (covering != level1) {
            level1 = covering;
            level0.set(block0, covering);
        }
        level2 = Level2.empty(prev);
        level1.set(block1, level2);
        return level2;
    }

    /**
     * Make {@code level2} the bottom level covering {@code key} of the previous value page table if {@code prev}, else
     * of the current one, in place of the level that it changes.
     */
    private void publishLevel2(final boolean prev, final long key, final Level2 level2) {
        (prev ? prevRoot : root).get(block0(key)).set(block1(key), level2);
    }

    /**
     * Make {@code block} the leaf covering {@code key}, replacing any compact leaf there, in the previous value page
     * table if {@code prev}, else in the current one, whose bottom level covering {@code key} is {@code level2}.
     *
     * @return the bottom level covering {@code key}, which replaces {@code level2} if that had no room for the block
     */
    private Level2 setBlock(final boolean prev, final long key, final Level2 level2, final int[] block) {
        final int index = block2(key);
        final Level2 updated = level2.reserveOne(index).withBlocks();
        if (updated != level2) {
            publishLevel2(prev, key, updated);
        }
        updated.putBlock(index, block);
        return updated;
    }

    /**
     * Make {@code compact} the leaf covering {@code key}, which must not be a full block, in the previous value page
     * table if {@code prev}, else in the current one, whose bottom level covering {@code key} is {@code level2}.
     *
     * @return the bottom level covering {@code key}, which replaces {@code level2} if that had no room for the leaf
     */
    private Level2 setCompact(
            final boolean prev,
            final long key,
            final Level2 level2,
            final IntCompactLeaf compact) {
        final int index = block2(key);
        final Level2 updated = level2.reserveOne(index).withCompacts();
        if (updated != level2) {
            publishLevel2(prev, key, updated);
        }
        updated.putCompact(index, compact);
        return updated;
    }
    // endregion page table

    /**
     * @return an empty compact leaf with room for {@code capacity} values
     */
    private IntCompactLeaf newCompactLeaf(final int capacity) {
        return capacity <= sortedThreshold ? new IntSortedLeaf(capacity) : new IntBitmapLeaf(capacity);
    }

    /**
     * Make the leaf covering {@code key} in {@code level2}, the current bottom level covering it, whose compact leaf is
     * {@code compact} (or null if the leaf is absent), ready for {@code count} more offsets. The leaf may move to a new
     * bottom level, so a caller that writes the leaf again reads the bottom level again.
     *
     * @return the compact leaf to write, or null if the leaf is now a full block
     */
    private IntCompactLeaf prepareCompactLeaf(
            final long key,
            final Level2 level2,
            final IntCompactLeaf compact,
            final int count) {
        if (compact == null) {
            if (count > compactThreshold) {
                setBlock(false, key, level2, newBlock());
                return null;
            }
            final IntCompactLeaf newCompact = newCompactLeaf(count);
            setCompact(false, key, level2, newCompact);
            return newCompact;
        }
        final int newSize = compact.size() + count;
        if (newSize > compactThreshold) {
            setBlock(false, key, level2, toBlock(compact));
            return null;
        }
        return maybeLeaveSortedTier(false, key, level2, compact, newSize);
    }

    /**
     * Walks the row keys of a write one leaf at a time, so that it visits each leaf's row keys together without a row
     * key consumer or a row sequence per leaf.
     */
    private abstract static class LeafCursor {
        /** The first row key of the current leaf. */
        long firstKey;
        /** The number of row keys in the current leaf. */
        int count;

        abstract boolean hasMore();

        /** Advance to the next leaf that holds a row key. */
        abstract void nextLeaf();

        /** Store the offsets of the current leaf's row keys, ascending, in {@code offsets}. */
        abstract void offsets(int[] offsets);

        /**
         * Copy the values of the current leaf's row keys, which start at {@code srcOffset} in {@code values}, into
         * {@code block} at their offsets.
         */
        abstract void copyTo(IntChunk<? extends Values> values, int srcOffset, int[] block);

        /** Return to the first leaf. */
        abstract void rewind();
    }

    /**
     * @return {@code ranges} or {@code keys}, whichever walks {@code rowSequence} more cheaply, reset to its row keys
     */
    private static LeafCursor leafCursor(final RowSequence rowSequence, final LeafRanges ranges, final LeafKeys keys) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            keys.reset(rowSequence.asRowKeyChunk());
            return keys;
        }
        ranges.reset(rowSequence.asRowKeyRangesChunk());
        return ranges;
    }

    /**
     * A {@link LeafCursor} over a chunk of row keys.
     */
    private static final class LeafKeys extends LeafCursor {
        private LongChunk<OrderedRowKeys> keys;
        /** The position in {@link #keys} of the current leaf's first row key. */
        private int firstIndex;
        /** The position in {@link #keys} of the next leaf's first row key. */
        private int nextIndex;

        private void reset(final LongChunk<OrderedRowKeys> keys) {
            this.keys = keys;
            rewind();
        }

        @Override
        void rewind() {
            nextIndex = 0;
        }

        @Override
        boolean hasMore() {
            return nextIndex < keys.size();
        }

        @Override
        void nextLeaf() {
            firstIndex = nextIndex;
            firstKey = keys.get(firstIndex);
            final long lastKeyInLeaf = firstKey | INDEX_MASK;
            int index = firstIndex + 1;
            while (index < keys.size() && keys.get(index) <= lastKeyInLeaf) {
                ++index;
            }
            nextIndex = index;
            count = index - firstIndex;
        }

        @Override
        void offsets(final int[] offsets) {
            for (int ii = 0; ii < count; ++ii) {
                offsets[ii] = (int) (keys.get(firstIndex + ii) & INDEX_MASK);
            }
        }

        @Override
        void copyTo(final IntChunk<? extends Values> values, final int srcOffset, final int[] block) {
            for (int ii = 0; ii < count; ++ii) {
                block[(int) (keys.get(firstIndex + ii) & INDEX_MASK)] = values.get(srcOffset + ii);
            }
        }
    }

    /**
     * A {@link LeafCursor} over a chunk of row key ranges. A range that crosses a leaf boundary is visited in parts,
     * one in each leaf.
     */
    private static final class LeafRanges extends LeafCursor {
        private LongChunk<OrderedRowKeyRanges> ranges;
        /** The position in {@link #ranges} of the first range not yet fully visited. */
        private int nextRange;
        /** The first row key not yet visited, within the range at {@link #nextRange}. */
        private long nextKey;
        /** The positions in {@link #ranges} of the first range in the current leaf and past its last range. */
        private int firstRange;
        private int endRange;

        private void reset(final LongChunk<OrderedRowKeyRanges> ranges) {
            this.ranges = ranges;
            rewind();
        }

        @Override
        void rewind() {
            nextRange = 0;
            nextKey = ranges.size() == 0 ? 0 : ranges.get(0);
        }

        @Override
        boolean hasMore() {
            return nextRange < ranges.size();
        }

        @Override
        void nextLeaf() {
            firstKey = nextKey;
            firstRange = nextRange;
            final long lastKeyInLeaf = firstKey | INDEX_MASK;
            long rangeFirst = firstKey;
            int leafCount = 0;
            int ri = nextRange;
            while (true) {
                final long rangeLast = ranges.get(ri + 1);
                if (rangeLast > lastKeyInLeaf) {
                    // the range continues into the next leaf
                    leafCount += (int) (lastKeyInLeaf - rangeFirst + 1);
                    endRange = ri + 2;
                    nextRange = ri;
                    nextKey = lastKeyInLeaf + 1;
                    break;
                }
                leafCount += (int) (rangeLast - rangeFirst + 1);
                ri += 2;
                if (ri == ranges.size() || ranges.get(ri) > lastKeyInLeaf) {
                    endRange = ri;
                    nextRange = ri;
                    nextKey = ri == ranges.size() ? 0 : ranges.get(ri);
                    break;
                }
                rangeFirst = ranges.get(ri);
            }
            count = leafCount;
        }

        /** @return the first offset within the current leaf of the range at {@code ri} */
        private int firstOffset(final int ri) {
            return (int) (Math.max(ranges.get(ri), firstKey) & INDEX_MASK);
        }

        /** @return the last offset within the current leaf of the range at {@code ri} */
        private int lastOffset(final int ri) {
            return (int) (Math.min(ranges.get(ri + 1), firstKey | INDEX_MASK) & INDEX_MASK);
        }

        @Override
        void offsets(final int[] offsets) {
            int next = 0;
            for (int ri = firstRange; ri < endRange; ri += 2) {
                final int lastOffset = lastOffset(ri);
                for (int offset = firstOffset(ri); offset <= lastOffset; ++offset) {
                    offsets[next++] = offset;
                }
            }
        }

        @Override
        void copyTo(final IntChunk<? extends Values> values, final int srcOffset, final int[] block) {
            int position = srcOffset;
            for (int ri = firstRange; ri < endRange; ri += 2) {
                final int firstOffset = firstOffset(ri);
                final int length = lastOffset(ri) - firstOffset + 1;
                values.copyToTypedArray(position, block, firstOffset, length);
                position += length;
            }
        }
    }

    /**
     * Holds the offsets that a write records as previous values in a compact leaf, and their values, for one leaf at a
     * time.
     */
    private static final class PrevScratch {
        private final int[] offsets;
        private final int[] values;
        private final IntChunk<? extends Values> valuesChunk;

        /**
         * @param capacity the most offsets a compact leaf holds
         */
        private PrevScratch(final int capacity) {
            offsets = new int[capacity];
            values = new int[capacity];
            valuesChunk = IntChunk.chunkWrap(values);
        }
    }

    /**
     * @return {@code compact}, the leaf covering {@code key} in {@code level2}, the bottom level covering it in the
     *         previous value page table if {@code prev}, else in the current one; or a bitmap leaf with its values that
     *         replaces it if it is a sorted leaf and {@code newSize} is more than a sorted leaf holds
     */
    private IntCompactLeaf maybeLeaveSortedTier(
            final boolean prev,
            final long key,
            final Level2 level2,
            final IntCompactLeaf compact,
            final int newSize) {
        if (newSize <= sortedThreshold || !(compact instanceof IntSortedLeaf)) {
            return compact;
        }
        final IntBitmapLeaf bitmap = new IntBitmapLeaf(newSize);
        ((IntSortedLeaf) compact).copyTo(bitmap);
        setCompact(prev, key, level2, bitmap);
        return bitmap;
    }

    /**
     * @return whether writes must record previous values
     */
    private boolean shouldTrackPrevious() {
        return prevFlusher != null && !(preparedLeaves && prepareClockStep == updateGraph.clock().currentStep());
    }

    @Override
    public int parallelPopulationAlignment() {
        return BLOCK_SIZE;
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence changedRows) {
        prepareForParallelPopulation(changedRows, false);
    }

    @Override
    public void prepareForParallelPopulation(final RowSequence changedRows, final boolean alignedDivisions) {
        startPreparation(!alignedDivisions);
        prepareLeafIndices(changedRows);
        if (!alignedDivisions) {
            prepareLeaves(changedRows);
        }
    }

    @Override
    public void prepareForParallelPopulationInDivisions(final RowSequence changedRows) {
        startPreparation(true);
        // prepareDivisionForParallelPopulation builds the leaves, a division at a time, in the room this allocates
        prepareLeafIndices(changedRows);
    }

    @Override
    public void prepareDivisionForParallelPopulation(final RowSequence division) {
        prepareLeaves(division);
    }

    /**
     * Record that this cycle's writers populate prepared rows, so that preparing again in this cycle fails.
     *
     * @param leavesBuilt whether the writers will find every leaf built and its previous values recorded
     */
    private void startPreparation(final boolean leavesBuilt) {
        final long currentStep = updateGraph.clock().currentStep();
        if (prepareClockStep == currentStep) {
            throw new IllegalStateException("May not call prepareForParallelPopulation twice on one clock cycle!");
        }
        prepareClockStep = currentStep;
        preparedLeaves = leavesBuilt;
    }

    /**
     * Allocate the page table entries that writing the row keys of {@code rows} may need, for current and, when
     * tracking them, previous values, so that writers of different leaves change no shared structure. Each leaf gets a
     * position in its bottom level, the leaves of a bottom level reserved together. A leaf that may hold more values
     * than a compact leaf holds needs room only for a full block; its writer then makes it a full block from its first
     * chunk, unless its bottom level has room for compact leaves anyway (see {@link #writesAlignedDivisions()}). Any
     * other leaf needs room only for a compact leaf. The previous values get room for both.
     */
    private void prepareLeafIndices(final RowSequence rows) {
        final boolean trackPrevious = prevFlusher != null;
        int[] indices = new int[64];
        int count = 0;
        long level2Key = -1;
        Level2 level2 = null;
        Level2 prevLevel2 = null;
        boolean mayHoldBlocks = false;
        boolean mayHoldCompacts = false;
        boolean mayHoldSnapshots = false;
        try (final RowSequence.Iterator it = rows.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final long firstKey = it.peekNextKey();
                final int keyCount = it.getNextRowSequenceThrough(firstKey | INDEX_MASK).intSize();
                if (firstKey >> BLOCK1_SHIFT != level2Key) {
                    if (level2 != null) {
                        prepareLevel2(level2Key << BLOCK1_SHIFT, level2, prevLevel2, indices, count, mayHoldBlocks,
                                mayHoldCompacts, mayHoldSnapshots);
                    }
                    level2Key = firstKey >> BLOCK1_SHIFT;
                    level2 = ensureLevel2(firstKey);
                    prevLevel2 = trackPrevious ? ensurePrevLevel2(firstKey) : null;
                    count = 0;
                    mayHoldBlocks = false;
                    mayHoldCompacts = false;
                    mayHoldSnapshots = false;
                }
                final int block2 = block2(firstKey);
                final IntCompactLeaf compact = level2.getCompact(block2);
                final boolean isBlock = level2.getBlock(block2) != null;
                final boolean mayBeBlock =
                        !isBlock && (compact == null ? 0 : compact.size()) + keyCount > compactThreshold;
                mayHoldBlocks |= mayBeBlock;
                mayHoldCompacts |= !isBlock && !mayBeBlock;
                if (trackPrevious) {
                    final IntCompactLeaf recorded = prevLevel2.getCompact(block2);
                    mayHoldSnapshots |= prevLevel2.getBlock(block2) == null
                            && (recorded == null ? 0 : recorded.size()) + keyCount > recordThreshold;
                }
                if (count == indices.length) {
                    indices = Arrays.copyOf(indices, count * 2);
                }
                indices[count++] = block2;
            }
        }
        if (level2 != null) {
            prepareLevel2(level2Key << BLOCK1_SHIFT, level2, prevLevel2, indices, count, mayHoldBlocks,
                    mayHoldCompacts, mayHoldSnapshots);
        }
    }

    /**
     * Reserve the leaves at {@code indices[0, count)} of {@code level2}, the bottom level covering {@code key}, and of
     * {@code prevLevel2}, its previous values, if not null, and allocate the arrays that writing them may need.
     */
    private void prepareLevel2(
            final long key,
            final Level2 level2,
            final Level2 prevLevel2,
            final int[] indices,
            final int count,
            final boolean mayHoldBlocks,
            final boolean mayHoldCompacts,
            final boolean mayHoldSnapshots) {
        final Level2 prepared = level2.reserve(indices, count).withArrays(mayHoldBlocks, mayHoldCompacts);
        if (prepared != level2) {
            publishLevel2(false, key, prepared);
        }
        if (prevLevel2 != null) {
            // a snapshot may still be a compact leaf in between, when the rows are written in pieces
            final Level2 preparedPrev = prevLevel2.reserve(indices, count).withArrays(mayHoldSnapshots, true);
            if (preparedPrev != prevLevel2) {
                publishLevel2(true, key, preparedPrev);
            }
        }
    }

    /**
     * Build each leaf of {@code rows} with each of its row keys, as {@link #prepareLeaf} does, in the page table
     * entries that {@link #prepareLeafIndices} allocated, so that leaves of other rows may be built concurrently.
     */
    private void prepareLeaves(final RowSequence rows) {
        final boolean trackPrevious = prevFlusher != null;
        final int[] offsets = new int[BLOCK_SIZE];
        final PrevScratch scratch = trackPrevious ? new PrevScratch(recordThreshold) : null;
        final LeafRanges ranges = new LeafRanges();
        final LeafKeys keys = new LeafKeys();
        try (final RowSequence.Iterator it = rows.getRowSequenceIterator()) {
            while (it.hasMore()) {
                // each piece of the row keys is a whole number of leaves, so that each leaf is prepared once
                final RowSequence piece = it.getNextRowSequenceThrough(it.peekNextKey() | PREPARE_PIECE_MASK);
                final LeafCursor leaves = leafCursor(piece, ranges, keys);
                while (leaves.hasMore()) {
                    leaves.nextLeaf();
                    prepareLeaf(leaves, trackPrevious, offsets, scratch);
                }
            }
        }
    }

    /**
     * @return whether this cycle's writers populate rows that {@link #prepareLeafIndices} reserved, so that they must
     *         not change which leaves a bottom level holds
     */
    private boolean writesPreparedRows() {
        return prepareClockStep == updateGraph.clock().currentStep();
    }

    /**
     * @return whether this cycle's writers populate leaves that
     *         {@link #prepareForParallelPopulation(RowSequence, boolean)} prepared for divisions aligned to leaves, so
     *         that a leaf without room for a compact leaf must become a full block
     */
    private boolean writesAlignedDivisions() {
        return !preparedLeaves && writesPreparedRows();
    }

    /**
     * Build the current leaf of {@code leaves} with each of its row keys, holding null where it held no value, and
     * record the previous values of those row keys if {@code trackPrevious}.
     *
     * @param offsets room for the offsets of a leaf
     * @param scratch room for previous values, if {@code trackPrevious}
     */
    private void prepareLeaf(
            final LeafCursor leaves,
            final boolean trackPrevious,
            final int[] offsets,
            final PrevScratch scratch) {
        final long firstKey = leaves.firstKey;
        final int count = leaves.count;
        final int block2 = block2(firstKey);
        final Level2 level2 = ensureLevel2(firstKey);
        leaves.offsets(offsets);
        if (trackPrevious) {
            recordPrevious(firstKey, level2, block2, offsets, count, scratch);
        }
        if (level2.getBlock(block2) == null) {
            final IntCompactLeaf compact = level2.getCompact(block2);
            final int absent = compact == null ? count : compact.countAbsent(offsets, count);
            final IntCompactLeaf prepared = prepareCompactLeaf(firstKey, level2, compact, absent);
            if (prepared != null) {
                prepared.insertAbsent(offsets, count);
            }
        }
    }

    /**
     * Record the current value at {@code offset} of the leaf at {@code block2} of {@code level2} (null if not
     * allocated), which covers {@code key}, as its previous value, unless this cycle has already recorded it.
     */
    private void recordPrevious(final long key, final Level2 level2, final int block2, final int offset) {
        final Level2 prevLevel2 = ensurePrevLevel2(key);
        final int slot = prevLevel2.slot(block2);
        if (prevLevel2.blockAt(slot) != null) {
            return;
        }
        IntCompactLeaf recorded = prevLevel2.compactAt(slot);
        if (recorded != null && recorded.contains(offset)) {
            return;
        }
        final int value = getValue(level2, block2, offset);
        recorded = preparePrevLeaf(key, prevLevel2, block2, recorded, 1, level2);
        if (recorded != null) {
            recorded.put(offset, value);
        }
    }

    /**
     * Record the current values at {@code offsets[0, count)}, ascending and distinct, of the leaf at {@code block2} of
     * {@code level2} (null if not allocated), which covers {@code key}, as their previous values, except those this
     * cycle has already recorded.
     *
     * @param scratch holds the offsets to record and their values, when they fit in a compact leaf
     */
    private void recordPrevious(
            final long key,
            final Level2 level2,
            final int block2,
            final int[] offsets,
            final int count,
            final PrevScratch scratch) {
        final Level2 prevLevel2 = ensurePrevLevel2(key);
        final int slot = prevLevel2.slot(block2);
        if (prevLevel2.blockAt(slot) != null) {
            return;
        }
        final IntCompactLeaf alreadyRecorded = prevLevel2.compactAt(slot);
        final int absent = alreadyRecorded == null ? count : alreadyRecorded.countAbsent(offsets, count);
        if (absent == 0) {
            return;
        }
        final IntCompactLeaf recorded = preparePrevLeaf(key, prevLevel2, block2, alreadyRecorded, absent, level2);
        if (recorded == null) {
            return;
        }
        final int leafSlot = level2 == null ? -1 : level2.slot(block2);
        final int[] block = level2 == null ? null : level2.blockAt(leafSlot);
        final IntCompactLeaf compact = level2 == null || block != null ? null : level2.compactAt(leafSlot);
        int next = 0;
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            if (absent == count || !recorded.contains(offset)) {
                scratch.offsets[next] = offset;
                if (block != null) {
                    scratch.values[next] = block[offset];
                } else {
                    scratch.values[next] = compact == null ? NULL_INT : compact.get(offset);
                }
                ++next;
            }
        }
        recorded.putSorted(scratch.offsets, scratch.valuesChunk, 0, absent);
    }

    /**
     * Make the previous value leaf at {@code block2} of {@code prevLevel2}, whose recorded offsets are {@code recorded}
     * (or null if none), ready to record {@code count} more offsets of the leaf at {@code block2} of {@code level2}
     * (null if not allocated), both covering {@code key}.
     *
     * @return the leaf to record into, or null if the previous value leaf is now a snapshot of the whole leaf
     */
    private IntCompactLeaf preparePrevLeaf(
            final long key,
            final Level2 prevLevel2,
            final int block2,
            final IntCompactLeaf recorded,
            final int count,
            final Level2 level2) {
        final int newSize = (recorded == null ? 0 : recorded.size()) + count;
        if (newSize > recordThreshold) {
            // the offsets not yet recorded have not changed this cycle, so their current values are previous values
            final int[] snapshot = snapshotRecycler.borrowItem();
            final int[] block = level2 == null ? null : level2.getBlock(block2);
            if (block != null) {
                System.arraycopy(block, 0, snapshot, 0, BLOCK_SIZE);
            } else {
                Arrays.fill(snapshot, NULL_INT);
                final IntCompactLeaf compact = level2 == null ? null : level2.getCompact(block2);
                if (compact != null) {
                    compact.copyTo(snapshot);
                }
            }
            if (recorded != null) {
                recorded.copyTo(snapshot);
            }
            setBlock(true, key, prevLevel2, snapshot);
            return null;
        }
        if (recorded == null) {
            final IntCompactLeaf newRecorded = newCompactLeaf(count);
            setCompact(true, key, prevLevel2, newRecorded);
            return newRecorded;
        }
        return maybeLeaveSortedTier(true, key, prevLevel2, recorded, newSize);
    }

    /**
     * @return the current value at {@code offset} of the leaf at {@code block2} of {@code level2}, which may be null
     */
    private static int getValue(final Level2 level2, final int block2, final int offset) {
        if (level2 == null) {
            return NULL_INT;
        }
        final int slot = level2.slot(block2);
        final int[] block = level2.blockAt(slot);
        if (block != null) {
            return block[offset];
        }
        final IntCompactLeaf compact = level2.compactAt(slot);
        return compact == null ? NULL_INT : compact.get(offset);
    }

    /**
     * @return the value of {@code rowKey} at the start of this cycle
     */
    private int getPrevValue(final long rowKey) {
        final Level0 localPrevRoot = prevRoot;
        if (localPrevRoot != null && rowKey >= 0) {
            final Level2 prevLevel2 = getLevel2(localPrevRoot, rowKey);
            if (prevLevel2 != null) {
                final int slot = prevLevel2.slot(block2(rowKey));
                final int offset = (int) (rowKey & INDEX_MASK);
                final int[] snapshot = prevLevel2.blockAt(slot);
                if (snapshot != null) {
                    return snapshot[offset];
                }
                final IntCompactLeaf recorded = prevLevel2.compactAt(slot);
                if (recorded != null && recorded.contains(offset)) {
                    return recorded.get(offset);
                }
            }
        }
        return getInt(rowKey);
    }

    // region allocateNullFilledBlock
    private static int[] newBlock() {
        final int[] block = new int[BLOCK_SIZE];
        Arrays.fill(block, NULL_INT);
        return block;
    }
    // endregion allocateNullFilledBlock

    private static int[] toBlock(final IntCompactLeaf compact) {
        final int[] block = newBlock();
        compact.copyTo(block);
        return block;
    }

    // region primitive get
    @Override
    public final int getInt(final long rowKey) {
        if (rowKey < 0) {
            return NULL_INT;
        }
        final Level2 level2 = getLevel2(rowKey);
        if (level2 == null) {
            return NULL_INT;
        }
        final int slot = level2.slot(block2(rowKey));
        final int offset = (int) (rowKey & INDEX_MASK);
        final int[] block = level2.blockAt(slot);
        if (block != null) {
            return block[offset];
        }
        final IntCompactLeaf compact = level2.compactAt(slot);
        return compact == null ? NULL_INT : compact.get(offset);
    }

    @Override
    public final int getPrevInt(final long rowKey) {
        return getPrevValue(rowKey);
    }
    // endregion primitive get

    @Override
    public void startTrackingPrevValues() {
        if (prevFlusher != null) {
            throw new IllegalStateException(
                    "Can't call startTrackingPrevValues() twice: " + getClass().getCanonicalName());
        }
        prevFlusher = new UpdateCommitter<>(this, updateGraph, IntegerAdaptiveSparseArraySource::commitUpdates);
    }

    private void commitUpdates() {
        final Level0 localPrevRoot = prevRoot;
        prevRoot = null;
        if (localPrevRoot != null) {
            recyclePrevious(localPrevRoot);
        }
        if (blocksToClear == null) {
            return;
        }
        try (final RowSet ignored1 = blocksToClear;
                final RowSet ignored2 = blocks2ToClear;
                final RowSet ignored3 = blocks1ToClear) {
            if (emptyResult) {
                root = Level0.empty();
                return;
            }
            clearLeaves(blocksToClear);
            clearLevel2s(blocks2ToClear);
            clearLevel1s(blocks1ToClear);
        } finally {
            blocksToClear = null;
            blocks2ToClear = null;
            blocks1ToClear = null;
            emptyResult = false;
        }
    }

    /**
     * Release the leaves of {@code leafKeys}, row keys shifted right by {@code LOG_BLOCK_SIZE}, the leaves of each
     * bottom level together.
     */
    private void clearLeaves(final RowSet leafKeys) {
        int[] indices = new int[64];
        int count = 0;
        long level2Key = -1;
        try (final RowSet.Iterator it = leafKeys.iterator()) {
            while (it.hasNext()) {
                final long key = it.nextLong() << LOG_BLOCK_SIZE;
                if (key >> BLOCK1_SHIFT != level2Key) {
                    clearLeaves(level2Key, indices, count);
                    level2Key = key >> BLOCK1_SHIFT;
                    count = 0;
                }
                if (count == indices.length) {
                    indices = Arrays.copyOf(indices, count * 2);
                }
                indices[count++] = block2(key);
            }
        }
        clearLeaves(level2Key, indices, count);
    }

    /**
     * Release the leaves at {@code indices[0, count)} of the bottom level of {@code level2Key}, a row key shifted right
     * by {@code BLOCK1_SHIFT}, if any.
     */
    private void clearLeaves(final long level2Key, final int[] indices, final int count) {
        if (count == 0) {
            return;
        }
        final long key = level2Key << BLOCK1_SHIFT;
        final Level2 level2 = getLevel2(key);
        if (level2 != null) {
            final Level2 cleared = level2.clear(indices, count);
            if (cleared != level2) {
                publishLevel2(false, key, cleared);
            }
        }
    }

    /**
     * Release the bottom levels of {@code level2Keys}, row keys shifted right by {@code BLOCK1_SHIFT}, the levels of
     * each middle level together.
     */
    private void clearLevel2s(final RowSet level2Keys) {
        int[] indices = new int[64];
        int count = 0;
        long level1Key = -1;
        try (final RowSet.Iterator it = level2Keys.iterator()) {
            while (it.hasNext()) {
                final long key = it.nextLong() << BLOCK1_SHIFT;
                if (key >> BLOCK0_SHIFT != level1Key) {
                    clearLevel2s(level1Key, indices, count);
                    level1Key = key >> BLOCK0_SHIFT;
                    count = 0;
                }
                if (count == indices.length) {
                    indices = Arrays.copyOf(indices, count * 2);
                }
                indices[count++] = block1(key);
            }
        }
        clearLevel2s(level1Key, indices, count);
    }

    /**
     * Release the bottom levels at {@code indices[0, count)} of the middle level of {@code level1Key}, a row key
     * shifted right by {@code BLOCK0_SHIFT}, if any.
     */
    private void clearLevel2s(final long level1Key, final int[] indices, final int count) {
        if (count == 0) {
            return;
        }
        final int block0 = block0(level1Key << BLOCK0_SHIFT);
        final Level1 level1 = root.get(block0);
        if (level1 != null) {
            final Level1 cleared = level1.clear(indices, count);
            if (cleared != level1) {
                root.set(block0, cleared);
            }
        }
    }

    /**
     * Release the middle levels of {@code level1Keys}, row keys shifted right by {@code BLOCK0_SHIFT}.
     */
    private void clearLevel1s(final RowSet level1Keys) {
        if (level1Keys.isEmpty()) {
            return;
        }
        final int[] indices = new int[level1Keys.intSize()];
        int count = 0;
        try (final RowSet.Iterator it = level1Keys.iterator()) {
            while (it.hasNext()) {
                indices[count++] = block0(it.nextLong() << BLOCK0_SHIFT);
            }
        }
        root = root.clear(indices, count);
    }

    /**
     * Release {@code prev}, the previous value page table, returning its snapshot blocks and its arrays indexed by
     * child index, with every entry null, to their recyclers.
     */
    private static void recyclePrevious(final Level0 prev) {
        for (final Level1 child : prev.children) {
            if (child != null) {
                child.recyclePrevious();
            }
        }
    }

    @Override
    public void ensureCapacity(final long capacity, final boolean nullFilled) {
        // leaves are allocated when written, and unwritten row keys read as null
    }

    @Override
    public boolean isImmutable() {
        return immutable;
    }

    @Override
    public void setImmutable() {
        immutable = true;
    }

    // region boxed methods
    @Override
    public void set(final long key, final Integer value) {
        set(key, unbox(value));
    }
    // endregion boxed methods

    @Override
    public final void set(final long key, final int value) {
        final Level2 level2 = ensureLevel2(key);
        final int block2 = block2(key);
        final int offset = (int) (key & INDEX_MASK);
        if (shouldTrackPrevious()) {
            recordPrevious(key, level2, block2, offset);
        }
        final int[] block = level2.getBlock(block2);
        if (block != null) {
            block[offset] = value;
            return;
        }
        setInCompactLeaf(key, level2, offset, value);
    }

    /**
     * Set the value of {@code key}, whose leaf in {@code level2}, the bottom level covering it, is not a full block.
     *
     * @return the bottom level covering {@code key}, which replaces {@code level2} if the write changed the leaves it
     *         holds
     */
    private Level2 setInCompactLeaf(final long key, final Level2 level2, final int offset, final int value) {
        final int block2 = block2(key);
        IntCompactLeaf compact = level2.getCompact(block2);
        if (compact != null && compact.contains(offset)) {
            compact.put(offset, value);
            return level2;
        }
        compact = prepareCompactLeaf(key, level2, compact, 1);
        final Level2 current = getLevel2(key);
        if (compact == null) {
            current.getBlock(block2)[offset] = value;
        } else {
            compact.put(offset, value);
        }
        return current;
    }

    @Override
    public void setNull(final long key) {
        final Level2 level2 = getLevel2(key);
        if (level2 == null) {
            return;
        }
        final int block2 = block2(key);
        final int offset = (int) (key & INDEX_MASK);
        final int[] block = level2.getBlock(block2);
        if (block != null) {
            if (block[offset] != NULL_INT) {
                if (shouldTrackPrevious()) {
                    recordPrevious(key, level2, block2, offset);
                }
                block[offset] = NULL_INT;
            }
            return;
        }
        final IntCompactLeaf compact = level2.getCompact(block2);
        if (compact != null && compact.contains(offset)) {
            if (shouldTrackPrevious()) {
                recordPrevious(key, level2, block2, offset);
            }
            compact.remove(offset);
        }
    }

    @Override
    public void setNull(@NotNull final RowSequence rowSequence) {
        final boolean trackPrevious = shouldTrackPrevious();
        final int[] offsets = trackPrevious ? new int[BLOCK_SIZE] : null;
        final PrevScratch scratch = trackPrevious ? new PrevScratch(recordThreshold) : null;
        rowSequence.forAllRowKeyRanges((final long firstKey, final long lastKey) -> {
            long key = firstKey;
            while (true) {
                final long lastKeyInLeaf = Math.min(lastKey, key | INDEX_MASK);
                final Level2 level2 = getLevel2(key);
                if (level2 != null) {
                    final int block2 = block2(key);
                    final int firstOffset = (int) (key & INDEX_MASK);
                    final int lastOffset = (int) (lastKeyInLeaf & INDEX_MASK);
                    final int[] block = level2.getBlock(block2);
                    if (block != null) {
                        if (trackPrevious) {
                            // the offsets that already hold null are unchanged, so need no previous value
                            int count = 0;
                            for (int offset = firstOffset; offset <= lastOffset; ++offset) {
                                if (block[offset] != NULL_INT) {
                                    offsets[count++] = offset;
                                }
                            }
                            if (count > 0) {
                                recordPrevious(key, level2, block2, offsets, count, scratch);
                            }
                        }
                        Arrays.fill(block, firstOffset, lastOffset + 1, NULL_INT);
                    } else {
                        final IntCompactLeaf compact = level2.getCompact(block2);
                        if (compact != null) {
                            if (trackPrevious) {
                                final int count = compact.copyOffsets(firstOffset, lastOffset, offsets);
                                if (count > 0) {
                                    recordPrevious(key, level2, block2, offsets, count, scratch);
                                }
                            }
                            compact.removeRange(firstOffset, lastOffset);
                        }
                    }
                }
                if (lastKeyInLeaf == lastKey) {
                    break;
                }
                key = lastKeyInLeaf + 1;
            }
        });
    }

    @Override
    public void shift(final RowSet keysToShift, final long shiftDelta) {
        // Shifting up has to walk the keys in descending order so that a shifted value never overwrites one that has
        // not been read yet.
        try (final RowSet.SearchIterator it =
                (shiftDelta > 0) ? keysToShift.reverseIterator() : keysToShift.searchIterator()) {
            it.forEachLong((final long key) -> {
                set(key + shiftDelta, getInt(key));
                setNull(key);
                return true;
            });
        }
    }

    @Override
    public void clearBlocks(final RowSet blocksToClear, final RowSet removeBlocks2, final RowSet removeBlocks1,
            final boolean empty) {
        if (prevFlusher == null) {
            // without previous values, the rows of a static result are never removed
            return;
        }
        if (this.blocksToClear != null) {
            throw new IllegalStateException("Cannot call clearBlocks multiple times on the same cycle!");
        }
        Assert.eqNull(blocks2ToClear, "blocks2ToClear");
        Assert.eqNull(blocks1ToClear, "blocks1ToClear");
        this.blocksToClear = blocksToClear.copy();
        this.blocks2ToClear = removeBlocks2.copy();
        this.blocks1ToClear = removeBlocks1.copy();
        this.emptyResult = empty;
        prevFlusher.maybeActivate();
    }

    /**
     * The leaf being read by a chunked fill, and the position in the destination chunk. A fill of previous values also
     * reads the leaf's previous values: a snapshot of the whole leaf, or the recorded offsets.
     */
    private static final class FillState {
        private final Level0 prevRoot;
        private long maxKeyInCurrentLeaf = -1;
        private int[] block;
        private IntCompactLeaf compact;
        private int[] prevSnapshot;
        private IntCompactLeaf prevRecorded;
        private int destOffset;

        /**
         * @param prevRoot the previous values to read, or null to read current values
         */
        private FillState(final Level0 prevRoot) {
            this.prevRoot = prevRoot;
        }

        private void readLeaf(final IntegerAdaptiveSparseArraySource source, final long key) {
            maxKeyInCurrentLeaf = key | INDEX_MASK;
            final int block2 = block2(key);
            final Level2 level2 = source.getLevel2(key);
            if (level2 == null) {
                block = null;
                compact = null;
            } else {
                final int slot = level2.slot(block2);
                block = level2.blockAt(slot);
                compact = block == null ? level2.compactAt(slot) : null;
            }
            if (prevRoot != null) {
                final Level2 prevLevel2 = getLevel2(prevRoot, key);
                if (prevLevel2 == null) {
                    prevSnapshot = null;
                    prevRecorded = null;
                } else {
                    final int slot = prevLevel2.slot(block2);
                    prevSnapshot = prevLevel2.blockAt(slot);
                    prevRecorded = prevSnapshot == null ? prevLevel2.compactAt(slot) : null;
                }
            }
        }

        private int get(final int offset) {
            if (block != null) {
                return block[offset];
            }
            return compact == null ? NULL_INT : compact.get(offset);
        }

        private int getPrev(final int offset) {
            if (prevSnapshot != null) {
                return prevSnapshot[offset];
            }
            if (prevRecorded != null && prevRecorded.contains(offset)) {
                return prevRecorded.get(offset);
            }
            return get(offset);
        }

        /**
         * Fill {@code chunk} at the positions of {@code keys[start, end)}, row keys within the current leaf in
         * ascending order, with their values.
         */
        private void fillKeys(
                final WritableIntChunk<? super Values> chunk,
                final LongChunk<OrderedRowKeys> keys,
                final int start,
                final int end) {
            final int[] values = prevSnapshot != null ? prevSnapshot : block;
            if (values != null) {
                for (int ii = start; ii < end; ++ii) {
                    chunk.set(ii, values[(int) (keys.get(ii) & INDEX_MASK)]);
                }
                if (prevSnapshot != null) {
                    return;
                }
            } else if (compact != null) {
                compact.fillKeys(keys, start, end, chunk, start);
            } else {
                chunk.fillWithNullValue(start, end - start);
            }
            if (prevRecorded != null) {
                prevRecorded.overlayKeys(keys, start, end, chunk, start);
            }
        }

        /**
         * Fill {@code chunk} at {@link #destOffset} with the values of offsets {@code firstOffset} through
         * {@code firstOffset + length - 1}.
         */
        private void fillRange(final WritableIntChunk<? super Values> chunk, final int firstOffset, final int length) {
            if (prevSnapshot != null) {
                chunk.copyFromTypedArray(prevSnapshot, firstOffset, destOffset, length);
                return;
            }
            if (block != null) {
                chunk.copyFromTypedArray(block, firstOffset, destOffset, length);
            } else if (compact != null) {
                compact.fillRange(firstOffset, firstOffset + length - 1, chunk, destOffset);
            } else {
                chunk.fillWithNullValue(destOffset, length);
            }
            if (prevRecorded != null) {
                prevRecorded.overlayRange(firstOffset, firstOffset + length - 1, chunk, destOffset);
            }
        }
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        fill(dest.asWritableIntChunk(), rowSequence, null);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        fill(dest.asWritableIntChunk(), rowSequence, prevRoot);
    }

    /**
     * Fill {@code chunk} with the values of {@code rowSequence}, previous values if {@code prevRoot} is not null.
     */
    private void fill(
            final WritableIntChunk<? super Values> chunk,
            final RowSequence rowSequence,
            final Level0 prevRoot) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillByKeys(chunk, rowSequence, prevRoot);
        } else {
            fillByRanges(chunk, rowSequence, prevRoot);
        }
    }

    private void fillByRanges(
            final WritableIntChunk<? super Values> chunk,
            final RowSequence rowSequence,
            final Level0 prevRoot) {
        final FillState state = new FillState(prevRoot);
        rowSequence.forAllRowKeyRanges((final long firstKey, final long lastKey) -> {
            long key = firstKey;
            while (true) {
                if (key > state.maxKeyInCurrentLeaf) {
                    state.readLeaf(this, key);
                }
                final long lastKeyInLeaf = Math.min(lastKey, state.maxKeyInCurrentLeaf);
                final int length = (int) (lastKeyInLeaf - key + 1);
                state.fillRange(chunk, (int) (key & INDEX_MASK), length);
                state.destOffset += length;
                if (lastKeyInLeaf == lastKey) {
                    break;
                }
                key = lastKeyInLeaf + 1;
            }
        });
        chunk.setSize(state.destOffset);
    }

    /**
     * Fill {@code chunk} with the values of {@code rowSequence}, a leaf at a time: each run of row keys within one leaf
     * is read in one pass over the leaf.
     */
    private void fillByKeys(
            final WritableIntChunk<? super Values> chunk,
            final RowSequence rowSequence,
            final Level0 prevRoot) {
        final FillState state = new FillState(prevRoot);
        final LongChunk<OrderedRowKeys> keys = rowSequence.asRowKeyChunk();
        final int size = keys.size();
        int start = 0;
        while (start < size) {
            final long firstKey = keys.get(start);
            state.readLeaf(this, firstKey);
            final long lastKeyInLeaf = firstKey | INDEX_MASK;
            int end = start + 1;
            while (end < size && keys.get(end) <= lastKeyInLeaf) {
                ++end;
            }
            state.fillKeys(chunk, keys, start, end);
            start = end;
        }
        chunk.setSize(size);
    }

    // region getChunk
    @Override
    public IntChunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return IntChunk.getEmptyChunk();
        }
        final long firstKey = rowSequence.firstRowKey();
        final long lastKey = rowSequence.lastRowKey();
        if (lastKey - firstKey + 1 == rowSequence.size() && (firstKey >> BLOCK2_SHIFT == lastKey >> BLOCK2_SHIFT)) {
            // a contiguous range within one leaf can alias the leaf when it is a full block, or read as nulls when
            // there is no leaf
            final Level2 level2 = getLevel2(firstKey);
            final int slot = level2 == null ? -1 : level2.slot(block2(firstKey));
            if (level2 == null || level2.compactAt(slot) == null) {
                return DefaultGetContext.resetChunkFromArray(context,
                        level2 == null ? null : level2.blockAt(slot),
                        (int) (firstKey & INDEX_MASK),
                        (int) rowSequence.size());
            }
        }
        return getChunkByFilling(context, rowSequence).asIntChunk();
    }

    @Override
    public IntChunk<Values> getPrevChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (prevRoot == null) {
            return getChunk(context, rowSequence);
        }
        return getPrevChunkByFilling(context, rowSequence).asIntChunk();
    }
    // endregion getChunk

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillUnordered(dest.asWritableIntChunk(), keys, null);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillUnordered(dest.asWritableIntChunk(), keys, prevRoot);
    }

    /**
     * Fill {@code chunk} with the values of {@code keys}, previous values if {@code prevRoot} is not null.
     */
    private void fillUnordered(
            final WritableIntChunk<? super Values> chunk,
            final LongChunk<? extends RowKeys> keys,
            final Level0 prevRoot) {
        final FillState state = new FillState(prevRoot);
        long currentLeafKey = -1;
        final int size = keys.size();
        for (int ki = 0; ki < size; ++ki) {
            final long key = keys.get(ki);
            if (key == RowSequence.NULL_ROW_KEY) {
                chunk.set(ki, NULL_INT);
                continue;
            }
            final long leafKey = key & ~INDEX_MASK;
            if (leafKey != currentLeafKey) {
                state.readLeaf(this, key);
                currentLeafKey = leafKey;
            }
            final int offset = (int) (key & INDEX_MASK);
            chunk.set(ki, prevRoot == null ? state.get(offset) : state.getPrev(offset));
        }
        chunk.setSize(size);
    }

    @Override
    public boolean providesFillUnordered() {
        return true;
    }

    /**
     * Holds the walk over the leaves of a chunked write, the offsets of the keys it puts into one leaf, and the
     * previous values it records there.
     */
    private static final class AdaptiveFillFromContext implements FillFromContext {
        private final LeafRanges ranges = new LeafRanges();
        private final LeafKeys keys = new LeafKeys();
        private final int[] offsets = new int[BLOCK_SIZE];
        private final PrevScratch scratch;
        /** The leaf indices a chunk writes within one bottom level, to reserve together. */
        private int[] leafIndices = new int[64];

        private AdaptiveFillFromContext(final int compactThreshold) {
            scratch = new PrevScratch(compactThreshold);
        }
    }

    @Override
    public FillFromContext makeFillFromContext(final int chunkCapacity) {
        return new AdaptiveFillFromContext(recordThreshold);
    }

    @Override
    public void fillFromChunk(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return;
        }
        final AdaptiveFillFromContext fillFromContext = (AdaptiveFillFromContext) context;
        final LeafCursor leaves = leafCursor(rowSequence, fillFromContext.ranges, fillFromContext.keys);
        final int[] offsets = fillFromContext.offsets;
        final IntChunk<? extends Values> values = src.asIntChunk();
        final boolean trackPrevious = shouldTrackPrevious();
        final boolean alignedDivisions = writesAlignedDivisions();
        if (!writesPreparedRows()) {
            reserveLeaves(leaves, fillFromContext);
            leaves.rewind();
        }
        int srcOffset = 0;
        while (leaves.hasMore()) {
            leaves.nextLeaf();
            final long firstKey = leaves.firstKey;
            final int count = leaves.count;
            final Level2 level2 = ensureLevel2(firstKey);
            final int block2 = block2(firstKey);
            final int slot = level2.slot(block2);
            int[] block = level2.blockAt(slot);
            final IntCompactLeaf existing = block == null ? level2.compactAt(slot) : null;
            // a new leaf of more values than a compact leaf holds becomes a full block, which needs no offsets, and
            // previous values need them only when they may be recorded in a compact leaf
            if (trackPrevious
                    || (block == null && (existing != null || count <= compactThreshold))) {
                leaves.offsets(offsets);
            }
            if (trackPrevious) {
                recordPrevious(firstKey, level2, block2, offsets, count, fillFromContext.scratch);
            }
            if (block == null && existing == null && alignedDivisions && !level2.hasRoomForCompact()) {
                // the preparation left room only for a full block, because the leaf will hold more values than a
                // compact leaf holds
                block = newBlock();
                setBlock(false, firstKey, level2, block);
            }
            if (block == null) {
                final int absent = existing == null ? count : existing.countAbsent(offsets, count);
                final IntCompactLeaf compact = prepareCompactLeaf(firstKey, level2, existing, absent);
                if (compact != null) {
                    compact.putSorted(offsets, values, srcOffset, count);
                    srcOffset += count;
                    continue;
                }
                block = getLevel2(firstKey).getBlock(block2);
            }
            if (values.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }
            leaves.copyTo(values, srcOffset, block);
            srcOffset += count;
        }
    }

    /**
     * Give every leaf of {@code leaves} a position in its bottom level, the leaves of each bottom level reserved
     * together, so that a level grows once for a chunk rather than once for each new leaf.
     */
    private void reserveLeaves(final LeafCursor leaves, final AdaptiveFillFromContext context) {
        int[] indices = context.leafIndices;
        int count = 0;
        long level2Key = -1;
        Level2 level2 = null;
        while (leaves.hasMore()) {
            leaves.nextLeaf();
            final long key = leaves.firstKey;
            if (key >> BLOCK1_SHIFT != level2Key) {
                if (level2 != null) {
                    reserveLeaves(level2Key << BLOCK1_SHIFT, level2, indices, count);
                }
                level2Key = key >> BLOCK1_SHIFT;
                level2 = ensureLevel2(key);
                count = 0;
            }
            if (count == indices.length) {
                indices = context.leafIndices = Arrays.copyOf(indices, count * 2);
            }
            indices[count++] = block2(key);
        }
        if (level2 != null) {
            reserveLeaves(level2Key << BLOCK1_SHIFT, level2, indices, count);
        }
    }

    /**
     * Give the leaves at {@code indices[0, count)} of {@code level2}, the bottom level covering {@code key}, positions.
     */
    private void reserveLeaves(final long key, final Level2 level2, final int[] indices, final int count) {
        final Level2 reserved = level2.reserve(indices, count);
        if (reserved != level2) {
            publishLevel2(false, key, reserved);
        }
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys) {
        final IntChunk<? extends Values> values = src.asIntChunk();
        final boolean trackPrevious = shouldTrackPrevious();
        final int size = keys.size();
        long currentLeafKey = -1;
        Level2 level2 = null;
        int block2 = 0;
        for (int ki = 0; ki < size; ++ki) {
            final long key = keys.get(ki);
            final long leafKey = key & ~INDEX_MASK;
            if (leafKey != currentLeafKey) {
                level2 = ensureLevel2(key);
                block2 = block2(key);
                currentLeafKey = leafKey;
                final int[] block = level2.getBlock(block2);
                if (block != null && values.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
            }
            final int offset = (int) (key & INDEX_MASK);
            if (trackPrevious) {
                recordPrevious(key, level2, block2, offset);
            }
            final int[] block = level2.getBlock(block2);
            if (block != null) {
                block[offset] = values.get(ki);
            } else {
                level2 = setInCompactLeaf(key, level2, offset, values.get(ki));
            }
        }
    }

    @TestUseOnly
    @Override
    public long estimateSize() {
        return root.estimateSize();
    }

    // region reinterpretation
    // endregion reinterpretation
}
