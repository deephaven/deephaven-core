//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterSparseArraySource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import io.deephaven.base.verify.Require;
import java.time.ZoneId;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
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
import io.deephaven.engine.table.impl.sources.sparse.LongBitmapLeaf;
import io.deephaven.engine.table.impl.sources.sparse.LongCompactLeaf;
import io.deephaven.engine.table.impl.sources.sparse.LongSortedLeaf;
import io.deephaven.engine.table.impl.sources.sparse.ChildPositions;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.util.SoftRecycler;
import io.deephaven.util.SoftRecyclerWindow;
import io.deephaven.util.annotations.TestUseOnly;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;
import static io.deephaven.util.QueryConstants.NULL_LONG;
// region boxing imports
import static io.deephaven.util.type.TypeUtils.unbox;
// endregion boxing imports

/**
 * A sparse in-memory column source of longs whose leaves adapt to the number of values they hold.
 * <p>
 * Row keys are addressed through three page table levels above leaves that each cover
 * {@link io.deephaven.engine.table.impl.sources.sparse.SparseConstants#BLOCK_SIZE} consecutive row keys. A leaf is one
 * of:
 * <ul>
 * <li>a {@link LongSortedLeaf}, for at most {@link #DEFAULT_SORTED_THRESHOLD} values</li>
 * <li>a {@link LongBitmapLeaf}, for at most {@link #DEFAULT_COMPACT_THRESHOLD} values</li>
 * <li>a full block of {@code BLOCK_SIZE} values</li>
 * </ul>
 * A leaf takes the next representation when a write would take it past its threshold, and never returns to a smaller
 * one.
 * <p>
 * The middle and bottom page table levels hold their children densely, at positions ranked among the child indices they
 * hold: sorted indices for a few children, then a bitmap with the number of indices before each word, following the
 * leaves' own tiers. A level of more than an eighth of its children indexes its arrays by child index instead. The top
 * level holds its children at the positions of their indices in a sorted array, since it is indexed by the high bits of
 * a row key and rarely holds more than a few. Each level is held directly by its parent, or by the source for the top
 * level, so that a read follows one reference per level. A level's positions and arrays are fixed but for appending
 * positions and setting children at them; any other change makes a new level, which replaces the old one in its parent,
 * so that a concurrent reader finds each child where the positions it read say.
 * <p>
 * When tracking previous values, the first write to a row in a cycle records the row's value in a second page table of
 * previous values. Each leaf of that table holds the recorded offsets of its leaf as a sorted or bitmap leaf, following
 * the same thresholds, or, once more offsets are recorded than a compact leaf holds, a full block that snapshots the
 * whole leaf. A row without a recorded value has not changed this cycle, so its previous value is its current value.
 * The previous values are discarded when the cycle completes. The snapshot blocks and the arrays of the previous value
 * page table come from recyclers shared by the sources of this type; the leaves and page table levels created in a
 * cycle are listed, so that completing it clears exactly the entries set and returns each array indexed by leaf index
 * with every entry null.
 * <p>
 * For parallel population, {@link #prepareForParallelPopulation(RowSequence, boolean)} allocates the page table above
 * each leaf to be written and reserves the leaf's position, for current and previous values, so that writers of
 * different leaves change no shared structure. When the writers' divisions are aligned to leaves, each writer builds
 * and records its own leaves; a bottom level gets an array of compact leaves only when one of its leaves may be
 * compact, and a writer makes a leaf with no room for a compact leaf a full block from its first chunk. Otherwise it
 * also builds each leaf with every offset to be written, holding null, and records the previous values, so that writers
 * only store values.
 */
public class LongSparseArraySource extends SparseArrayColumnSource<Long>
        implements MutableColumnSourceGetDefaults.ForLong , ConvertibleTimeSource {

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
    private static final SoftRecycler<long[]> snapshotRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.LONG_RECYCLER_CAPACITY,
                    SparseArrayColumnSourceConfiguration.LONG_RECYCLER_MAXIMUM_CAPACITY,
                    () -> new long[BLOCK_SIZE], null));
    // endregion recycler
    // The arrays of the previous value page table, which are returned with every entry null.
    private static final SoftRecycler<long[][]> blocksArrayRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.LONG_RECYCLER_CAPACITY2,
                    SparseArrayColumnSourceConfiguration.LONG_RECYCLER_MAXIMUM_CAPACITY2,
                    () -> new long[BLOCK2_SIZE][], null));
    private static final SoftRecycler<LongCompactLeaf[]> compactsArrayRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.LONG_RECYCLER_CAPACITY2,
                    SparseArrayColumnSourceConfiguration.LONG_RECYCLER_MAXIMUM_CAPACITY2,
                    () -> new LongCompactLeaf[BLOCK2_SIZE], null));
    private static final SoftRecycler<Level2[]> level2ArrayRecycler =
            SoftRecyclerWindow.register(new SoftRecycler<>(SparseArrayColumnSourceConfiguration.LONG_RECYCLER_CAPACITY1,
                    SparseArrayColumnSourceConfiguration.LONG_RECYCLER_MAXIMUM_CAPACITY1,
                    () -> new Level2[BLOCK1_SIZE], null));

    private final int sortedThreshold;
    private final int compactThreshold;
    private final int recordThreshold;
    /** The current values; a change that a level cannot make in place publishes a new level here. */
    private Level0 root = new Level0(1);

    /**
     * Commits the previous values at the end of each cycle in which a value changed; null when not tracking previous
     * values.
     */
    private transient UpdateCommitter<LongSparseArraySource> prevFlusher;
    /**
     * The previous values recorded this cycle, or null if none have been recorded.
     */
    private transient Level0 prevRoot;
    /**
     * The leaves of {@link #prevRoot}, as row keys shifted right by {@code LOG_BLOCK_SIZE}, so that completing the
     * cycle clears their entries and returns their snapshot blocks without reading through the page table. Writers of
     * different leaves may add to it concurrently, so it and {@link #prevLevel2Keys} are guarded by its lock.
     */
    private transient final LongArrayList prevLeafKeys = new LongArrayList();
    /**
     * The bottom page table levels of {@link #prevRoot}, as row keys shifted right by {@code BLOCK1_SHIFT}, so that
     * completing the cycle returns their arrays and clears their entries in the levels above.
     */
    private transient final LongArrayList prevLevel2Keys = new LongArrayList();

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
     * Make a source whose leaves are all full blocks.
     */
    public LongSparseArraySource() {
        this(DEFAULT_SORTED_THRESHOLD, 0);
    }

    /**
     * @param sortedThreshold the largest number of values a sorted leaf holds
     * @param compactThreshold the largest number of values a sorted or bitmap leaf holds, from 0, when every leaf is a
     *        full block, to the block size
     */
    public LongSparseArraySource(final int sortedThreshold, final int compactThreshold) {
        super(long.class);
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
     * The number of children past which a middle or bottom page table level indexes its arrays of children by child
     * index. Up to an eighth of the level, ranked positions keep the arrays small; past it, an array indexed by child
     * index holds at most eight entries for each child it holds.
     */
    private static final int DIRECT_THRESHOLD = BLOCK2_SIZE / 8;

    /**
     * @return the length of the arrays for {@code size} children of a ranked level whose arrays have length
     *         {@code capacity}: the same if they fit, else at least double, so that a level that grows a few children at
     *         a time is copied a logarithmic number of times
     */
    private static int grownCapacity(final int capacity, final int size) {
        return size <= capacity ? capacity : Math.max(size, Math.min(DIRECT_THRESHOLD, capacity * 2));
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

    /**
     * The top page table level: the {@link Level1} children it holds, at the positions of their indices in a sorted
     * array. A row key's index at this level is its bits from {@code BLOCK0_SHIFT} up, so that an array indexed by
     * child index would have {@code BLOCK0_SIZE} entries; this level is never indexed directly, and usually holds one
     * child.
     * <p>
     * Appending an index past the last one writes an unused entry and then the size. Anything else that changes the
     * indices makes a new level, which the source publishes as its root, so that a reader that loaded a level finds
     * each child where its indices say. A child is replaced at its position.
     */
    private static final class Level0 {
        private final int[] indices;
        private final Level1[] children;
        private int size;

        private Level0(final int capacity) {
            indices = new int[capacity];
            children = new Level1[capacity];
        }

        /**
         * @return the position of {@code index}, or -1 if it has none
         */
        private int find(final int index) {
            int low = 0;
            int high = size - 1;
            while (low <= high) {
                final int mid = (low + high) >>> 1;
                final int midIndex = indices[mid];
                if (midIndex < index) {
                    low = mid + 1;
                } else if (midIndex > index) {
                    high = mid - 1;
                } else {
                    return mid;
                }
            }
            return -1;
        }

        private Level1 get(final int index) {
            final int position = find(index);
            return position < 0 ? null : children[position];
        }

        /**
         * @return this level, or a new level holding the same children, with a position for {@code index}, which has
         *         none
         */
        private Level0 reserve(final int index) {
            final int oldSize = size;
            if ((oldSize == 0 || index > indices[oldSize - 1]) && oldSize < indices.length) {
                indices[oldSize] = index;
                size = oldSize + 1;
                return this;
            }
            final Level0 grown = new Level0(oldSize < indices.length ? indices.length : indices.length * 2);
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
            grown.size = oldSize + 1;
            return grown;
        }

        /**
         * @return this level, or a new level, without the children at {@code removed[0, count)}, ascending
         */
        private Level0 clear(final int[] removed, final int count) {
            final int[] held = Arrays.copyOf(indices, size);
            final int[] kept = new int[held.length];
            final int[] keptFrom = new int[held.length];
            final int keptCount = ChildPositions.keep(held, removed, count, kept, keptFrom);
            if (keptCount == held.length) {
                return this;
            }
            final Level0 result = new Level0(Math.max(1, keptCount));
            System.arraycopy(kept, 0, result.indices, 0, keptCount);
            gather(children, result.children, keptFrom, keptCount);
            result.size = keptCount;
            return result;
        }

        private long estimateSize() {
            if (size == 0) {
                return 0;
            }
            long estimate = (long) indices.length * (Integer.BYTES + Long.BYTES);
            for (int ii = 0; ii < size; ++ii) {
                if (children[ii] != null) {
                    estimate += children[ii].estimateSize();
                }
            }
            return estimate;
        }
    }

    /**
     * The middle page table level: the {@link Level2} children it holds. A level of up to {@link #DIRECT_THRESHOLD}
     * children holds them densely, in child index order, at the positions its {@link ChildPositions} report; a level of
     * more indexes its array by child index.
     * <p>
     * The positions and the array of a level are fixed, except that appending positions past the last one changes the
     * positions in place, once the array has room for them, and a child is replaced at its position. Anything else
     * makes a new level, which the source publishes in its parent, so that a reader that loaded a level finds each
     * child where its positions say.
     */
    private static final class Level1 {
        /**
         * Whether this level holds previous values, whose array indexed by child index is borrowed from a recycler and
         * returned when the cycle completes.
         */
        private final boolean recycled;
        /** The positions of the children, or null when the array is indexed by child index. */
        private final ChildPositions positions;
        private final Level2[] children;

        private Level1(final boolean recycled, final ChildPositions positions, final Level2[] children) {
            this.recycled = recycled;
            this.positions = positions;
            this.children = children;
        }

        private static Level1 empty(final boolean recycled) {
            return new Level1(recycled, ChildPositions.empty(), new Level2[1]);
        }

        /**
         * @return the position of the child at {@code index}, or -1 if it has none
         */
        private int slot(final int index) {
            return positions == null ? index : positions.find(index);
        }

        private Level2 get(final int index) {
            final int slot = slot(index);
            // a position appended after this level was replaced may be past its array
            return slot < 0 || slot >= children.length ? null : children[slot];
        }

        /** Make {@code child} the child at {@code index}, which has a position. */
        private void set(final int index, final Level2 child) {
            children[slot(index)] = child;
        }

        /**
         * @return this level, or a new level holding the same children, with a position for {@code index}
         */
        private Level1 reserve(final int index) {
            if (positions == null || positions.find(index) >= 0) {
                return this;
            }
            final int newSize = positions.size() + 1;
            if (newSize > DIRECT_THRESHOLD) {
                final Level2[] direct = recycled ? level2ArrayRecycler.borrowItem() : new Level2[BLOCK1_SIZE];
                final int[] held = positions.indices();
                return new Level1(recycled, null, scatter(children, direct, held, held.length));
            }
            final int[] added = {index};
            if (index > positions.lastIndex()) {
                Level1 level = this;
                if (newSize > children.length) {
                    // grow the array first, so that a position appended in place is within the array readers find
                    level = new Level1(recycled, positions,
                            Arrays.copyOf(children, grownCapacity(children.length, newSize)));
                }
                final ChildPositions appended = level.positions.append(added, 1);
                return appended == level.positions ? level : new Level1(recycled, appended, level.children);
            }
            final int[] held = positions.indices();
            final int[] movedTo = new int[held.length];
            final int[] merged = ChildPositions.merge(held, added, 1, movedTo);
            return new Level1(recycled, ChildPositions.of(merged, newSize),
                    scatter(children, new Level2[grownCapacity(children.length, newSize)], movedTo, held.length));
        }

        /**
         * @return this level, or a new level, without the children at {@code removed[0, count)}, ascending
         */
        private Level1 clear(final int[] removed, final int count) {
            if (positions == null) {
                for (int ii = 0; ii < count; ++ii) {
                    children[removed[ii]] = null;
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
            return new Level1(recycled, ChildPositions.of(kept, keptCount),
                    gather(children, new Level2[keptCount], keptFrom, keptCount));
        }

        /**
         * Release the child at {@code index} of a previous value level when the cycle completes. Only an array indexed
         * by child index is reused, so only its entries need clearing; the level is discarded with any other array.
         */
        private void clearPrevious(final int index) {
            if (positions == null) {
                children[index] = null;
            }
        }

        /** Return the array indexed by child index, whose entries must all be null, to its recycler. */
        private void recycleArray() {
            if (positions == null) {
                level2ArrayRecycler.returnItem(children);
            }
        }

        private long estimateSize() {
            long estimate = positions == null ? 0 : positions.estimateSize();
            estimate += (long) children.length * Long.BYTES;
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
     * so that reading a full block needs no test of the leaf's type. A level of up to {@link #DIRECT_THRESHOLD} leaves
     * holds them densely, in leaf index order, at the positions its {@link ChildPositions} report; a level of more
     * indexes its arrays by leaf index.
     * <p>
     * The positions and the arrays of a level are fixed, except that appending positions past the last one changes the
     * positions in place, once the arrays have room for them, and a leaf is set at its position. Anything else, such as
     * moving a leaf or growing an array, makes a new level, which the source publishes in its parent, so that a reader
     * that loaded a level finds each leaf where its positions say. A writer therefore uses the level that its last
     * change returned.
     * <p>
     * Only one thread changes which leaves a level holds at a time; a parallel population sets only the leaves at
     * positions its preparation reserved, in arrays it allocated.
     */
    private static final class Level2 {
        /**
         * Whether this level holds previous values, whose arrays indexed by leaf index are borrowed from recyclers and
         * returned when the cycle completes.
         */
        private final boolean recycled;
        /** The positions of the leaves, or null when the arrays are indexed by leaf index. */
        private final ChildPositions positions;
        /** The length of the arrays of leaves, which holds every position. */
        private final int capacity;
        /** The full blocks, or null if no leaf may be one. */
        private final long[][] blocks;
        /** The compact leaves, or null if no leaf may be one. */
        private final LongCompactLeaf[] compacts;

        private Level2(
                final boolean recycled,
                final ChildPositions positions,
                final int capacity,
                final long[][] blocks,
                final LongCompactLeaf[] compacts) {
            this.recycled = recycled;
            this.positions = positions;
            this.capacity = capacity;
            this.blocks = blocks;
            this.compacts = compacts;
        }

        private static Level2 empty(final boolean recycled) {
            return new Level2(recycled, ChildPositions.empty(), 0, null, null);
        }

        /**
         * @return the position of the leaf at {@code index}, or -1 if it has none
         */
        private int slot(final int index) {
            return positions == null ? index : positions.find(index);
        }

        /**
         * @return the full block at position {@code slot}, or null if there is none
         */
        private long[] blockAt(final int slot) {
            final long[][] localBlocks = blocks;
            // a position appended after this level was replaced may be past its arrays
            return localBlocks == null || slot < 0 || slot >= localBlocks.length ? null : localBlocks[slot];
        }

        /**
         * @return the compact leaf at position {@code slot}, or null if there is none
         */
        private LongCompactLeaf compactAt(final int slot) {
            final LongCompactLeaf[] localCompacts = compacts;
            return localCompacts == null || slot < 0 || slot >= localCompacts.length ? null : localCompacts[slot];
        }

        private long[] getBlock(final int index) {
            return blockAt(slot(index));
        }

        private LongCompactLeaf getCompact(final int index) {
            return compactAt(slot(index));
        }

        private long[][] newDirectBlocks() {
            return recycled ? blocksArrayRecycler.borrowItem() : new long[BLOCK2_SIZE][];
        }

        private LongCompactLeaf[] newDirectCompacts() {
            return recycled ? compactsArrayRecycler.borrowItem() : new LongCompactLeaf[BLOCK2_SIZE];
        }

        /**
         * Give each of {@code indices[0, count)}, ascending and distinct, a position, so that setting its leaf writes
         * only its own entry. Positions after the existing ones are appended; otherwise the leaves are moved once, into
         * new arrays.
         *
         * @return this level, or a new level holding the same leaves, with the positions
         */
        private Level2 reserve(final int[] indices, final int count) {
            if (positions == null) {
                return this;
            }
            int absent = 0;
            for (int ii = 0; ii < count; ++ii) {
                if (positions.find(indices[ii]) < 0) {
                    ++absent;
                }
            }
            if (absent == 0) {
                return this;
            }
            final int[] absentIndices;
            if (absent == count) {
                absentIndices = indices;
            } else {
                absentIndices = new int[absent];
                int next = 0;
                for (int ii = 0; ii < count; ++ii) {
                    if (positions.find(indices[ii]) < 0) {
                        absentIndices[next++] = indices[ii];
                    }
                }
            }
            final int newSize = positions.size() + absent;
            if (newSize > DIRECT_THRESHOLD) {
                return makeDirect();
            }
            if (absentIndices[0] > positions.lastIndex()) {
                return append(absentIndices, absent, newSize);
            }
            return merge(absentIndices, absent, newSize);
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
                level = new Level2(recycled, positions, newCapacity,
                        blocks == null ? null : Arrays.copyOf(blocks, newCapacity),
                        compacts == null ? null : Arrays.copyOf(compacts, newCapacity));
            }
            final ChildPositions appended = level.positions.append(indices, count);
            return appended == level.positions
                    ? level
                    : new Level2(recycled, appended, level.capacity, level.blocks, level.compacts);
        }

        /**
         * @return a new level holding {@code indices[0, count)}, none held, among the indices held, for {@code newSize}
         *         leaves in all, with each leaf held moved once into new arrays
         */
        private Level2 merge(final int[] indices, final int count, final int newSize) {
            final int[] held = positions.indices();
            final int[] movedTo = new int[held.length];
            final int[] merged = ChildPositions.merge(held, indices, count, movedTo);
            final int newCapacity = grownCapacity(capacity, newSize);
            return new Level2(recycled, ChildPositions.of(merged, newSize), newCapacity,
                    blocks == null ? null : scatter(blocks, new long[newCapacity][], movedTo, held.length),
                    compacts == null
                            ? null
                            : scatter(compacts, new LongCompactLeaf[newCapacity], movedTo, held.length));
        }

        /**
         * @return a new level holding the same leaves, in arrays indexed by leaf index
         */
        private Level2 makeDirect() {
            final int[] held = positions.indices();
            return new Level2(recycled, null, BLOCK2_SIZE,
                    blocks == null ? null : scatter(blocks, newDirectBlocks(), held, held.length),
                    compacts == null ? null : scatter(compacts, newDirectCompacts(), held, held.length));
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
            if (blocks != null) {
                return this;
            }
            return new Level2(recycled, positions, capacity,
                    positions == null ? newDirectBlocks() : new long[capacity][], compacts);
        }

        /**
         * @return this level, or a new level holding the same leaves, with an array of compact leaves
         */
        private Level2 withCompacts() {
            if (compacts != null) {
                return this;
            }
            return new Level2(recycled, positions, capacity, blocks,
                    positions == null ? newDirectCompacts() : new LongCompactLeaf[capacity]);
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
            return compacts != null;
        }

        /**
         * @return this level, or a new level, without the leaves at {@code indices[0, count)}, ascending
         */
        private Level2 clear(final int[] indices, final int count) {
            if (positions == null) {
                for (int ii = 0; ii < count; ++ii) {
                    if (blocks != null) {
                        blocks[indices[ii]] = null;
                    }
                    if (compacts != null) {
                        compacts[indices[ii]] = null;
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
            return new Level2(recycled, ChildPositions.of(kept, keptCount), keptCount,
                    blocks == null ? null : gather(blocks, new long[keptCount][], keptFrom, keptCount),
                    compacts == null ? null : gather(compacts, new LongCompactLeaf[keptCount], keptFrom, keptCount));
        }

        /**
         * Release the leaf at {@code index} of a previous value level when the cycle completes. Only arrays indexed by
         * leaf index are reused, so only their entries need clearing; the level is discarded with any other arrays.
         */
        private void clearPrevious(final int index) {
            if (positions != null) {
                return;
            }
            if (blocks != null) {
                blocks[index] = null;
            }
            if (compacts != null) {
                compacts[index] = null;
            }
        }

        /** Return the arrays indexed by leaf index, whose entries must all be null, to their recyclers. */
        private void recycleArrays() {
            if (positions != null) {
                return;
            }
            if (blocks != null) {
                blocksArrayRecycler.returnItem(blocks);
            }
            if (compacts != null) {
                compactsArrayRecycler.returnItem(compacts);
            }
        }

        private long estimateSize() {
            long estimate = positions == null ? 0 : positions.estimateSize();
            if (blocks != null) {
                estimate += (long) blocks.length * Long.BYTES;
                for (final long[] block : blocks) {
                    if (block != null) {
                        estimate += (long) BLOCK_SIZE * Long.BYTES;
                    }
                }
            }
            if (compacts != null) {
                estimate += (long) compacts.length * Long.BYTES;
                for (final LongCompactLeaf compact : compacts) {
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
            prevRoot = new Level0(1);
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
        int position0 = level0.find(block0);
        if (position0 < 0) {
            final Level0 reserved = level0.reserve(block0);
            if (reserved != level0) {
                level0 = reserved;
                if (prev) {
                    prevRoot = reserved;
                } else {
                    root = reserved;
                }
            }
            position0 = level0.find(block0);
            level0.children[position0] = Level1.empty(prev);
        }
        Level1 level1 = level0.children[position0];
        final int block1 = block1(key);
        Level2 level2 = level1.get(block1);
        if (level2 != null) {
            return level2;
        }
        final Level1 reserved = level1.reserve(block1);
        if (reserved != level1) {
            level1 = reserved;
            level0.children[position0] = reserved;
        }
        level2 = Level2.empty(prev);
        level1.set(block1, level2);
        if (prev) {
            synchronized (prevLeafKeys) {
                prevLevel2Keys.add(key >> BLOCK1_SHIFT);
            }
        }
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
    private Level2 setBlock(final boolean prev, final long key, final Level2 level2, final long[] block) {
        final int index = block2(key);
        final Level2 updated = level2.reserveOne(index).withBlocks();
        if (updated != level2) {
            publishLevel2(prev, key, updated);
        }
        final int slot = updated.slot(index);
        updated.blocks[slot] = block;
        if (updated.compacts != null) {
            updated.compacts[slot] = null;
        }
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
            final LongCompactLeaf compact) {
        final int index = block2(key);
        final Level2 updated = level2.reserveOne(index).withCompacts();
        if (updated != level2) {
            publishLevel2(prev, key, updated);
        }
        updated.compacts[updated.slot(index)] = compact;
        return updated;
    }
    // endregion page table

    /**
     * @return an empty compact leaf with room for {@code capacity} values
     */
    private LongCompactLeaf newCompactLeaf(final int capacity) {
        return capacity <= sortedThreshold ? new LongSortedLeaf(capacity) : new LongBitmapLeaf(capacity);
    }

    /**
     * Make the leaf covering {@code key} in {@code level2}, the current bottom level covering it, whose compact leaf is
     * {@code compact} (or null if the leaf is absent), ready for {@code count} more offsets. The leaf may move to a new
     * bottom level, so a caller that writes the leaf again reads the bottom level again.
     *
     * @return the compact leaf to write, or null if the leaf is now a full block
     */
    private LongCompactLeaf prepareCompactLeaf(
            final long key,
            final Level2 level2,
            final LongCompactLeaf compact,
            final int count) {
        if (compact == null) {
            if (count > compactThreshold) {
                setBlock(false, key, level2, newBlock());
                return null;
            }
            final LongCompactLeaf newCompact = newCompactLeaf(count);
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
        abstract void copyTo(LongChunk<? extends Values> values, int srcOffset, long[] block);

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
        void copyTo(final LongChunk<? extends Values> values, final int srcOffset, final long[] block) {
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
        void copyTo(final LongChunk<? extends Values> values, final int srcOffset, final long[] block) {
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
        private final long[] values;
        private final LongChunk<? extends Values> valuesChunk;

        /**
         * @param capacity the most offsets a compact leaf holds
         */
        private PrevScratch(final int capacity) {
            offsets = new int[capacity];
            values = new long[capacity];
            valuesChunk = LongChunk.chunkWrap(values);
        }
    }

    /**
     * @return {@code compact}, the leaf covering {@code key} in {@code level2}, the bottom level covering it in the
     *         previous value page table if {@code prev}, else in the current one; or a bitmap leaf with its values that
     *         replaces it if it is a sorted leaf and {@code newSize} is more than a sorted leaf holds
     */
    private LongCompactLeaf maybeLeaveSortedTier(
            final boolean prev,
            final long key,
            final Level2 level2,
            final LongCompactLeaf compact,
            final int newSize) {
        if (newSize <= sortedThreshold || !(compact instanceof LongSortedLeaf)) {
            return compact;
        }
        final LongBitmapLeaf bitmap = new LongBitmapLeaf(newSize);
        ((LongSortedLeaf) compact).copyTo(bitmap);
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
                final LongCompactLeaf compact = level2.getCompact(block2);
                final boolean isBlock = level2.getBlock(block2) != null;
                final boolean mayBeBlock =
                        !isBlock && (compact == null ? 0 : compact.size()) + keyCount > compactThreshold;
                mayHoldBlocks |= mayBeBlock;
                mayHoldCompacts |= !isBlock && !mayBeBlock;
                if (trackPrevious) {
                    final LongCompactLeaf recorded = prevLevel2.getCompact(block2);
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
            final LongCompactLeaf compact = level2.getCompact(block2);
            final int absent = compact == null ? count : compact.countAbsent(offsets, count);
            final LongCompactLeaf prepared = prepareCompactLeaf(firstKey, level2, compact, absent);
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
        LongCompactLeaf recorded = prevLevel2.compactAt(slot);
        if (recorded != null && recorded.contains(offset)) {
            return;
        }
        final long value = getValue(level2, block2, offset);
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
        final LongCompactLeaf alreadyRecorded = prevLevel2.compactAt(slot);
        final int absent = alreadyRecorded == null ? count : alreadyRecorded.countAbsent(offsets, count);
        if (absent == 0) {
            return;
        }
        final LongCompactLeaf recorded = preparePrevLeaf(key, prevLevel2, block2, alreadyRecorded, absent, level2);
        if (recorded == null) {
            return;
        }
        final int leafSlot = level2 == null ? -1 : level2.slot(block2);
        final long[] block = level2 == null ? null : level2.blockAt(leafSlot);
        final LongCompactLeaf compact = level2 == null || block != null ? null : level2.compactAt(leafSlot);
        int next = 0;
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            if (absent == count || !recorded.contains(offset)) {
                scratch.offsets[next] = offset;
                if (block != null) {
                    scratch.values[next] = block[offset];
                } else {
                    scratch.values[next] = compact == null ? NULL_LONG : compact.get(offset);
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
    private LongCompactLeaf preparePrevLeaf(
            final long key,
            final Level2 prevLevel2,
            final int block2,
            final LongCompactLeaf recorded,
            final int count,
            final Level2 level2) {
        if (recorded == null) {
            synchronized (prevLeafKeys) {
                prevLeafKeys.add(key >> LOG_BLOCK_SIZE);
            }
        }
        final int newSize = (recorded == null ? 0 : recorded.size()) + count;
        if (newSize > recordThreshold) {
            // the offsets not yet recorded have not changed this cycle, so their current values are previous values
            final long[] snapshot = snapshotRecycler.borrowItem();
            final long[] block = level2 == null ? null : level2.getBlock(block2);
            if (block != null) {
                System.arraycopy(block, 0, snapshot, 0, BLOCK_SIZE);
            } else {
                Arrays.fill(snapshot, NULL_LONG);
                final LongCompactLeaf compact = level2 == null ? null : level2.getCompact(block2);
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
            final LongCompactLeaf newRecorded = newCompactLeaf(count);
            setCompact(true, key, prevLevel2, newRecorded);
            return newRecorded;
        }
        return maybeLeaveSortedTier(true, key, prevLevel2, recorded, newSize);
    }

    /**
     * @return the current value at {@code offset} of the leaf at {@code block2} of {@code level2}, which may be null
     */
    private static long getValue(final Level2 level2, final int block2, final int offset) {
        if (level2 == null) {
            return NULL_LONG;
        }
        final int slot = level2.slot(block2);
        final long[] block = level2.blockAt(slot);
        if (block != null) {
            return block[offset];
        }
        final LongCompactLeaf compact = level2.compactAt(slot);
        return compact == null ? NULL_LONG : compact.get(offset);
    }

    /**
     * @return the value of {@code rowKey} at the start of this cycle
     */
    private long getPrevValue(final long rowKey) {
        final Level0 localPrevRoot = prevRoot;
        if (localPrevRoot != null && rowKey >= 0) {
            final Level2 prevLevel2 = getLevel2(localPrevRoot, rowKey);
            if (prevLevel2 != null) {
                final int slot = prevLevel2.slot(block2(rowKey));
                final int offset = (int) (rowKey & INDEX_MASK);
                final long[] snapshot = prevLevel2.blockAt(slot);
                if (snapshot != null) {
                    return snapshot[offset];
                }
                final LongCompactLeaf recorded = prevLevel2.compactAt(slot);
                if (recorded != null && recorded.contains(offset)) {
                    return recorded.get(offset);
                }
            }
        }
        return getLong(rowKey);
    }

    // region allocateNullFilledBlock
    private static long[] newBlock() {
        final long[] block = new long[BLOCK_SIZE];
        Arrays.fill(block, NULL_LONG);
        return block;
    }
    // endregion allocateNullFilledBlock

    private static long[] toBlock(final LongCompactLeaf compact) {
        final long[] block = newBlock();
        compact.copyTo(block);
        return block;
    }

    // region primitive get
    @Override
    public final long getLong(final long rowKey) {
        if (rowKey < 0) {
            return NULL_LONG;
        }
        final Level2 level2 = getLevel2(rowKey);
        if (level2 == null) {
            return NULL_LONG;
        }
        final int slot = level2.slot(block2(rowKey));
        final int offset = (int) (rowKey & INDEX_MASK);
        final long[] block = level2.blockAt(slot);
        if (block != null) {
            return block[offset];
        }
        final LongCompactLeaf compact = level2.compactAt(slot);
        return compact == null ? NULL_LONG : compact.get(offset);
    }

    @Override
    public final long getPrevLong(final long rowKey) {
        return getPrevValue(rowKey);
    }
    // endregion primitive get

    @Override
    public void startTrackingPrevValues() {
        if (prevFlusher != null) {
            throw new IllegalStateException(
                    "Can't call startTrackingPrevValues() twice: " + getClass().getCanonicalName());
        }
        prevFlusher = new UpdateCommitter<>(this, updateGraph, LongSparseArraySource::commitUpdates);
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
                root = new Level0(1);
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
        final int position = root.find(block0(level1Key << BLOCK0_SHIFT));
        if (position >= 0) {
            root.children[position] = root.children[position].clear(indices, count);
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
     * Clear each entry this cycle set in {@code prev}, the previous value page table, returning the snapshot blocks and
     * the arrays of entries to their recyclers.
     */
    private void recyclePrevious(final Level0 prev) {
        synchronized (prevLeafKeys) {
            for (int ii = 0; ii < prevLeafKeys.size(); ++ii) {
                final long key = prevLeafKeys.getLong(ii) << LOG_BLOCK_SIZE;
                final Level2 level2 = getLevel2(prev, key);
                final int block2 = block2(key);
                final long[] snapshot = level2.getBlock(block2);
                if (snapshot != null) {
                    snapshotRecycler.returnItem(snapshot);
                }
                level2.clearPrevious(block2);
            }
            prevLeafKeys.clear();

            // each level1 was created with a level2, so the sorted level2 keys visit every level1, one at a time
            prevLevel2Keys.sort(null);
            long lastLevel1Key = -1;
            Level1 level1 = null;
            for (int ii = 0; ii < prevLevel2Keys.size(); ++ii) {
                final long level2Key = prevLevel2Keys.getLong(ii);
                final long key = level2Key << BLOCK1_SHIFT;
                final long level1Key = level2Key >> LOG_BLOCK1_SIZE;
                if (level1Key != lastLevel1Key) {
                    if (level1 != null) {
                        level1.recycleArray();
                    }
                    level1 = prev.get(block0(key));
                    lastLevel1Key = level1Key;
                }
                final int block1 = block1(key);
                level1.get(block1).recycleArrays();
                level1.clearPrevious(block1);
            }
            if (level1 != null) {
                level1.recycleArray();
            }
            prevLevel2Keys.clear();
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
    public void set(final long key, final Long value) {
        set(key, unbox(value));
    }
    // endregion boxed methods

    @Override
    public final void set(final long key, final long value) {
        final Level2 level2 = ensureLevel2(key);
        final int block2 = block2(key);
        final int offset = (int) (key & INDEX_MASK);
        if (shouldTrackPrevious()) {
            recordPrevious(key, level2, block2, offset);
        }
        final long[] block = level2.getBlock(block2);
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
    private Level2 setInCompactLeaf(final long key, final Level2 level2, final int offset, final long value) {
        final int block2 = block2(key);
        LongCompactLeaf compact = level2.getCompact(block2);
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
        final long[] block = level2.getBlock(block2);
        if (block != null) {
            if (block[offset] != NULL_LONG) {
                if (shouldTrackPrevious()) {
                    recordPrevious(key, level2, block2, offset);
                }
                block[offset] = NULL_LONG;
            }
            return;
        }
        final LongCompactLeaf compact = level2.getCompact(block2);
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
                    final long[] block = level2.getBlock(block2);
                    if (block != null) {
                        if (trackPrevious) {
                            // the offsets that already hold null are unchanged, so need no previous value
                            int count = 0;
                            for (int offset = firstOffset; offset <= lastOffset; ++offset) {
                                if (block[offset] != NULL_LONG) {
                                    offsets[count++] = offset;
                                }
                            }
                            if (count > 0) {
                                recordPrevious(key, level2, block2, offsets, count, scratch);
                            }
                        }
                        Arrays.fill(block, firstOffset, lastOffset + 1, NULL_LONG);
                    } else {
                        final LongCompactLeaf compact = level2.getCompact(block2);
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
                set(key + shiftDelta, getLong(key));
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
        private long[] block;
        private LongCompactLeaf compact;
        private long[] prevSnapshot;
        private LongCompactLeaf prevRecorded;
        private int destOffset;

        /**
         * @param prevRoot the previous values to read, or null to read current values
         */
        private FillState(final Level0 prevRoot) {
            this.prevRoot = prevRoot;
        }

        private void readLeaf(final LongSparseArraySource source, final long key) {
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

        private long get(final int offset) {
            if (block != null) {
                return block[offset];
            }
            return compact == null ? NULL_LONG : compact.get(offset);
        }

        private long getPrev(final int offset) {
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
                final WritableLongChunk<? super Values> chunk,
                final LongChunk<OrderedRowKeys> keys,
                final int start,
                final int end) {
            final long[] values = prevSnapshot != null ? prevSnapshot : block;
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
        private void fillRange(final WritableLongChunk<? super Values> chunk, final int firstOffset, final int length) {
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
        fill(dest.asWritableLongChunk(), rowSequence, null);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        fill(dest.asWritableLongChunk(), rowSequence, prevRoot);
    }

    /**
     * Fill {@code chunk} with the values of {@code rowSequence}, previous values if {@code prevRoot} is not null.
     */
    private void fill(
            final WritableLongChunk<? super Values> chunk,
            final RowSequence rowSequence,
            final Level0 prevRoot) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillByKeys(chunk, rowSequence, prevRoot);
        } else {
            fillByRanges(chunk, rowSequence, prevRoot);
        }
    }

    private void fillByRanges(
            final WritableLongChunk<? super Values> chunk,
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
            final WritableLongChunk<? super Values> chunk,
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
    public LongChunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return LongChunk.getEmptyChunk();
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
        return getChunkByFilling(context, rowSequence).asLongChunk();
    }

    @Override
    public LongChunk<Values> getPrevChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (prevRoot == null) {
            return getChunk(context, rowSequence);
        }
        return getPrevChunkByFilling(context, rowSequence).asLongChunk();
    }
    // endregion getChunk

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillUnordered(dest.asWritableLongChunk(), keys, null);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillUnordered(dest.asWritableLongChunk(), keys, prevRoot);
    }

    /**
     * Fill {@code chunk} with the values of {@code keys}, previous values if {@code prevRoot} is not null.
     */
    private void fillUnordered(
            final WritableLongChunk<? super Values> chunk,
            final LongChunk<? extends RowKeys> keys,
            final Level0 prevRoot) {
        final FillState state = new FillState(prevRoot);
        long currentLeafKey = -1;
        final int size = keys.size();
        for (int ki = 0; ki < size; ++ki) {
            final long key = keys.get(ki);
            if (key == RowSequence.NULL_ROW_KEY) {
                chunk.set(ki, NULL_LONG);
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
        final LongChunk<? extends Values> values = src.asLongChunk();
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
            long[] block = level2.blockAt(slot);
            final LongCompactLeaf existing = block == null ? level2.compactAt(slot) : null;
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
                final LongCompactLeaf compact = prepareCompactLeaf(firstKey, level2, existing, absent);
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
        final LongChunk<? extends Values> values = src.asLongChunk();
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
                final long[] block = level2.getBlock(block2);
                if (block != null && values.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
            }
            final int offset = (int) (key & INDEX_MASK);
            if (trackPrevious) {
                recordPrevious(key, level2, block2, offset);
            }
            final long[] block = level2.getBlock(block2);
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
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || alternateDataType == Instant.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateDataType == this.getType()) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;
        } else if (alternateDataType == Instant.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toInstant();
        }

        throw new IllegalArgumentException("Cannot reinterpret `" + getType().getName() + "` to `" + alternateDataType.getName() + "`");
    }

    @Override
    public boolean supportsTimeConversion() {
        return true;
    }

    @Override
    public ColumnSource<ZonedDateTime> toZonedDateTime(@NotNull final ZoneId zone) {
        return new ZonedDateTimeSparseArraySource(Require.neqNull(zone, "zone"), this);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(@NotNull final ZoneId zone) {
        return new LongAsLocalDateColumnSource(this, zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(@NotNull final ZoneId zone) {
        return new LongAsLocalTimeColumnSource(this, zone);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new InstantSparseArraySource(this);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return this;
    }
    // endregion reinterpretation
}
