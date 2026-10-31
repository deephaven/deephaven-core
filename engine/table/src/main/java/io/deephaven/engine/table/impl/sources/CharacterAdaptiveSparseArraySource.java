//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftCallback;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.WritableSourceWithPrepareForParallelPopulation;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.sparse.CharBitmapLeaf;
import io.deephaven.engine.table.impl.sources.sparse.CharCompactLeaf;
import io.deephaven.engine.table.impl.sources.sparse.CharSortedLeaf;
import io.deephaven.engine.updategraph.UpdateCommitter;
import io.deephaven.util.annotations.TestUseOnly;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
// region boxing imports
import static io.deephaven.util.type.TypeUtils.unbox;
// endregion boxing imports

/**
 * A sparse in-memory column source of chars whose leaves adapt to the number of values they hold.
 * <p>
 * Row keys are addressed as in {@link CharacterSparseArraySource}: three page table levels above leaves that each cover
 * {@link io.deephaven.engine.table.impl.sources.sparse.SparseConstants#BLOCK_SIZE} consecutive row keys. A leaf is one
 * of:
 * <ul>
 * <li>a {@link CharSortedLeaf}, for at most {@link #DEFAULT_SORTED_THRESHOLD} values</li>
 * <li>a {@link CharBitmapLeaf}, for at most {@link #DEFAULT_COMPACT_THRESHOLD} values</li>
 * <li>a full block of {@code BLOCK_SIZE} values</li>
 * </ul>
 * A leaf takes the next representation when a write would take it past its threshold, and never returns to a smaller
 * one.
 * <p>
 * When tracking previous values, the first write to a row in a cycle records the row's value in a second page table of
 * previous values. Each leaf of that table holds the recorded offsets of its leaf as a sorted or bitmap leaf, following
 * the same thresholds, or, once more offsets are recorded than a compact leaf holds, a full block that snapshots the
 * whole leaf. A row without a recorded value has not changed this cycle, so its previous value is its current value.
 * The previous values are discarded when the cycle completes.
 * <p>
 * For parallel population, {@link #prepareForParallelPopulation(RowSequence, boolean)} allocates the page table above
 * each leaf to be written, for current and previous values, so that writers of different leaves change no shared
 * structure. When the writers' divisions are aligned to leaves, each writer builds and records its own leaves.
 * Otherwise it also builds each leaf with every offset to be written, holding null, and records the previous values, so
 * that writers only store values.
 */
public class CharacterAdaptiveSparseArraySource extends AbstractColumnSource<Character>
        implements MutableColumnSourceGetDefaults.ForChar /* MIXIN_IMPLS */, WritableColumnSource<Character>,
        FillUnordered<Values>, InMemoryColumnSource, PossiblyImmutableColumnSource, RowSetShiftCallback,
        SparseBlockColumnSource, WritableSourceWithPrepareForParallelPopulation {

    /** The largest number of values a sorted leaf holds. */
    public static final int DEFAULT_SORTED_THRESHOLD = 16;
    /** The largest number of values a sorted or bitmap leaf holds. */
    public static final int DEFAULT_COMPACT_THRESHOLD = 128;

    private final int sortedThreshold;
    private final int compactThreshold;
    private final Level0 root = new Level0();

    /**
     * Commits the previous values at the end of each cycle in which a value changed; null when not tracking previous
     * values.
     */
    private transient UpdateCommitter<CharacterAdaptiveSparseArraySource> prevFlusher;
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
    public CharacterAdaptiveSparseArraySource() {
        this(DEFAULT_SORTED_THRESHOLD, DEFAULT_COMPACT_THRESHOLD);
    }

    /**
     * @param sortedThreshold the largest number of values a sorted leaf holds
     * @param compactThreshold the largest number of values a sorted or bitmap leaf holds, from 1 to the block size
     */
    CharacterAdaptiveSparseArraySource(final int sortedThreshold, final int compactThreshold) {
        super(char.class);
        this.sortedThreshold = checkSortedThreshold(sortedThreshold, compactThreshold);
        this.compactThreshold = compactThreshold;
    }
    // endregion constructor

    static int checkSortedThreshold(final int sortedThreshold, final int compactThreshold) {
        Require.inRange(compactThreshold, "compactThreshold", BLOCK_SIZE + 1, "BLOCK_SIZE + 1");
        Require.gtZero(compactThreshold, "compactThreshold");
        Require.gtZero(sortedThreshold, "sortedThreshold");
        return sortedThreshold;
    }

    // region page table
    // Like the levels of CharacterSparseArraySource, each page table level holds its first child without allocating an
    // array of children.
    private static final int NO_CHILD = -1;
    private static final int MANY_CHILDREN = -2;

    /** The top page table level, holding {@link Level1} children. */
    private static final class Level0 {
        private int oneIndex = NO_CHILD;
        private Level1 oneChild;
        private Level1[] children;

        private Level1 get(final int index) {
            if (oneIndex == index) {
                return oneChild;
            }
            return children == null ? null : children[index];
        }

        private void set(final int index, final Level1 child) {
            if (oneIndex == index || oneIndex == NO_CHILD) {
                oneIndex = index;
                oneChild = child;
                return;
            }
            if (children == null) {
                children = new Level1[BLOCK0_SIZE];
                children[oneIndex] = oneChild;
                oneChild = null;
                oneIndex = MANY_CHILDREN;
            }
            children[index] = child;
        }

        private void clear() {
            oneIndex = NO_CHILD;
            oneChild = null;
            children = null;
        }

        private long estimateSize() {
            if (children == null) {
                return oneChild == null ? 0 : oneChild.estimateSize();
            }
            long size = (long) children.length * Long.BYTES;
            for (final Level1 child : children) {
                if (child != null) {
                    size += child.estimateSize();
                }
            }
            return size;
        }
    }

    /** The middle page table level, holding {@link Level2} children. */
    private static final class Level1 {
        private int oneIndex = NO_CHILD;
        private Level2 oneChild;
        private Level2[] children;

        private Level2 get(final int index) {
            if (oneIndex == index) {
                return oneChild;
            }
            return children == null ? null : children[index];
        }

        private void set(final int index, final Level2 child) {
            if (oneIndex == index || oneIndex == NO_CHILD) {
                oneIndex = index;
                oneChild = child;
                return;
            }
            if (children == null) {
                children = new Level2[BLOCK1_SIZE];
                children[oneIndex] = oneChild;
                oneChild = null;
                oneIndex = MANY_CHILDREN;
            }
            children[index] = child;
        }

        private long estimateSize() {
            if (children == null) {
                return oneChild == null ? 0 : oneChild.estimateSize();
            }
            long size = (long) children.length * Long.BYTES;
            for (final Level2 child : children) {
                if (child != null) {
                    size += child.estimateSize();
                }
            }
            return size;
        }
    }

    /**
     * The bottom page table level, holding leaves. A leaf is either a full block or a compact leaf, kept in separate
     * arrays so that reading a full block takes no more than reading a {@link CharacterSparseArraySource} block.
     */
    private static final class Level2 {
        private int oneIndex = NO_CHILD;
        private char[] oneBlock;
        private CharCompactLeaf oneCompact;
        private char[][] blocks;
        private CharCompactLeaf[] compacts;

        private char[] getBlock(final int index) {
            if (oneIndex == index) {
                return oneBlock;
            }
            return blocks == null ? null : blocks[index];
        }

        private CharCompactLeaf getCompact(final int index) {
            if (oneIndex == index) {
                return oneCompact;
            }
            return compacts == null ? null : compacts[index];
        }

        /**
         * Make {@code index} addressable, moving the single leaf into the arrays when {@code index} is a second one.
         */
        private void ensureIndex(final int index) {
            if (oneIndex == index || oneIndex == MANY_CHILDREN) {
                return;
            }
            if (oneIndex == NO_CHILD) {
                oneIndex = index;
                return;
            }
            if (oneBlock != null) {
                blocks = new char[BLOCK2_SIZE][];
                blocks[oneIndex] = oneBlock;
            } else if (oneCompact != null) {
                compacts = new CharCompactLeaf[BLOCK2_SIZE];
                compacts[oneIndex] = oneCompact;
            }
            oneBlock = null;
            oneCompact = null;
            oneIndex = MANY_CHILDREN;
        }

        /** Make {@code block} the leaf at {@code index}, replacing any compact leaf there. */
        private void setBlock(final int index, final char[] block) {
            ensureIndex(index);
            if (oneIndex == index) {
                oneBlock = block;
                oneCompact = null;
                return;
            }
            if (blocks == null) {
                blocks = new char[BLOCK2_SIZE][];
            }
            blocks[index] = block;
            if (compacts != null) {
                compacts[index] = null;
            }
        }

        /** Make {@code compact} the leaf at {@code index}, which must not hold a full block. */
        private void setCompact(final int index, final CharCompactLeaf compact) {
            ensureIndex(index);
            if (oneIndex == index) {
                oneCompact = compact;
                return;
            }
            if (compacts == null) {
                compacts = new CharCompactLeaf[BLOCK2_SIZE];
            }
            compacts[index] = compact;
        }

        /**
         * Allocate what writing the leaf at {@code index} may need, so that making it a block or a compact leaf writes
         * only its own slot.
         *
         * @param mayBeBlock whether the leaf may become a full block
         * @param mayBeCompact whether the leaf may become a compact leaf
         */
        private void prepareIndex(final int index, final boolean mayBeBlock, final boolean mayBeCompact) {
            ensureIndex(index);
            if (oneIndex != MANY_CHILDREN) {
                return;
            }
            if (mayBeBlock && blocks == null) {
                blocks = new char[BLOCK2_SIZE][];
            }
            if (mayBeCompact && compacts == null) {
                compacts = new CharCompactLeaf[BLOCK2_SIZE];
            }
        }

        /** Release the leaf at {@code index}, if any. */
        private void clear(final int index) {
            if (oneIndex == index) {
                oneBlock = null;
                oneCompact = null;
                return;
            }
            if (oneIndex != MANY_CHILDREN) {
                return;
            }
            if (blocks != null) {
                blocks[index] = null;
            }
            if (compacts != null) {
                compacts[index] = null;
            }
        }

        private long estimateSize() {
            if (oneIndex != MANY_CHILDREN) {
                if (oneBlock != null) {
                    return (long) BLOCK_SIZE * Character.BYTES;
                }
                return oneCompact == null ? 0 : oneCompact.estimateSize();
            }
            long size = 0;
            if (blocks != null) {
                size += (long) blocks.length * Long.BYTES;
                for (final char[] block : blocks) {
                    if (block != null) {
                        size += (long) BLOCK_SIZE * Character.BYTES;
                    }
                }
            }
            if (compacts != null) {
                size += (long) compacts.length * Long.BYTES;
                for (final CharCompactLeaf compact : compacts) {
                    if (compact != null) {
                        size += compact.estimateSize();
                    }
                }
            }
            return size;
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
        return ensureLevel2(root, key);
    }

    /**
     * @return the bottom page table level of {@code root} covering {@code key}, allocating the levels as needed
     */
    private static Level2 ensureLevel2(final Level0 root, final long key) {
        final int block0 = block0(key);
        Level1 level1 = root.get(block0);
        if (level1 == null) {
            root.set(block0, level1 = new Level1());
        }
        final int block1 = block1(key);
        Level2 level2 = level1.get(block1);
        if (level2 == null) {
            level1.set(block1, level2 = new Level2());
        }
        return level2;
    }
    // endregion page table

    /**
     * @return an empty compact leaf with room for {@code capacity} values
     */
    private CharCompactLeaf newCompactLeaf(final int capacity) {
        return capacity <= sortedThreshold ? new CharSortedLeaf(capacity) : new CharBitmapLeaf(capacity);
    }

    /**
     * Make the leaf at {@code block2} of {@code level2}, whose current compact leaf is {@code compact} (or null if the
     * leaf is absent), ready for {@code count} more offsets.
     *
     * @return the compact leaf to write, or null if the leaf is now a full block
     */
    private CharCompactLeaf prepareCompactLeaf(
            final Level2 level2,
            final int block2,
            final CharCompactLeaf compact,
            final int count) {
        if (compact == null) {
            if (count > compactThreshold) {
                level2.setBlock(block2, newBlock());
                return null;
            }
            final CharCompactLeaf newCompact = newCompactLeaf(count);
            level2.setCompact(block2, newCompact);
            return newCompact;
        }
        final int newSize = compact.size() + count;
        if (newSize > compactThreshold) {
            level2.setBlock(block2, toBlock(compact));
            return null;
        }
        return maybeLeaveSortedTier(level2, block2, compact, newSize);
    }

    /**
     * Store the offsets within their leaf of {@code leafKeys}, which are all within one leaf, in {@code offsets}.
     */
    private static void leafOffsets(final RowSequence leafKeys, final int[] offsets, final MutableInt offsetCount) {
        offsetCount.set(0);
        leafKeys.forAllRowKeyRanges((final long rangeFirst, final long rangeLast) -> {
            int next = offsetCount.get();
            final int lastOffset = (int) (rangeLast & INDEX_MASK);
            for (int offset = (int) (rangeFirst & INDEX_MASK); offset <= lastOffset; ++offset) {
                offsets[next++] = offset;
            }
            offsetCount.set(next);
        });
    }

    /**
     * @return {@code compact}, or a bitmap leaf with its values that replaces it in {@code level2} if it is a sorted
     *         leaf and {@code newSize} is more than a sorted leaf holds
     */
    private CharCompactLeaf maybeLeaveSortedTier(
            final Level2 level2,
            final int block2,
            final CharCompactLeaf compact,
            final int newSize) {
        if (newSize <= sortedThreshold || !(compact instanceof CharSortedLeaf)) {
            return compact;
        }
        final CharBitmapLeaf bitmap = new CharBitmapLeaf(newSize);
        ((CharSortedLeaf) compact).copyTo(bitmap);
        level2.setCompact(block2, bitmap);
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
        final long currentStep = updateGraph.clock().currentStep();
        if (prepareClockStep == currentStep) {
            throw new IllegalStateException("May not call prepareForParallelPopulation twice on one clock cycle!");
        }
        prepareClockStep = currentStep;
        preparedLeaves = !alignedDivisions;
        if (changedRows.isEmpty()) {
            return;
        }
        final boolean trackPrevious = prevFlusher != null;
        final int[] offsets = alignedDivisions ? null : new int[BLOCK_SIZE];
        final MutableInt offsetCount = new MutableInt();
        try (final RowSequence.Iterator it = changedRows.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final long firstKey = it.peekNextKey();
                final RowSequence leafKeys = it.getNextRowSequenceThrough(firstKey | INDEX_MASK);
                final int count = leafKeys.intSize();
                final int block2 = block2(firstKey);
                final Level2 level2 = ensureLevel2(firstKey);
                final CharCompactLeaf compact = level2.getCompact(block2);
                final boolean isBlock = level2.getBlock(block2) != null;
                final boolean mayBeBlock =
                        !isBlock && (compact == null ? 0 : compact.size()) + count > compactThreshold;
                level2.prepareIndex(block2, mayBeBlock, !isBlock && !mayBeBlock);
                if (trackPrevious) {
                    final Level2 prevLevel2 = ensurePrevLevel2(firstKey);
                    final CharCompactLeaf recorded = prevLevel2.getCompact(block2);
                    final boolean mayBeSnapshot = prevLevel2.getBlock(block2) == null
                            && (recorded == null ? 0 : recorded.size()) + count > compactThreshold;
                    prevLevel2.prepareIndex(block2, mayBeSnapshot, true);
                }
                if (alignedDivisions) {
                    continue;
                }
                leafOffsets(leafKeys, offsets, offsetCount);
                if (trackPrevious) {
                    recordPrevious(firstKey, level2, block2, offsets, count);
                }
                if (!isBlock) {
                    final int absent = compact == null ? count : compact.countAbsent(offsets, count);
                    final CharCompactLeaf prepared = prepareCompactLeaf(level2, block2, compact, absent);
                    if (prepared != null) {
                        prepared.insertAbsent(offsets, count);
                    }
                }
            }
        }
    }

    /**
     * @return the bottom level of the previous value page table covering {@code key}, allocating as needed
     */
    private Level2 ensurePrevLevel2(final long key) {
        if (prevRoot == null) {
            prevRoot = new Level0();
            prevFlusher.maybeActivate();
        }
        return ensureLevel2(prevRoot, key);
    }

    /**
     * Record the current value at {@code offset} of the leaf at {@code block2} of {@code level2} (null if not
     * allocated), which covers {@code key}, as its previous value, unless this cycle has already recorded it.
     */
    private void recordPrevious(final long key, final Level2 level2, final int block2, final int offset) {
        final Level2 prevLevel2 = ensurePrevLevel2(key);
        if (prevLevel2.getBlock(block2) != null) {
            return;
        }
        CharCompactLeaf recorded = prevLevel2.getCompact(block2);
        if (recorded != null && recorded.contains(offset)) {
            return;
        }
        final char value = getValue(level2, block2, offset);
        recorded = preparePrevLeaf(prevLevel2, block2, recorded, 1, level2);
        if (recorded != null) {
            recorded.put(offset, value);
        }
    }

    /**
     * Record the current values at {@code offsets[0, count)}, ascending and distinct, of the leaf at {@code block2} of
     * {@code level2} (null if not allocated), which covers {@code key}, as their previous values, except those this
     * cycle has already recorded.
     */
    private void recordPrevious(
            final long key,
            final Level2 level2,
            final int block2,
            final int[] offsets,
            final int count) {
        final Level2 prevLevel2 = ensurePrevLevel2(key);
        if (prevLevel2.getBlock(block2) != null) {
            return;
        }
        CharCompactLeaf recorded = prevLevel2.getCompact(block2);
        final int absent = recorded == null ? count : recorded.countAbsent(offsets, count);
        if (absent == 0) {
            return;
        }
        recorded = preparePrevLeaf(prevLevel2, block2, recorded, absent, level2);
        if (recorded == null) {
            return;
        }
        for (int ii = 0; ii < count; ++ii) {
            final int offset = offsets[ii];
            if (!recorded.contains(offset)) {
                recorded.put(offset, getValue(level2, block2, offset));
            }
        }
    }

    /**
     * Make the previous value leaf at {@code block2} of {@code prevLevel2}, whose recorded offsets are {@code recorded}
     * (or null if none), ready to record {@code count} more offsets of the leaf at {@code block2} of {@code level2}
     * (null if not allocated).
     *
     * @return the leaf to record into, or null if the previous value leaf is now a snapshot of the whole leaf
     */
    private CharCompactLeaf preparePrevLeaf(
            final Level2 prevLevel2,
            final int block2,
            final CharCompactLeaf recorded,
            final int count,
            final Level2 level2) {
        final int newSize = (recorded == null ? 0 : recorded.size()) + count;
        if (newSize > compactThreshold) {
            // the offsets not yet recorded have not changed this cycle, so their current values are previous values
            final char[] snapshot = newBlock();
            if (level2 != null) {
                final char[] block = level2.getBlock(block2);
                if (block != null) {
                    System.arraycopy(block, 0, snapshot, 0, BLOCK_SIZE);
                } else {
                    final CharCompactLeaf compact = level2.getCompact(block2);
                    if (compact != null) {
                        compact.copyTo(snapshot);
                    }
                }
            }
            if (recorded != null) {
                recorded.copyTo(snapshot);
            }
            prevLevel2.setBlock(block2, snapshot);
            return null;
        }
        if (recorded == null) {
            final CharCompactLeaf newRecorded = newCompactLeaf(count);
            prevLevel2.setCompact(block2, newRecorded);
            return newRecorded;
        }
        return maybeLeaveSortedTier(prevLevel2, block2, recorded, newSize);
    }

    /**
     * @return the current value at {@code offset} of the leaf at {@code block2} of {@code level2}, which may be null
     */
    private static char getValue(final Level2 level2, final int block2, final int offset) {
        if (level2 == null) {
            return NULL_CHAR;
        }
        final char[] block = level2.getBlock(block2);
        if (block != null) {
            return block[offset];
        }
        final CharCompactLeaf compact = level2.getCompact(block2);
        return compact == null ? NULL_CHAR : compact.get(offset);
    }

    /**
     * @return the value of {@code rowKey} at the start of this cycle
     */
    private char getPrevValue(final long rowKey) {
        final Level0 localPrevRoot = prevRoot;
        if (localPrevRoot != null && rowKey >= 0) {
            final Level2 prevLevel2 = getLevel2(localPrevRoot, rowKey);
            if (prevLevel2 != null) {
                final int block2 = block2(rowKey);
                final int offset = (int) (rowKey & INDEX_MASK);
                final char[] snapshot = prevLevel2.getBlock(block2);
                if (snapshot != null) {
                    return snapshot[offset];
                }
                final CharCompactLeaf recorded = prevLevel2.getCompact(block2);
                if (recorded != null && recorded.contains(offset)) {
                    return recorded.get(offset);
                }
            }
        }
        return getChar(rowKey);
    }

    // region allocateNullFilledBlock
    private static char[] newBlock() {
        final char[] block = new char[BLOCK_SIZE];
        Arrays.fill(block, NULL_CHAR);
        return block;
    }
    // endregion allocateNullFilledBlock

    private static char[] toBlock(final CharCompactLeaf compact) {
        final char[] block = newBlock();
        compact.copyTo(block);
        return block;
    }

    // region primitive get
    @Override
    public final char getChar(final long rowKey) {
        if (rowKey < 0) {
            return NULL_CHAR;
        }
        final Level2 level2 = getLevel2(rowKey);
        if (level2 == null) {
            return NULL_CHAR;
        }
        final int block2 = block2(rowKey);
        final int offset = (int) (rowKey & INDEX_MASK);
        final char[] block = level2.getBlock(block2);
        if (block != null) {
            return block[offset];
        }
        final CharCompactLeaf compact = level2.getCompact(block2);
        return compact == null ? NULL_CHAR : compact.get(offset);
    }

    @Override
    public final char getPrevChar(final long rowKey) {
        return getPrevValue(rowKey);
    }
    // endregion primitive get

    @Override
    public void startTrackingPrevValues() {
        if (prevFlusher != null) {
            throw new IllegalStateException(
                    "Can't call startTrackingPrevValues() twice: " + getClass().getCanonicalName());
        }
        prevFlusher = new UpdateCommitter<>(this, updateGraph, CharacterAdaptiveSparseArraySource::commitUpdates);
    }

    private void commitUpdates() {
        prevRoot = null;
        if (blocksToClear == null) {
            return;
        }
        try (final RowSet ignored1 = blocksToClear;
                final RowSet ignored2 = blocks2ToClear;
                final RowSet ignored3 = blocks1ToClear) {
            if (emptyResult) {
                root.clear();
                return;
            }
            blocksToClear.forAllRowKeys((final long block) -> {
                final Level2 level2 = getLevel2(block << LOG_BLOCK_SIZE);
                if (level2 != null) {
                    level2.clear(block2(block << LOG_BLOCK_SIZE));
                }
            });
            blocks2ToClear.forAllRowKeys((final long blockIndex2) -> {
                final long key = blockIndex2 << BLOCK1_SHIFT;
                final Level1 level1 = root.get(block0(key));
                if (level1 != null) {
                    level1.set(block1(key), null);
                }
            });
            blocks1ToClear
                    .forAllRowKeys((final long blockIndex1) -> root.set(block0(blockIndex1 << BLOCK0_SHIFT), null));
        } finally {
            blocksToClear = null;
            blocks2ToClear = null;
            blocks1ToClear = null;
            emptyResult = false;
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
    public void set(final long key, final Character value) {
        set(key, unbox(value));
    }
    // endregion boxed methods

    @Override
    public final void set(final long key, final char value) {
        final Level2 level2 = ensureLevel2(key);
        final int block2 = block2(key);
        final int offset = (int) (key & INDEX_MASK);
        if (shouldTrackPrevious()) {
            recordPrevious(key, level2, block2, offset);
        }
        final char[] block = level2.getBlock(block2);
        if (block != null) {
            block[offset] = value;
            return;
        }
        setInCompactLeaf(level2, block2, offset, value);
    }

    /**
     * Set the value at {@code offset} of the leaf at {@code block2} of {@code level2}, which is not a full block.
     */
    private void setInCompactLeaf(final Level2 level2, final int block2, final int offset, final char value) {
        CharCompactLeaf compact = level2.getCompact(block2);
        if (compact == null || !compact.contains(offset)) {
            compact = prepareCompactLeaf(level2, block2, compact, 1);
            if (compact == null) {
                level2.getBlock(block2)[offset] = value;
                return;
            }
        }
        compact.put(offset, value);
    }

    @Override
    public void setNull(final long key) {
        final Level2 level2 = getLevel2(key);
        if (level2 == null) {
            return;
        }
        final int block2 = block2(key);
        final int offset = (int) (key & INDEX_MASK);
        final char[] block = level2.getBlock(block2);
        if (block != null) {
            if (block[offset] != NULL_CHAR) {
                if (shouldTrackPrevious()) {
                    recordPrevious(key, level2, block2, offset);
                }
                block[offset] = NULL_CHAR;
            }
            return;
        }
        final CharCompactLeaf compact = level2.getCompact(block2);
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
        rowSequence.forAllRowKeyRanges((final long firstKey, final long lastKey) -> {
            long key = firstKey;
            while (true) {
                final long lastKeyInLeaf = Math.min(lastKey, key | INDEX_MASK);
                final Level2 level2 = getLevel2(key);
                if (level2 != null) {
                    final int block2 = block2(key);
                    final int firstOffset = (int) (key & INDEX_MASK);
                    final int lastOffset = (int) (lastKeyInLeaf & INDEX_MASK);
                    final char[] block = level2.getBlock(block2);
                    if (block != null) {
                        if (trackPrevious) {
                            // the offsets that already hold null are unchanged, so need no previous value
                            int count = 0;
                            for (int offset = firstOffset; offset <= lastOffset; ++offset) {
                                if (block[offset] != NULL_CHAR) {
                                    offsets[count++] = offset;
                                }
                            }
                            if (count > 0) {
                                recordPrevious(key, level2, block2, offsets, count);
                            }
                        }
                        Arrays.fill(block, firstOffset, lastOffset + 1, NULL_CHAR);
                    } else {
                        final CharCompactLeaf compact = level2.getCompact(block2);
                        if (compact != null) {
                            if (trackPrevious) {
                                final int count = compact.copyOffsets(firstOffset, lastOffset, offsets);
                                if (count > 0) {
                                    recordPrevious(key, level2, block2, offsets, count);
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
                set(key + shiftDelta, getChar(key));
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
        private char[] block;
        private CharCompactLeaf compact;
        private char[] prevSnapshot;
        private CharCompactLeaf prevRecorded;
        private int destOffset;

        /**
         * @param prevRoot the previous values to read, or null to read current values
         */
        private FillState(final Level0 prevRoot) {
            this.prevRoot = prevRoot;
        }

        private void readLeaf(final CharacterAdaptiveSparseArraySource source, final long key) {
            maxKeyInCurrentLeaf = key | INDEX_MASK;
            final int block2 = block2(key);
            final Level2 level2 = source.getLevel2(key);
            if (level2 == null) {
                block = null;
                compact = null;
            } else {
                block = level2.getBlock(block2);
                compact = block == null ? level2.getCompact(block2) : null;
            }
            if (prevRoot != null) {
                final Level2 prevLevel2 = getLevel2(prevRoot, key);
                if (prevLevel2 == null) {
                    prevSnapshot = null;
                    prevRecorded = null;
                } else {
                    prevSnapshot = prevLevel2.getBlock(block2);
                    prevRecorded = prevSnapshot == null ? prevLevel2.getCompact(block2) : null;
                }
            }
        }

        private char get(final int offset) {
            if (block != null) {
                return block[offset];
            }
            return compact == null ? NULL_CHAR : compact.get(offset);
        }

        private char getPrev(final int offset) {
            if (prevSnapshot != null) {
                return prevSnapshot[offset];
            }
            if (prevRecorded != null && prevRecorded.contains(offset)) {
                return prevRecorded.get(offset);
            }
            return get(offset);
        }

        /**
         * Fill {@code chunk} at {@link #destOffset} with the values of offsets {@code firstOffset} through
         * {@code firstOffset + length - 1}.
         */
        private void fillRange(final WritableCharChunk<? super Values> chunk, final int firstOffset, final int length) {
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
        fill(dest.asWritableCharChunk(), rowSequence, null);
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        fill(dest.asWritableCharChunk(), rowSequence, prevRoot);
    }

    /**
     * Fill {@code chunk} with the values of {@code rowSequence}, previous values if {@code prevRoot} is not null.
     */
    private void fill(
            final WritableCharChunk<? super Values> chunk,
            final RowSequence rowSequence,
            final Level0 prevRoot) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillByKeys(chunk, rowSequence, prevRoot);
        } else {
            fillByRanges(chunk, rowSequence, prevRoot);
        }
    }

    private void fillByRanges(
            final WritableCharChunk<? super Values> chunk,
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

    private void fillByKeys(
            final WritableCharChunk<? super Values> chunk,
            final RowSequence rowSequence,
            final Level0 prevRoot) {
        final FillState state = new FillState(prevRoot);
        if (prevRoot == null) {
            rowSequence.forAllRowKeys((final long key) -> {
                if (key > state.maxKeyInCurrentLeaf) {
                    state.readLeaf(this, key);
                }
                chunk.set(state.destOffset++, state.get((int) (key & INDEX_MASK)));
            });
        } else {
            rowSequence.forAllRowKeys((final long key) -> {
                if (key > state.maxKeyInCurrentLeaf) {
                    state.readLeaf(this, key);
                }
                chunk.set(state.destOffset++, state.getPrev((int) (key & INDEX_MASK)));
            });
        }
        chunk.setSize(state.destOffset);
    }

    // region getChunk
    @Override
    public CharChunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return CharChunk.getEmptyChunk();
        }
        final long firstKey = rowSequence.firstRowKey();
        final long lastKey = rowSequence.lastRowKey();
        if (lastKey - firstKey + 1 == rowSequence.size() && (firstKey >> BLOCK2_SHIFT == lastKey >> BLOCK2_SHIFT)) {
            // a contiguous range within one leaf can alias the leaf when it is a full block, or read as nulls when
            // there is no leaf
            final Level2 level2 = getLevel2(firstKey);
            final int block2 = block2(firstKey);
            if (level2 == null || level2.getCompact(block2) == null) {
                return DefaultGetContext.resetChunkFromArray(context,
                        level2 == null ? null : level2.getBlock(block2),
                        (int) (firstKey & INDEX_MASK),
                        (int) rowSequence.size());
            }
        }
        return getChunkByFilling(context, rowSequence).asCharChunk();
    }

    @Override
    public CharChunk<Values> getPrevChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (prevRoot == null) {
            return getChunk(context, rowSequence);
        }
        return getPrevChunkByFilling(context, rowSequence).asCharChunk();
    }
    // endregion getChunk

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillUnordered(dest.asWritableCharChunk(), keys, null);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillUnordered(dest.asWritableCharChunk(), keys, prevRoot);
    }

    /**
     * Fill {@code chunk} with the values of {@code keys}, previous values if {@code prevRoot} is not null.
     */
    private void fillUnordered(
            final WritableCharChunk<? super Values> chunk,
            final LongChunk<? extends RowKeys> keys,
            final Level0 prevRoot) {
        final FillState state = new FillState(prevRoot);
        long currentLeafKey = -1;
        final int size = keys.size();
        for (int ki = 0; ki < size; ++ki) {
            final long key = keys.get(ki);
            if (key == RowSequence.NULL_ROW_KEY) {
                chunk.set(ki, NULL_CHAR);
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
     * Holds the offsets of the keys that a chunked write puts into one leaf.
     */
    private static final class AdaptiveFillFromContext implements FillFromContext {
        private final int[] offsets = new int[BLOCK_SIZE];
    }

    @Override
    public FillFromContext makeFillFromContext(final int chunkCapacity) {
        return new AdaptiveFillFromContext();
    }

    @Override
    public void fillFromChunk(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return;
        }
        final int[] offsets = ((AdaptiveFillFromContext) context).offsets;
        final CharChunk<? extends Values> values = src.asCharChunk();
        final boolean trackPrevious = shouldTrackPrevious();
        final MutableInt offsetCount = new MutableInt();
        final MutableInt position = new MutableInt();
        int srcOffset = 0;
        try (final RowSequence.Iterator it = rowSequence.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final long firstKey = it.peekNextKey();
                final RowSequence leafKeys = it.getNextRowSequenceThrough(firstKey | INDEX_MASK);
                final int count = leafKeys.intSize();
                final Level2 level2 = ensureLevel2(firstKey);
                final int block2 = block2(firstKey);
                char[] block = level2.getBlock(block2);
                final CharCompactLeaf existing = block == null ? level2.getCompact(block2) : null;
                // a new leaf of more values than a compact leaf holds becomes a full block, which needs no offsets
                if (trackPrevious || (block == null && (existing != null || count <= compactThreshold))) {
                    leafOffsets(leafKeys, offsets, offsetCount);
                }
                if (trackPrevious) {
                    recordPrevious(firstKey, level2, block2, offsets, count);
                }
                if (block == null) {
                    final int absent = existing == null ? count : existing.countAbsent(offsets, count);
                    final CharCompactLeaf compact = prepareCompactLeaf(level2, block2, existing, absent);
                    if (compact != null) {
                        compact.putSorted(offsets, values, srcOffset, count);
                        srcOffset += count;
                        continue;
                    }
                    block = level2.getBlock(block2);
                }
                if (values.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                final char[] destBlock = block;
                position.set(srcOffset);
                leafKeys.forAllRowKeyRanges((final long rangeFirst, final long rangeLast) -> {
                    final int length = (int) (rangeLast - rangeFirst + 1);
                    values.copyToTypedArray(position.getAndAdd(length), destBlock, (int) (rangeFirst & INDEX_MASK),
                            length);
                });
                srcOffset += count;
            }
        }
    }

    @Override
    public void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys) {
        final CharChunk<? extends Values> values = src.asCharChunk();
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
                final char[] block = level2.getBlock(block2);
                if (block != null && values.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
            }
            final int offset = (int) (key & INDEX_MASK);
            if (trackPrevious) {
                recordPrevious(key, level2, block2, offset);
            }
            final char[] block = level2.getBlock(block2);
            if (block != null) {
                block[offset] = values.get(ki);
            } else {
                setInCompactLeaf(level2, block2, offset, values.get(ki));
            }
        }
    }

    @TestUseOnly
    @Override
    public long estimateSize() {
        return root.estimateSize();
    }
}
