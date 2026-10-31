//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterAdaptiveSparseArraySource and run "./gradlew replicateSourcesAndChunks" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetShiftCallback;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.DefaultGetContext;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.table.impl.sources.sparse.DoubleBitmapLeaf;
import io.deephaven.engine.table.impl.sources.sparse.DoubleCompactLeaf;
import io.deephaven.engine.table.impl.sources.sparse.DoubleSortedLeaf;
import io.deephaven.util.annotations.TestUseOnly;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
// region boxing imports
import static io.deephaven.util.type.TypeUtils.unbox;
// endregion boxing imports

/**
 * A sparse in-memory column source of doubles whose leaves adapt to the number of values they hold.
 * <p>
 * Row keys are addressed as in {@link DoubleSparseArraySource}: three page table levels above leaves that each cover
 * {@link io.deephaven.engine.table.impl.sources.sparse.SparseConstants#BLOCK_SIZE} consecutive row keys. A leaf is one
 * of:
 * <ul>
 * <li>a {@link DoubleSortedLeaf}, for at most {@link #DEFAULT_SORTED_THRESHOLD} values</li>
 * <li>a {@link DoubleBitmapLeaf}, for at most {@link #DEFAULT_COMPACT_THRESHOLD} values</li>
 * <li>a full block of {@code BLOCK_SIZE} values</li>
 * </ul>
 * A leaf takes the next representation when a write would take it past its threshold, and never returns to a smaller
 * one.
 */
public class DoubleAdaptiveSparseArraySource extends AbstractColumnSource<Double>
        implements MutableColumnSourceGetDefaults.ForDouble /* MIXIN_IMPLS */, WritableColumnSource<Double>,
        FillUnordered<Values>, InMemoryColumnSource, PossiblyImmutableColumnSource, RowSetShiftCallback,
        SparseBlockColumnSource {

    /** The largest number of values a sorted leaf holds. */
    public static final int DEFAULT_SORTED_THRESHOLD = 16;
    /** The largest number of values a sorted or bitmap leaf holds. */
    public static final int DEFAULT_COMPACT_THRESHOLD = 128;

    private final int sortedThreshold;
    private final int compactThreshold;
    private final Level0 root = new Level0();

    /**
     * Whether the values are known never to change, as for the result of a static select.
     */
    private boolean immutable = false;

    // region constructor
    public DoubleAdaptiveSparseArraySource() {
        this(DEFAULT_SORTED_THRESHOLD, DEFAULT_COMPACT_THRESHOLD);
    }

    /**
     * @param sortedThreshold the largest number of values a sorted leaf holds
     * @param compactThreshold the largest number of values a sorted or bitmap leaf holds, from 1 to the block size
     */
    DoubleAdaptiveSparseArraySource(final int sortedThreshold, final int compactThreshold) {
        super(double.class);
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
    // Like the levels of DoubleSparseArraySource, each page table level holds its first child without allocating an
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

        private long estimateSize() {
            long size = 32;
            if (children == null) {
                return oneChild == null ? size : size + oneChild.estimateSize();
            }
            size += 16 + (long) children.length * Long.BYTES;
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
            long size = 32;
            if (children == null) {
                return oneChild == null ? size : size + oneChild.estimateSize();
            }
            size += 16 + (long) children.length * Long.BYTES;
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
     * arrays so that reading a full block takes no more than reading a {@link DoubleSparseArraySource} block.
     */
    private static final class Level2 {
        private int oneIndex = NO_CHILD;
        private double[] oneBlock;
        private DoubleCompactLeaf oneCompact;
        private double[][] blocks;
        private DoubleCompactLeaf[] compacts;

        private double[] getBlock(final int index) {
            if (oneIndex == index) {
                return oneBlock;
            }
            return blocks == null ? null : blocks[index];
        }

        private DoubleCompactLeaf getCompact(final int index) {
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
                blocks = new double[BLOCK2_SIZE][];
                blocks[oneIndex] = oneBlock;
            } else if (oneCompact != null) {
                compacts = new DoubleCompactLeaf[BLOCK2_SIZE];
                compacts[oneIndex] = oneCompact;
            }
            oneBlock = null;
            oneCompact = null;
            oneIndex = MANY_CHILDREN;
        }

        /** Make {@code block} the leaf at {@code index}, replacing any compact leaf there. */
        private void setBlock(final int index, final double[] block) {
            ensureIndex(index);
            if (oneIndex == index) {
                oneBlock = block;
                oneCompact = null;
                return;
            }
            if (blocks == null) {
                blocks = new double[BLOCK2_SIZE][];
            }
            blocks[index] = block;
            if (compacts != null) {
                compacts[index] = null;
            }
        }

        /** Make {@code compact} the leaf at {@code index}, which must not hold a full block. */
        private void setCompact(final int index, final DoubleCompactLeaf compact) {
            ensureIndex(index);
            if (oneIndex == index) {
                oneCompact = compact;
                return;
            }
            if (compacts == null) {
                compacts = new DoubleCompactLeaf[BLOCK2_SIZE];
            }
            compacts[index] = compact;
        }

        private long estimateSize() {
            long size = 40;
            if (oneIndex != MANY_CHILDREN) {
                if (oneBlock != null) {
                    size += 16 + (long) BLOCK_SIZE * Double.BYTES;
                } else if (oneCompact != null) {
                    size += oneCompact.estimateSize();
                }
                return size;
            }
            if (blocks != null) {
                size += 16 + (long) blocks.length * Long.BYTES;
                for (final double[] block : blocks) {
                    if (block != null) {
                        size += 16 + (long) BLOCK_SIZE * Double.BYTES;
                    }
                }
            }
            if (compacts != null) {
                size += 16 + (long) compacts.length * Long.BYTES;
                for (final DoubleCompactLeaf compact : compacts) {
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
        final Level1 level1 = root.get(block0(key));
        return level1 == null ? null : level1.get(block1(key));
    }

    /**
     * @return the bottom page table level covering {@code key}, allocating the levels as needed
     */
    private Level2 ensureLevel2(final long key) {
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
    private DoubleCompactLeaf newCompactLeaf(final int capacity) {
        return capacity <= sortedThreshold ? new DoubleSortedLeaf(capacity) : new DoubleBitmapLeaf(capacity);
    }

    /**
     * Make the leaf at {@code block2} of {@code level2}, whose current compact leaf is {@code compact} (or null if the
     * leaf is absent), ready for {@code count} more values.
     *
     * @return the compact leaf to write, or null if the leaf is now a full block
     */
    private DoubleCompactLeaf prepareCompactLeaf(
            final Level2 level2,
            final int block2,
            final DoubleCompactLeaf compact,
            final int count) {
        if (compact == null) {
            if (count > compactThreshold) {
                level2.setBlock(block2, newBlock());
                return null;
            }
            final DoubleCompactLeaf newCompact = newCompactLeaf(count);
            level2.setCompact(block2, newCompact);
            return newCompact;
        }
        final int newSize = compact.size() + count;
        if (newSize > compactThreshold) {
            // some of the offsets may already hold values, so this may convert a leaf that would have fit
            level2.setBlock(block2, toBlock(compact));
            return null;
        }
        if (newSize > sortedThreshold && compact instanceof DoubleSortedLeaf) {
            final DoubleBitmapLeaf bitmap = new DoubleBitmapLeaf(newSize);
            ((DoubleSortedLeaf) compact).copyTo(bitmap);
            level2.setCompact(block2, bitmap);
            return bitmap;
        }
        return compact;
    }

    // region allocateNullFilledBlock
    private static double[] newBlock() {
        final double[] block = new double[BLOCK_SIZE];
        Arrays.fill(block, NULL_DOUBLE);
        return block;
    }
    // endregion allocateNullFilledBlock

    private static double[] toBlock(final DoubleCompactLeaf compact) {
        final double[] block = newBlock();
        compact.copyTo(block);
        return block;
    }

    // region primitive get
    @Override
    public final double getDouble(final long rowKey) {
        if (rowKey < 0) {
            return NULL_DOUBLE;
        }
        final Level2 level2 = getLevel2(rowKey);
        if (level2 == null) {
            return NULL_DOUBLE;
        }
        final int block2 = block2(rowKey);
        final int offset = (int) (rowKey & INDEX_MASK);
        final double[] block = level2.getBlock(block2);
        if (block != null) {
            return block[offset];
        }
        final DoubleCompactLeaf compact = level2.getCompact(block2);
        return compact == null ? NULL_DOUBLE : compact.get(offset);
    }

    @Override
    public final double getPrevDouble(final long rowKey) {
        return getDouble(rowKey);
    }
    // endregion primitive get

    @Override
    public void startTrackingPrevValues() {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " does not track previous values");
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
    public void set(final long key, final Double value) {
        set(key, unbox(value));
    }
    // endregion boxed methods

    @Override
    public final void set(final long key, final double value) {
        final Level2 level2 = ensureLevel2(key);
        final int block2 = block2(key);
        final int offset = (int) (key & INDEX_MASK);
        final double[] block = level2.getBlock(block2);
        if (block != null) {
            block[offset] = value;
            return;
        }
        setInCompactLeaf(level2, block2, offset, value);
    }

    /**
     * Set the value at {@code offset} of the leaf at {@code block2} of {@code level2}, which is not a full block.
     */
    private void setInCompactLeaf(final Level2 level2, final int block2, final int offset, final double value) {
        DoubleCompactLeaf compact = level2.getCompact(block2);
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
        final double[] block = level2.getBlock(block2);
        if (block != null) {
            block[offset] = NULL_DOUBLE;
            return;
        }
        final DoubleCompactLeaf compact = level2.getCompact(block2);
        if (compact != null) {
            compact.remove(offset);
        }
    }

    @Override
    public void setNull(@NotNull final RowSequence rowSequence) {
        rowSequence.forAllRowKeyRanges((final long firstKey, final long lastKey) -> {
            long key = firstKey;
            while (true) {
                final long lastKeyInLeaf = Math.min(lastKey, key | INDEX_MASK);
                final Level2 level2 = getLevel2(key);
                if (level2 != null) {
                    final int block2 = block2(key);
                    final int firstOffset = (int) (key & INDEX_MASK);
                    final int lastOffset = (int) (lastKeyInLeaf & INDEX_MASK);
                    final double[] block = level2.getBlock(block2);
                    if (block != null) {
                        Arrays.fill(block, firstOffset, lastOffset + 1, NULL_DOUBLE);
                    } else {
                        final DoubleCompactLeaf compact = level2.getCompact(block2);
                        if (compact != null) {
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
                set(key + shiftDelta, getDouble(key));
                setNull(key);
                return true;
            });
        }
    }

    @Override
    public void clearBlocks(final RowSet blocksToClear, final RowSet removeBlocks2, final RowSet removeBlocks1,
            final boolean empty) {
        // without previous values, nothing is released at the end of a cycle
    }

    /**
     * The leaf being read by a chunked fill, and the position in the destination chunk.
     */
    private static final class FillState {
        private long maxKeyInCurrentLeaf = -1;
        private double[] block;
        private DoubleCompactLeaf compact;
        private int destOffset;

        private void readLeaf(final DoubleAdaptiveSparseArraySource source, final long key) {
            maxKeyInCurrentLeaf = key | INDEX_MASK;
            final Level2 level2 = source.getLevel2(key);
            if (level2 == null) {
                block = null;
                compact = null;
            } else {
                final int block2 = block2(key);
                block = level2.getBlock(block2);
                compact = block == null ? level2.getCompact(block2) : null;
            }
        }

        private double get(final int offset) {
            if (block != null) {
                return block[offset];
            }
            return compact == null ? NULL_DOUBLE : compact.get(offset);
        }
    }

    @Override
    public void fillChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillByKeys(dest.asWritableDoubleChunk(), rowSequence);
        } else {
            fillByRanges(dest.asWritableDoubleChunk(), rowSequence);
        }
    }

    @Override
    public void fillPrevChunk(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final RowSequence rowSequence) {
        fillChunk(context, dest, rowSequence);
    }

    private void fillByRanges(final WritableDoubleChunk<? super Values> chunk, final RowSequence rowSequence) {
        final FillState state = new FillState();
        rowSequence.forAllRowKeyRanges((final long firstKey, final long lastKey) -> {
            long key = firstKey;
            while (true) {
                if (key > state.maxKeyInCurrentLeaf) {
                    state.readLeaf(this, key);
                }
                final long lastKeyInLeaf = Math.min(lastKey, state.maxKeyInCurrentLeaf);
                final int length = (int) (lastKeyInLeaf - key + 1);
                final int firstOffset = (int) (key & INDEX_MASK);
                if (state.block != null) {
                    chunk.copyFromTypedArray(state.block, firstOffset, state.destOffset, length);
                } else if (state.compact != null) {
                    state.compact.fillRange(firstOffset, firstOffset + length - 1, chunk, state.destOffset);
                } else {
                    chunk.fillWithNullValue(state.destOffset, length);
                }
                state.destOffset += length;
                if (lastKeyInLeaf == lastKey) {
                    break;
                }
                key = lastKeyInLeaf + 1;
            }
        });
        chunk.setSize(state.destOffset);
    }

    private void fillByKeys(final WritableDoubleChunk<? super Values> chunk, final RowSequence rowSequence) {
        final FillState state = new FillState();
        rowSequence.forAllRowKeys((final long key) -> {
            if (key > state.maxKeyInCurrentLeaf) {
                state.readLeaf(this, key);
            }
            chunk.set(state.destOffset++, state.get((int) (key & INDEX_MASK)));
        });
        chunk.setSize(state.destOffset);
    }

    // region getChunk
    @Override
    public DoubleChunk<Values> getChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        if (rowSequence.isEmpty()) {
            return DoubleChunk.getEmptyChunk();
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
        return getChunkByFilling(context, rowSequence).asDoubleChunk();
    }

    @Override
    public DoubleChunk<Values> getPrevChunk(@NotNull final GetContext context, @NotNull final RowSequence rowSequence) {
        return getChunk(context, rowSequence);
    }
    // endregion getChunk

    @Override
    public void fillChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        final WritableDoubleChunk<? super Values> chunk = dest.asWritableDoubleChunk();
        final FillState state = new FillState();
        long currentLeafKey = -1;
        final int size = keys.size();
        for (int ki = 0; ki < size; ++ki) {
            final long key = keys.get(ki);
            if (key == RowSequence.NULL_ROW_KEY) {
                chunk.set(ki, NULL_DOUBLE);
                continue;
            }
            final long leafKey = key & ~INDEX_MASK;
            if (leafKey != currentLeafKey) {
                state.readLeaf(this, key);
                currentLeafKey = leafKey;
            }
            chunk.set(ki, state.get((int) (key & INDEX_MASK)));
        }
        chunk.setSize(size);
    }

    @Override
    public void fillPrevChunkUnordered(
            @NotNull final FillContext context,
            @NotNull final WritableChunk<? super Values> dest,
            @NotNull final LongChunk<? extends RowKeys> keys) {
        fillChunkUnordered(context, dest, keys);
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
        final DoubleChunk<? extends Values> values = src.asDoubleChunk();
        int srcOffset = 0;
        try (final RowSequence.Iterator it = rowSequence.getRowSequenceIterator()) {
            while (it.hasMore()) {
                final long firstKey = it.peekNextKey();
                final RowSequence leafKeys = it.getNextRowSequenceThrough(firstKey | INDEX_MASK);
                final int count = leafKeys.intSize();
                final Level2 level2 = ensureLevel2(firstKey);
                final int block2 = block2(firstKey);
                double[] block = level2.getBlock(block2);
                if (block == null) {
                    final DoubleCompactLeaf compact =
                            prepareCompactLeaf(level2, block2, level2.getCompact(block2), count);
                    if (compact != null) {
                        final int[] count1 = {0};
                        leafKeys.forAllRowKeys((final long key) -> offsets[count1[0]++] = (int) (key & INDEX_MASK));
                        compact.putSorted(offsets, values, srcOffset, count);
                        srcOffset += count;
                        continue;
                    }
                    block = level2.getBlock(block2);
                }
                if (values.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                final double[] destBlock = block;
                final int[] position = {srcOffset};
                leafKeys.forAllRowKeyRanges((final long rangeFirst, final long rangeLast) -> {
                    final int length = (int) (rangeLast - rangeFirst + 1);
                    values.copyToTypedArray(position[0], destBlock, (int) (rangeFirst & INDEX_MASK), length);
                    position[0] += length;
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
        final DoubleChunk<? extends Values> values = src.asDoubleChunk();
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
                final double[] block = level2.getBlock(block2);
                if (block != null && values.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
            }
            final int offset = (int) (key & INDEX_MASK);
            final double[] block = level2.getBlock(block2);
            if (block != null) {
                block[offset] = values.get(ki);
            } else {
                setInCompactLeaf(level2, block2, offset, values.get(ki));
            }
        }
    }

    @TestUseOnly
    public long estimateSize() {
        return root.estimateSize();
    }
}
