/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterArraySource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources;

import java.util.function.LongFunction;
import java.util.function.ToLongFunction;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.LocalDate;
import java.time.LocalTime;
import io.deephaven.base.verify.Require;
import java.time.ZoneId;

import io.deephaven.time.DateTime;
import io.deephaven.engine.table.impl.util.copy.CopyKernel;

import gnu.trove.list.array.TIntArrayList;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.util.SoftRecycler;
import io.deephaven.util.compare.LongComparisons;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.commons.lang3.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.type.TypeUtils.box;
import static io.deephaven.util.type.TypeUtils.unbox;

/**
 * Simple array source for Long.
 * <p>
 * The C-haracterArraySource is replicated to all other types with
 * io.deephaven.engine.table.impl.sources.Replicate.
 *
 * (C-haracter is deliberately spelled that way in order to prevent Replicate from altering this very comment).
 */
public class LongArraySource extends ArraySourceHelper<Long, long[]>
        implements MutableColumnSourceGetDefaults.ForLong , ConvertableTimeSource {
    private static final SoftRecycler<long[]> recycler = new SoftRecycler<>(DEFAULT_RECYCLER_CAPACITY,
            () -> new long[BLOCK_SIZE], null);

    private long[][] blocks;
    private transient long[][] prevBlocks;

    public LongArraySource() {
        super(long.class);
        blocks = new long[INITIAL_NUMBER_OF_BLOCKS][];
        maxIndex = INITIAL_MAX_INDEX;
    }

    @Override
    public void startTrackingPrevValues() {
        super.startTrackingPrev(blocks.length);
        prevBlocks = new long[blocks.length][];
    }

    @Override
    public void ensureCapacity(long capacity, boolean nullFill) {
        ensureCapacity(capacity, blocks, prevBlocks, nullFill);
    }

    /**
     * This version of `prepareForParallelPopulation` will internally call {@link #ensureCapacity(long, boolean)} to
     * make sure there is room for the incoming values.
     *
     * @param changedRows row set in the dense table
     */
    @Override
    public void prepareForParallelPopulation(RowSequence changedRows) {
        final long currentStep = LogicalClock.DEFAULT.currentStep();
        if (ensurePreviousClockCycle == currentStep) {
            throw new IllegalStateException("May not call ensurePrevious twice on one clock cycle!");
        }
        ensurePreviousClockCycle = currentStep;

        if (changedRows.isEmpty()) {
            return;
        }

        // ensure that this source will have sufficient capacity to store these rows, does not need to be
        // null-filled as the values will be immediately written
        ensureCapacity(changedRows.lastRowKey() + 1, false);

        if (prevFlusher != null) {
            prevFlusher.maybeActivate();
        } else {
            // we are not tracking this source yet so we have nothing to do for the previous values
            return;
        }

        try (final RowSequence.Iterator it = changedRows.getRowSequenceIterator()) {
            do {
                final long firstKey = it.peekNextKey();

                final int block = (int) (firstKey >> LOG_BLOCK_SIZE);

                final long[] inUse;
                if (prevBlocks[block] == null) {
                    prevBlocks[block] = recycler.borrowItem();
                    prevInUse[block] = inUse = inUseRecycler.borrowItem();
                    if (prevAllocated == null) {
                        prevAllocated = new TIntArrayList();
                    }
                    prevAllocated.add(block);
                } else {
                    inUse = prevInUse[block];
                }

                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

                it.getNextRowSequenceThrough(maxKeyInCurrentBlock).forAllRowKeys(key -> {
                    final int nextIndexWithinBlock = (int) (key & INDEX_MASK);
                    final int nextIndexWithinInUse = nextIndexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                    final long nextMaskWithinInUse = 1L << nextIndexWithinBlock;
                    prevBlocks[block][nextIndexWithinBlock] = blocks[block][nextIndexWithinBlock];
                    inUse[nextIndexWithinInUse] |= nextMaskWithinInUse;
                });
            } while (it.hasMore());
        }
    }

    @Override
    public final void set(long key, Long value) {
        set(key, unbox(value));
    }

    @Override
    public final void set(long key, long value) {
        final int block = (int) (key >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (key & INDEX_MASK);
        if (shouldRecordPrevious(key, prevBlocks, recycler)) {
            prevBlocks[block][indexWithinBlock] = blocks[block][indexWithinBlock];
        }
        blocks[block][indexWithinBlock] = value;
    }

    @Override
    public void setNull(long key) {
        set(key, NULL_LONG);
    }

    @Override
    public final long getLong(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return NULL_LONG;
        }
        return getUnsafe(rowKey);
    }

    public final long getUnsafe(long rowKey) {
        final int blockIndex = (int) (rowKey >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (rowKey & INDEX_MASK);
        return blocks[blockIndex][indexWithinBlock];
    }

    public final long getAndSetUnsafe(long rowKey, long newValue) {
        final int blockIndex = (int) (rowKey >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (rowKey & INDEX_MASK);
        final long oldValue = blocks[blockIndex][indexWithinBlock];
        if (!LongComparisons.eq(oldValue, newValue)) {
            if (shouldRecordPrevious(rowKey, prevBlocks, recycler)) {
                prevBlocks[blockIndex][indexWithinBlock] = oldValue;
            }
            blocks[blockIndex][indexWithinBlock] = newValue;
        }
        return oldValue;
    }

    // region getAndAddUnsafe
    public final long getAndAddUnsafe(long index, long addend) {
        final int blockIndex = (int) (index >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (index & INDEX_MASK);
        final long oldValue = blocks[blockIndex][indexWithinBlock];
        if (addend != 0) {
            if (shouldRecordPrevious(index, prevBlocks, recycler)) {
                prevBlocks[blockIndex][indexWithinBlock] = oldValue;
            }
            blocks[blockIndex][indexWithinBlock] = oldValue + addend;
        }
        return oldValue;
    }
    // endregion getAndAddUnsafe

    @Override
    public Long getPrev(long rowKey) {
        return box(getPrevLong(rowKey));
    }

    @Override
    public final long getPrevLong(long rowKey) {
        if (rowKey < 0 || rowKey > maxIndex) {
            return NULL_LONG;
        }
        final int blockIndex = (int) (rowKey >> LOG_BLOCK_SIZE);
        final int indexWithinBlock = (int) (rowKey & INDEX_MASK);
        if (shouldUsePrevious(rowKey)) {
            return prevBlocks[blockIndex][indexWithinBlock];
        } else {
            return blocks[blockIndex][indexWithinBlock];
        }
    }

    @Override
    public void shift(long start, long end, long offset) {
        if (offset > 0) {
            for (long i = (int) end; i >= start; i--) {
                set((i + offset), getLong(i));
            }
        } else {
            for (int i = (int) start; i <= end; i++) {
                set((i + offset), getLong(i));
            }
        }
    }

    public void move(long source, long dest, long length) {
        if (prevBlocks != null) {
            throw new UnsupportedOperationException();
        }
        if (source == dest) {
            return;
        }
        if (((source - dest) & INDEX_MASK) == 0 && (source & INDEX_MASK) == 0) {
            // TODO (#3359): we can move full blocks!
        }
        if (source < dest && source + length >= dest) {
            for (long ii = length - 1; ii >= 0; ) {
                final long sourceKey = source + ii;
                final long destKey = dest + ii;
                final int sourceBlock = (int) (sourceKey >> LOG_BLOCK_SIZE);
                final int sourceIndexWithinBlock = (int) (sourceKey & INDEX_MASK);

                final int destBlock = (int) (destKey >> LOG_BLOCK_SIZE);
                final int destIndexWithinBlock = (int) (destKey & INDEX_MASK);

                final int valuesInBothBlocks = Math.min(destIndexWithinBlock + 1, sourceIndexWithinBlock + 1);
                final int toMove = (ii + 1) < valuesInBothBlocks ? (int)(ii + 1): valuesInBothBlocks;

                System.arraycopy(blocks[sourceBlock], sourceIndexWithinBlock - toMove + 1, blocks[destBlock], destIndexWithinBlock - toMove + 1, toMove);
                ii -= toMove;
            }
        } else {
            for (long ii = 0; ii < length;) {
                final long sourceKey = source + ii;
                final long destKey = dest + ii;
                final int sourceBlock = (int) (sourceKey >> LOG_BLOCK_SIZE);
                final int sourceIndexWithinBlock = (int) (sourceKey & INDEX_MASK);

                final int destBlock = (int) (destKey >> LOG_BLOCK_SIZE);
                final int destIndexWithinBlock = (int) (destKey & INDEX_MASK);

                final int valuesInBothBlocks = BLOCK_SIZE - Math.max(destIndexWithinBlock, sourceIndexWithinBlock);
                final int toMove = (length - ii < valuesInBothBlocks) ? (int)(length - ii): valuesInBothBlocks;

                System.arraycopy(blocks[sourceBlock], sourceIndexWithinBlock, blocks[destBlock], destIndexWithinBlock, toMove);
                ii += toMove;
            }
        }
    }

    @Override
    final long[] allocateNullFilledBlock(int size) {
        final long[] newBlock = new long[size];
        Arrays.fill(newBlock, NULL_LONG);
        return newBlock;
    }

    @Override
    final long[] allocateBlock(int size) {
        return new long[size];
    }

    @Override
    void resetBlocks(long[][] newBlocks, long[][] newPrev) {
        blocks = newBlocks;
        prevBlocks = newPrev;
    }

    @Override
    long[][] getPrevBlocks() {
        return prevBlocks;
    }

    @Override
    SoftRecycler<long[]> getRecycler() {
        return recycler;
    }

    @Override
    Object getBlock(int blockIndex) {
        return blocks[blockIndex];
    }

    @Override
    Object getPrevBlock(int blockIndex) {
        return prevBlocks[blockIndex];
    }

    @Override
    public long resetWritableChunkToBackingStore(@NotNull ResettableWritableChunk<?> chunk, long position) {
        Assert.eqNull(prevInUse, "prevInUse");
        final int blockNo = getBlockNo(position);
        final long [] backingArray = blocks[blockNo];
        chunk.asResettableWritableLongChunk().resetFromTypedArray(backingArray, 0, BLOCK_SIZE);
        return ((long)blockNo) << LOG_BLOCK_SIZE;
    }

    @Override
    public long resetWritableChunkToBackingStoreSlice(@NotNull ResettableWritableChunk<?> chunk, long position) {
        Assert.eqNull(prevInUse, "prevInUse");
        final int blockNo = getBlockNo(position);
        final long [] backingArray = blocks[blockNo];
        final long firstPosition = ((long) blockNo) << LOG_BLOCK_SIZE;
        final int offset = (int)(position - firstPosition);
        final int capacity = BLOCK_SIZE - offset;
        chunk.asResettableWritableLongChunk().resetFromTypedArray(backingArray, offset, capacity);
        return capacity;
    }

    // region fillChunk
    @Override
    public /* TYPE_MIXIN */ void fillChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillSparseChunk(destination, rowSequence /* CONVERTER_ARG */);
            return;
        }
        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destination.asWritableLongChunk();
        // endregion chunkDecl
        MutableInt destOffset = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((final long from, final long to) -> {
            final int fromBlock = getBlockNo(from);
            final int toBlock = getBlockNo(to);
            final int fromOffsetInBlock = (int) (from & INDEX_MASK);
            if (fromBlock == toBlock) {
                final int sz = LongSizedDataStructure.intSize("int cast", to - from + 1);
                // region copyFromArray
                destination.copyFromArray(getBlock(fromBlock), fromOffsetInBlock, destOffset.intValue(), sz);
                // endregion copyFromArray
                destOffset.add(sz);
            } else {
                final int sz = BLOCK_SIZE - fromOffsetInBlock;
                // region copyFromArray
                destination.copyFromArray(getBlock(fromBlock), fromOffsetInBlock, destOffset.intValue(), sz);
                // endregion copyFromArray
                destOffset.add(sz);
                for (int blockNo = fromBlock + 1; blockNo < toBlock; ++blockNo) {
                    // region copyFromArray
                    destination.copyFromArray(getBlock(blockNo), 0, destOffset.intValue(), BLOCK_SIZE);
                    // endregion copyFromArray
                    destOffset.add(BLOCK_SIZE);
                }
                int restSz = (int) (to & INDEX_MASK) + 1;
                // region copyFromArray
                destination.copyFromArray(getBlock(toBlock), 0, destOffset.intValue(), restSz);
                // endregion copyFromArray
                destOffset.add(restSz);
            }
        });
        destination.setSize(destOffset.intValue());
    }
    
    public <R> void fillChunk(
            @NotNull final ChunkSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            , LongFunction<R> converter) {
        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillSparseChunk(destination, rowSequence , converter);
            return;
        }
        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destination.asWritableObjectChunk();
        // endregion chunkDecl
        MutableInt destOffset = new MutableInt(0);
        rowSequence.forAllRowKeyRanges((final long from, final long to) -> {
            final int fromBlock = getBlockNo(from);
            final int toBlock = getBlockNo(to);
            final int fromOffsetInBlock = (int) (from & INDEX_MASK);
            if (fromBlock == toBlock) {
                final int sz = LongSizedDataStructure.intSize("int cast", to - from + 1);
                // region copyFromArray
                {
                    long[] block = (long[])getBlock(fromBlock);
                    for (int ii = 0; ii < sz; ii++) {
                        chunk.set(ii + destOffset.intValue(), converter.apply(block[ii + fromOffsetInBlock]));
                    }
                }
                // endregion copyFromArray
                destOffset.add(sz);
            } else {
                final int sz = BLOCK_SIZE - fromOffsetInBlock;
                // region copyFromArray
                {
                    long[] block = (long[])getBlock(fromBlock);
                    for (int ii = 0; ii < sz; ii++) {
                        chunk.set(ii + destOffset.intValue(), converter.apply(block[ii + fromOffsetInBlock]));
                    }
                }
                // endregion copyFromArray
                destOffset.add(sz);
                for (int blockNo = fromBlock + 1; blockNo < toBlock; ++blockNo) {
                    // region copyFromArray
                    {
                        long[] block = (long[])getBlock(blockNo);
                        for (int ii = 0; ii < BLOCK_SIZE; ii++) {
                            chunk.set(ii + destOffset.intValue(), converter.apply(block[ii + 0]));
                        }
                    }
                    // endregion copyFromArray
                    destOffset.add(BLOCK_SIZE);
                }
                int restSz = (int) (to & INDEX_MASK) + 1;
                // region copyFromArray
                {
                    long[] block = (long[])getBlock(toBlock);
                    for (int ii = 0; ii < restSz; ii++) {
                        chunk.set(ii + destOffset.intValue(), converter.apply(block[ii + 0]));
                    }
                }
                // endregion copyFromArray
                destOffset.add(restSz);
            }
        });
        destination.setSize(destOffset.intValue());
    }
    // endregion fillChunk

    private interface CopyFromBlockFunctor {
        void copy(int blockNo, int srcOffset, int length);
    }

    // region fillPrevChunk
    @Override
    public /* TYPE_MIXIN */ void fillPrevChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            /* CONVERTER */) {
        if (prevFlusher == null) {
            fillChunk(context, destination, rowSequence /* CONVERTER_ARG */);
            return;
        }

        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillSparsePrevChunk(destination, rowSequence /* CONVERTER_ARG */);
            return;
        }

        final ArraySourceHelper.FillContext effectiveContext = (ArraySourceHelper.FillContext) context;
        final MutableInt destOffset = new MutableInt(0);

        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destination.asWritableLongChunk();
        // endregion chunkDecl

        CopyFromBlockFunctor lambda = (blockNo, srcOffset, length) -> {
            final long[] inUse = prevInUse[blockNo];
            if (inUse != null) {
                // region conditionalCopy
                effectiveContext.copyKernel.conditionalCopy(destination, getBlock(blockNo), getPrevBlock(blockNo),
                        inUse, srcOffset, destOffset.intValue(), length);
                // endregion conditionalCopy
            } else {
                // region copyFromArray
                destination.copyFromArray(getBlock(blockNo), srcOffset, destOffset.intValue(), length);
                // endregion copyFromArray
            }
            destOffset.add(length);
        };

        rowSequence.forAllRowKeyRanges((final long from, final long to) -> {
            final int fromBlock = getBlockNo(from);
            final int toBlock = getBlockNo(to);
            final int fromOffsetInBlock = (int) (from & INDEX_MASK);
            if (fromBlock == toBlock) {
                final int sz = LongSizedDataStructure.intSize("int cast", to - from + 1);
                lambda.copy(fromBlock, fromOffsetInBlock, sz);
            } else {
                final int sz = BLOCK_SIZE - fromOffsetInBlock;
                lambda.copy(fromBlock, fromOffsetInBlock, sz);

                for (int blockNo = fromBlock + 1; blockNo < toBlock; ++blockNo) {
                    lambda.copy(blockNo, 0, BLOCK_SIZE);
                }

                int restSz = (int) (to & INDEX_MASK) + 1;
                lambda.copy(toBlock, 0, restSz);
            }
        });
        destination.setSize(destOffset.intValue());
    }
    
    public <R> void fillPrevChunk(
            @NotNull final ColumnSource.FillContext context,
            @NotNull final WritableChunk<? super Values> destination,
            @NotNull final RowSequence rowSequence
            , LongFunction<R> converter) {
        if (prevFlusher == null) {
            fillChunk(context, destination, rowSequence , converter);
            return;
        }

        if (rowSequence.getAverageRunLengthEstimate() < USE_RANGES_AVERAGE_RUN_LENGTH) {
            fillSparsePrevChunk(destination, rowSequence , converter);
            return;
        }

        final ArraySourceHelper.FillContext effectiveContext = (ArraySourceHelper.FillContext) context;
        final MutableInt destOffset = new MutableInt(0);

        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destination.asWritableObjectChunk();
        // endregion chunkDecl

        CopyFromBlockFunctor lambda = (blockNo, srcOffset, length) -> {
            final long[] inUse = prevInUse[blockNo];
            if (inUse != null) {
                // region conditionalCopy
                long[] baseInput = (long[]) getBlock(blockNo);
                long[] overInput = (long[]) getPrevBlock(blockNo);
                effectiveContext.copyKernel.conditionalCopy(destination, baseInput, overInput,
                        inUse, srcOffset, destOffset.intValue(), length);

                int bitsSet = 0;
                final int bitsetLen = (length + 63) >> 6;
                final int bitsetOffset = srcOffset >> 6;
                for (int i = 0; i < bitsetLen; ++i) {
                    bitsSet += Long.bitCount(inUse[i + bitsetOffset]);
                }
                final int totalBits = bitsetLen << 6;
                final boolean flipBase = bitsSet > totalBits / 2;

                // mem-copy from baseline
                for (int ii = 0; ii < length; ++ii) {
                    chunk.set(destOffset.intValue() + ii, converter.apply((flipBase ? overInput : baseInput)[srcOffset + ii]));
                }

                final int srcEndOffset = srcOffset + length;
                for (int ii = CopyKernel.Utils.nextSetBit(inUse, srcOffset, srcEndOffset, flipBase);
                     ii < srcEndOffset;
                     ii = CopyKernel.Utils.nextSetBit(inUse, ii + 1, srcEndOffset, flipBase)) {
                    chunk.set(destOffset.intValue() + ii - srcOffset,
                            converter.apply(flipBase ? baseInput[ii] : overInput[ii]));
                }
                // endregion conditionalCopy
            } else {
                // region copyFromArray
                {
                    long[] block = (long[])getBlock(blockNo);
                    for (int ii = 0; ii < length; ii++) {
                        chunk.set(ii + destOffset.intValue(), converter.apply(block[ii + srcOffset]));
                    }
                }
                // endregion copyFromArray
            }
            destOffset.add(length);
        };

        rowSequence.forAllRowKeyRanges((final long from, final long to) -> {
            final int fromBlock = getBlockNo(from);
            final int toBlock = getBlockNo(to);
            final int fromOffsetInBlock = (int) (from & INDEX_MASK);
            if (fromBlock == toBlock) {
                final int sz = LongSizedDataStructure.intSize("int cast", to - from + 1);
                lambda.copy(fromBlock, fromOffsetInBlock, sz);
            } else {
                final int sz = BLOCK_SIZE - fromOffsetInBlock;
                lambda.copy(fromBlock, fromOffsetInBlock, sz);

                for (int blockNo = fromBlock + 1; blockNo < toBlock; ++blockNo) {
                    lambda.copy(blockNo, 0, BLOCK_SIZE);
                }

                int restSz = (int) (to & INDEX_MASK) + 1;
                lambda.copy(toBlock, 0, restSz);
            }
        });
        destination.setSize(destOffset.intValue());
    }
    // endregion fillPrevChunk

    // region fillSparseChunk
    @Override
    protected /* TYPE_MIXIN */ void fillSparseChunk(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final RowSequence rows
            /* CONVERTER */) {
        if (rows.size() == 0) {
            destGeneric.setSize(0);
            return;
        }
        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destGeneric.asWritableLongChunk();
        // endregion chunkDecl
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        rows.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
            }
            // region conversion
            chunk.set(ctx.offset++, ctx.currentBlock[(int) (v & INDEX_MASK)]);
            // endregion conversion
        });
        chunk.setSize(ctx.offset);
    }
    
    protected <R> void fillSparseChunk(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final RowSequence rows
            , LongFunction<R> converter) {
        if (rows.size() == 0) {
            destGeneric.setSize(0);
            return;
        }
        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destGeneric.asWritableObjectChunk();
        // endregion chunkDecl
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        rows.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
            }
            // region conversion
            chunk.set(ctx.offset++,converter.apply( ctx.currentBlock[(int) (v & INDEX_MASK)]));
            // endregion conversion
        });
        chunk.setSize(ctx.offset);
    }
    // endregion fillSparseChunk

    // region fillSparsePrevChunk
    @Override
    protected /* TYPE_MIXIN */ void fillSparsePrevChunk(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final RowSequence rows
            /* CONVERTER */) {
        final long sz = rows.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }

        if (prevFlusher == null) {
            fillSparseChunk(destGeneric, rows /* CONVERTER_ARG */);
            return;
        }

        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destGeneric.asWritableLongChunk();
        // endregion chunkDecl
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        rows.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
                ctx.currentPrevBlock = prevBlocks[ctx.currentBlockNo];
                ctx.prevInUseBlock = prevInUse[ctx.currentBlockNo];
            }

            final int indexWithinBlock = (int) (v & INDEX_MASK);
            final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
            final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
            final boolean usePrev = ctx.prevInUseBlock != null && (ctx.prevInUseBlock[indexWithinInUse] & maskWithinInUse) != 0;
            // region conversion
            chunk.set(ctx.offset++, usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock]);
            // endregion conversion
        });
        chunk.setSize(ctx.offset);
    }
    
    protected <R> void fillSparsePrevChunk(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final RowSequence rows
            , LongFunction<R> converter) {
        final long sz = rows.size();
        if (sz == 0) {
            destGeneric.setSize(0);
            return;
        }

        if (prevFlusher == null) {
            fillSparseChunk(destGeneric, rows , converter);
            return;
        }

        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destGeneric.asWritableObjectChunk();
        // endregion chunkDecl
        final FillSparseChunkContext<long[]> ctx = new FillSparseChunkContext<>();
        rows.forAllRowKeys((final long v) -> {
            if (v >= ctx.capForCurrentBlock) {
                ctx.currentBlockNo = getBlockNo(v);
                ctx.capForCurrentBlock = (ctx.currentBlockNo + 1L) << LOG_BLOCK_SIZE;
                ctx.currentBlock = blocks[ctx.currentBlockNo];
                ctx.currentPrevBlock = prevBlocks[ctx.currentBlockNo];
                ctx.prevInUseBlock = prevInUse[ctx.currentBlockNo];
            }

            final int indexWithinBlock = (int) (v & INDEX_MASK);
            final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
            final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);
            final boolean usePrev = ctx.prevInUseBlock != null && (ctx.prevInUseBlock[indexWithinInUse] & maskWithinInUse) != 0;
            // region conversion
            chunk.set(ctx.offset++,converter.apply( usePrev ? ctx.currentPrevBlock[indexWithinBlock] : ctx.currentBlock[indexWithinBlock]));
            // endregion conversion
        });
        chunk.setSize(ctx.offset);
    }
    // endregion fillSparsePrevChunk

    // region fillSparseChunkUnordered
    @Override
    protected /* TYPE_MIXIN */ void fillSparseChunkUnordered(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends RowKeys> rows
            /* CONVERTER */) {
        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destGeneric.asWritableLongChunk();
        // endregion chunkDecl
        final int sz = rows.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = rows.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii, NULL_LONG);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                chunk.set(ii, NULL_LONG);
            } else {
                final long[] currentBlock = blocks[blockNo];
                // region conversion
                chunk.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
                // endregion conversion
            }
        }
        chunk.setSize(sz);
    }
    
    protected <R> void fillSparseChunkUnordered(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends RowKeys> rows
            , LongFunction<R> converter) {
        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destGeneric.asWritableObjectChunk();
        // endregion chunkDecl
        final int sz = rows.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = rows.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii, null);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                chunk.set(ii, null);
            } else {
                final long[] currentBlock = blocks[blockNo];
                // region conversion
                chunk.set(ii,converter.apply( currentBlock[(int) (fromIndex & INDEX_MASK)]));
                // endregion conversion
            }
        }
        chunk.setSize(sz);
    }
    // endregion fillSparseChunkUnordered

    // region fillSparsePrevChunkUnordered
    @Override
    protected /* TYPE_MIXIN */ void fillSparsePrevChunkUnordered(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends RowKeys> rows
            /* CONVERTER */) {
        // region chunkDecl
        final WritableLongChunk<? super Values> chunk = destGeneric.asWritableLongChunk();
        // endregion chunkDecl
        final int sz = rows.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = rows.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii, NULL_LONG);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                chunk.set(ii, NULL_LONG);
                continue;
            }
            final long[] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            // region conversion
            chunk.set(ii, currentBlock[(int) (fromIndex & INDEX_MASK)]);
            // endregion conversion
        }
        chunk.setSize(sz);
    }
    
    protected <R> void fillSparsePrevChunkUnordered(
            @NotNull final WritableChunk<? super Values> destGeneric,
            @NotNull final LongChunk<? extends RowKeys> rows
            , LongFunction<R> converter) {
        // region chunkDecl
        final WritableObjectChunk<R, ? super Values> chunk = destGeneric.asWritableObjectChunk();
        // endregion chunkDecl
        final int sz = rows.size();
        for (int ii = 0; ii < sz; ++ii) {
            final long fromIndex = rows.get(ii);
            if (fromIndex == RowSequence.NULL_ROW_KEY) {
                chunk.set(ii, null);
                continue;
            }
            final int blockNo = getBlockNo(fromIndex);
            if (blockNo >= blocks.length) {
                chunk.set(ii, null);
                continue;
            }
            final long[] currentBlock = shouldUsePrevious(fromIndex) ? prevBlocks[blockNo] : blocks[blockNo];
            // region conversion
            chunk.set(ii,converter.apply( currentBlock[(int) (fromIndex & INDEX_MASK)]));
            // endregion conversion
        }
        chunk.setSize(sz);
    }
    // endregion fillSparsePrevChunkUnordered

    // region fillFromChunkByRanges
    @Override
    /* TYPE_MIXIN */ void fillFromChunkByRanges(
            @NotNull final RowSequence rowSequence,
            final Chunk<? extends Values> src
            /* CONVERTER */) {
        if (rowSequence.size() == 0) {
            return;
        }
        // region chunkDecl
        final LongChunk<? extends Values> chunk = src.asLongChunk();
        // endregion chunkDecl
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        long[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block0 = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final long[] block = blocks[block0];

                if (block != knownUnaliasedBlock && chunk.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = block;

                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (trackPrevious) {
                    // this should be vectorized
                    for (int jj = 0; jj < length; ++jj) {
                        if (shouldRecordPrevious(firstKey + jj, prevBlocks, recycler)) {
                            prevBlocks[block0][sIndexWithinBlock + jj] = block[sIndexWithinBlock + jj];
                        }
                    }
                }

                // region copyToTypedArray
                chunk.copyToTypedArray(offset, block, sIndexWithinBlock, length);
                // endregion copyToTypedArray
                firstKey += length;
                offset += length;
            }
        }
    }
    
    <R> void fillFromChunkByRanges(
            @NotNull final RowSequence rowSequence,
            final Chunk<? extends Values> src
            , ToLongFunction<R> converter) {
        if (rowSequence.size() == 0) {
            return;
        }
        // region chunkDecl
        final ObjectChunk<R, ? extends Values> chunk = src.asObjectChunk();
        // endregion chunkDecl
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        int offset = 0;
        // This helps us reduce the number of calls to Chunk.isAlias
        long[] knownUnaliasedBlock = null;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block0 = (int) (firstKey >> LOG_BLOCK_SIZE);
                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                final long[] block = blocks[block0];

                if (block != knownUnaliasedBlock && chunk.isAlias(block)) {
                    throw new UnsupportedOperationException("Source chunk is an alias for target data");
                }
                knownUnaliasedBlock = block;

                // This 'if' with its constant condition should be very friendly to the branch predictor.
                if (trackPrevious) {
                    // this should be vectorized
                    for (int jj = 0; jj < length; ++jj) {
                        if (shouldRecordPrevious(firstKey + jj, prevBlocks, recycler)) {
                            prevBlocks[block0][sIndexWithinBlock + jj] = block[sIndexWithinBlock + jj];
                        }
                    }
                }

                // region copyToTypedArray
                for (int jj = 0; jj < length; jj++) {
                    block[jj + sIndexWithinBlock] = converter.applyAsLong(chunk.get(offset + jj));
                }
                // endregion copyToTypedArray
                firstKey += length;
                offset += length;
            }
        }
    }
    // endregion fillFromChunkByRanges

    public void copyFromChunk(long firstKey, final long totalLength, final Chunk<? extends Values> src, int offset) {
        if (totalLength == 0) {
            return;
        }
        final LongChunk<? extends Values> chunk = src.asLongChunk();

        final long lastKey = firstKey + totalLength - 1;

        while (firstKey <= lastKey) {
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
            final int length = (int) (lastKeyToUse - firstKey + 1);

            final int block0 = (int) (firstKey >> LOG_BLOCK_SIZE);
            final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
            final long[] block = blocks[block0];

            chunk.copyToTypedArray(offset, block, sIndexWithinBlock, length);
            firstKey += length;
            offset += length;
        }
    }

    // region fillFromChunkByKeys
    @Override
    /* TYPE_MIXIN */ void fillFromChunkByKeys(
            @NotNull final RowSequence rowSequence,
            final Chunk<? extends Values> src
            /* CONVERTER */) {
        if (rowSequence.size() == 0) {
            return;
        }
        // region chunkDecl
        final LongChunk<? extends Values> chunk = src.asLongChunk();
        // endregion chunkDecl
        final LongChunk<OrderedRowKeys> keys = rowSequence.asRowKeyChunk();

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size() && keys.get(lastII + 1) <= maxKeyInCurrentBlock) {
                ++lastII;
            }

            final int block0 = (int) (firstKey >> LOG_BLOCK_SIZE);
            final long[] block = blocks[block0];

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            while (ii <= lastII) {
                final long key = keys.get(ii);
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (trackPrevious) {
                    if (shouldRecordPrevious(key, prevBlocks, recycler)) {
                        prevBlocks[block0][indexWithinBlock] = block[indexWithinBlock];
                    }
                }
                // region conversion
                block[indexWithinBlock] = chunk.get(ii);
                // endregion conversion
                ++ii;
            }
        }
    }
    
    <R> void fillFromChunkByKeys(
            @NotNull final RowSequence rowSequence,
            final Chunk<? extends Values> src
            , ToLongFunction<R> converter) {
        if (rowSequence.size() == 0) {
            return;
        }
        // region chunkDecl
        final ObjectChunk<R, ? extends Values> chunk = src.asObjectChunk();
        // endregion chunkDecl
        final LongChunk<OrderedRowKeys> keys = rowSequence.asRowKeyChunk();

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size() && keys.get(lastII + 1) <= maxKeyInCurrentBlock) {
                ++lastII;
            }

            final int block0 = (int) (firstKey >> LOG_BLOCK_SIZE);
            final long[] block = blocks[block0];

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            while (ii <= lastII) {
                final long key = keys.get(ii);
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (trackPrevious) {
                    if (shouldRecordPrevious(key, prevBlocks, recycler)) {
                        prevBlocks[block0][indexWithinBlock] = block[indexWithinBlock];
                    }
                }
                // region conversion
                block[indexWithinBlock] = converter.applyAsLong(chunk.get(ii));
                // endregion conversion
                ++ii;
            }
        }
    }
    // endregion fillFromChunkByKeys

    // region fillFromChunkUnordered
    @Override
    public /* TYPE_MIXIN */ void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys
            /* CONVERTER */) {
        if (keys.size() == 0) {
            return;
        }
        // region chunkDecl
        final LongChunk<? extends Values> chunk = src.asLongChunk();
        // endregion chunkDecl

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block0 = (int) (firstKey >> LOG_BLOCK_SIZE);
            final long[] block = blocks[block0];

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (trackPrevious) {
                    if (shouldRecordPrevious(key, prevBlocks, recycler)) {
                        prevBlocks[block0][indexWithinBlock] = block[indexWithinBlock];
                    }
                }
                // region conversion
                block[indexWithinBlock] = chunk.get(ii);
                // endregion conversion
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }
    
    public <R> void fillFromChunkUnordered(
            @NotNull final FillFromContext context,
            @NotNull final Chunk<? extends Values> src,
            @NotNull final LongChunk<RowKeys> keys
            , ToLongFunction<R> converter) {
        if (keys.size() == 0) {
            return;
        }
        // region chunkDecl
        final ObjectChunk<R, ? extends Values> chunk = src.asObjectChunk();
        // endregion chunkDecl

        final boolean trackPrevious = prevFlusher != null && ensurePreviousClockCycle != LogicalClock.DEFAULT.currentStep();

        if (trackPrevious) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size(); ) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block0 = (int) (firstKey >> LOG_BLOCK_SIZE);
            final long[] block = blocks[block0];

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (trackPrevious) {
                    if (shouldRecordPrevious(key, prevBlocks, recycler)) {
                        prevBlocks[block0][indexWithinBlock] = block[indexWithinBlock];
                    }
                }
                // region conversion
                block[indexWithinBlock] = converter.applyAsLong(chunk.get(ii));
                // endregion conversion
                ++ii;
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }
    // endregion fillFromChunkUnordered

    // region reinterpretation
    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(@NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class || alternateDataType == Instant.class || alternateDataType == DateTime.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected <ALTERNATE_DATA_TYPE> ColumnSource<ALTERNATE_DATA_TYPE> doReinterpret(@NotNull Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        if (alternateDataType == this.getType()) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) this;
        } else if(alternateDataType == DateTime.class) {
            return (ColumnSource<ALTERNATE_DATA_TYPE>) toDateTime();
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
    public ColumnSource<ZonedDateTime> toZonedDateTime(final @NotNull ZoneId zone) {
        return new ZonedDateTimeArraySource(Require.neqNull(zone, "zone"), this);
    }

    @Override
    public ColumnSource<LocalDate> toLocalDate(final @NotNull ZoneId zone) {
        return new LocalDateWrapperSource(toZonedDateTime(zone), zone);
    }

    @Override
    public ColumnSource<LocalTime> toLocalTime(final @NotNull ZoneId zone) {
        return new LocalTimeWrapperSource(toZonedDateTime(zone), zone);
    }

    @Override
    public ColumnSource<DateTime> toDateTime() {
        return new DateTimeArraySource(this);
    }

    @Override
    public ColumnSource<Instant> toInstant() {
        return new InstantArraySource(this);
    }

    @Override
    public ColumnSource<Long> toEpochNano() {
        return this;
    }
    // endregion reinterpretation
}
