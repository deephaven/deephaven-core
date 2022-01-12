/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.impl.DefaultChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.MutableColumnSourceGetDefaults;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeyRanges;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.time.DateTime;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.chunkfillers.ChunkFiller;
import io.deephaven.engine.rowset.RowSequence;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.*;
import static io.deephaven.engine.table.impl.sources.sparse.SparseConstants.IN_USE_MASK;

/**
 * Array-backed ColumnSource for DateTimes. Allows reinterpret as long.
 */
public class DateTimeSparseArraySource extends AbstractSparseLongArraySource<DateTime>
        implements MutableColumnSourceGetDefaults.ForLongAsDateTime, DefaultChunkSource<Values> {

    public DateTimeSparseArraySource() {
        super(DateTime.class);
    }

    @Override
    public void set(long key, DateTime value) {
        set(key, value == null ? NULL_LONG : value.getNanos());
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
            @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class;
    }


    // the ArrayBackedColumnSource fillChunk can't handle changing the type
    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(dest.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillByRanges(this, rowSequence, dest);
        } else {
            filler.fillByIndices(this, rowSequence, dest);
        }
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
            @NotNull RowSequence rowSequence) {
        final ChunkFiller filler = ChunkFiller.forChunkType(dest.getChunkType());
        if (rowSequence.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillPrevByRanges(this, rowSequence, dest);
        } else {
            filler.fillPrevByIndices(this, rowSequence, dest);
        }
    }

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getChunkByFilling(context, rowSequence);
    }

    @Override
    public Chunk<Values> getPrevChunk(@NotNull GetContext context, @NotNull RowSequence rowSequence) {
        return getPrevChunkByFilling(context, rowSequence);
    }

    @Override
    void fillByUnRowSequence(@NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableObjectChunk<DateTime, ? super Values> objectChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size();) {
            final long firstKey = keys.get(ii);
            if (firstKey == RowSequence.NULL_ROW_KEY) {
                objectChunk.set(ii++, null);
                continue;
            }
            final long masked = firstKey & ~INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size()) {
                final int nextII = lastII + 1;
                final long nextKey = keys.get(nextII);
                final long nextMasked = nextKey & ~INDEX_MASK;
                if (nextMasked != masked) {
                    break;
                }
                lastII = nextII;
            }
            final long[] block = blocks.getInnermostBlockByKeyOrNull(firstKey);
            if (block == null) {
                objectChunk.fillWithNullValue(ii, lastII - ii + 1);
                ii = lastII + 1;
                continue;
            }
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                final long nanos = block[indexWithinBlock];
                objectChunk.set(ii++, nanos == NULL_LONG ? null : new DateTime(nanos));
            }
        }
        dest.setSize(keys.size());
    }

    void fillPrevByUnRowSequence(@NotNull WritableChunk<? super Values> dest,
            @NotNull LongChunk<? extends RowKeys> keys) {
        final WritableObjectChunk<DateTime, ? super Values> objectChunk = dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size();) {
            final long firstKey = keys.get(ii);
            if (firstKey == RowSequence.NULL_ROW_KEY) {
                objectChunk.set(ii++, null);
                continue;
            }
            final long masked = firstKey & ~INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size()) {
                final int nextII = lastII + 1;
                final long nextKey = keys.get(nextII);
                final long nextMasked = nextKey & ~INDEX_MASK;
                if (nextMasked != masked) {
                    break;
                }
                lastII = nextII;
            }

            final long[] block = blocks.getInnermostBlockByKeyOrNull(firstKey);
            if (block == null) {
                objectChunk.fillWithNullValue(ii, lastII - ii + 1);
                ii = lastII + 1;
                continue;
            }

            final long[] prevInUse = (prevFlusher == null || this.prevInUse == null) ? null
                    : this.prevInUse.getInnermostBlockByKeyOrNull(firstKey);
            final long[] prevBlock = prevInUse == null ? null : prevBlocks.getInnermostBlockByKeyOrNull(firstKey);
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                final long[] blockToUse =
                        (prevInUse != null && (prevInUse[indexWithinInUse] & maskWithinInUse) != 0) ? prevBlock : block;
                final long nanos = blockToUse == null ? NULL_LONG : blockToUse[indexWithinBlock];
                objectChunk.set(ii++, nanos == NULL_LONG ? null : new DateTime(nanos));
            }
        }
        dest.setSize(keys.size());
    }

    @Override
    public void fillFromChunkByRanges(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final ObjectChunk<DateTime, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<OrderedRowKeyRanges> ranges = rowSequence.asRowKeyRangesChunk();
        int offset = 0;
        for (int ii = 0; ii < ranges.size(); ii += 2) {
            long firstKey = ranges.get(ii);
            final long lastKey = ranges.get(ii + 1);

            while (firstKey <= lastKey) {
                final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
                final long lastKeyToUse = Math.min(maxKeyInCurrentBlock, lastKey);
                final int length = (int) (lastKeyToUse - firstKey + 1);

                final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
                final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
                final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
                final long[] block = ensureBlock(block0, block1, block2);

                final int sIndexWithinBlock = (int) (firstKey & INDEX_MASK);
                for (int jj = length - 1; jj >= 0; --jj) {
                    final long[] prevBlockInner = shouldRecordPrevious(firstKey + jj);
                    if (prevBlockInner != null) {
                        prevBlockInner[sIndexWithinBlock + jj] = block[sIndexWithinBlock + jj];
                    }

                    final DateTime time = chunk.get(offset + jj);
                    block[sIndexWithinBlock + jj] = (time == null) ? NULL_LONG : time.getNanos();
                }

                firstKey += length;
                offset += length;
            }
        }
    }

    @Override
    public void fillFromChunkByKeys(@NotNull RowSequence rowSequence, Chunk<? extends Values> src) {
        final ObjectChunk<DateTime, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<OrderedRowKeys> keys = rowSequence.asRowKeyChunk();

        for (int ii = 0; ii < keys.size();) {
            final long firstKey = keys.get(ii);
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;
            int lastII = ii;
            while (lastII + 1 < keys.size() && keys.get(lastII + 1) <= maxKeyInCurrentBlock) {
                ++lastII;
            }

            final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
            final long[] block = ensureBlock(block0, block1, block2);
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                final long[] prevBlockInner = shouldRecordPrevious(keys.get(ii));
                if (prevBlockInner != null) {
                    prevBlockInner[indexWithinBlock] = block[indexWithinBlock];
                }

                final DateTime time = chunk.get(ii++);
                block[indexWithinBlock] = (time == null) ? NULL_LONG : time.getNanos();
            }
        }
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context, @NotNull Chunk<? extends Values> src,
            @NotNull LongChunk<RowKeys> keys) {
        if (keys.size() == 0) {
            return;
        }
        final ObjectChunk<DateTime, ? extends Values> chunk = src.asObjectChunk();

        final boolean hasPrev = prevFlusher != null;

        if (hasPrev) {
            prevFlusher.maybeActivate();
        }

        for (int ii = 0; ii < keys.size();) {
            final long firstKey = keys.get(ii);
            final long minKeyInCurrentBlock = firstKey & ~INDEX_MASK;
            final long maxKeyInCurrentBlock = firstKey | INDEX_MASK;

            final int block0 = (int) (firstKey >> BLOCK0_SHIFT) & BLOCK0_MASK;
            final int block1 = (int) (firstKey >> BLOCK1_SHIFT) & BLOCK1_MASK;
            final int block2 = (int) (firstKey >> BLOCK2_SHIFT) & BLOCK2_MASK;
            final long[] block = ensureBlock(block0, block1, block2);

            if (chunk.isAlias(block)) {
                throw new UnsupportedOperationException("Source chunk is an alias for target data");
            }

            // This conditional with its constant condition should be very friendly to the branch predictor.

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (hasPrev) {
                    final long[] prevBlockInner = shouldRecordPrevious(keys.get(ii));
                    if (prevBlockInner != null) {
                        prevBlockInner[indexWithinBlock] = block[indexWithinBlock];
                    }
                }
                final DateTime time = chunk.get(ii++);
                block[indexWithinBlock] = (time == null) ? NULL_LONG : time.getNanos();
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock && key <= maxKeyInCurrentBlock);
        }
    }
}
