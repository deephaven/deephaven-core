/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.util.chunkfillers.ChunkFiller;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import org.jetbrains.annotations.NotNull;

import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.db.v2.sources.ArrayBackedColumnSource.USE_RANGES_AVERAGE_RUN_LENGTH;
import static io.deephaven.db.v2.sources.sparse.SparseConstants.*;
import static io.deephaven.db.v2.sources.sparse.SparseConstants.IN_USE_MASK;

/**
 * Array-backed ColumnSource for DBDateTimes. Allows reinterpret as long.
 */
public class DateTimeSparseArraySource extends AbstractSparseLongArraySource<DBDateTime>
    implements MutableColumnSourceGetDefaults.ForLongAsDateTime, DefaultChunkSource<Values> {

    public DateTimeSparseArraySource() {
        super(DBDateTime.class);
    }

    @Override
    WritableSource reinterpretForSerialization() {
        return (WritableSource) reinterpret(long.class);
    }

    @Override
    public void set(long key, DBDateTime value) {
        set(key, value == null ? NULL_LONG : value.getNanos());
    }

    @Override
    public void copy(ColumnSource<DBDateTime> sourceColumn, long sourceKey, long destKey) {
        set(destKey, sourceColumn.get(sourceKey));
    }

    @Override
    public <ALTERNATE_DATA_TYPE> boolean allowsReinterpret(
        @NotNull final Class<ALTERNATE_DATA_TYPE> alternateDataType) {
        return alternateDataType == long.class;
    }


    // the ArrayBackedColumnSource fillChunk can't handle changing the type
    @Override
    public void fillChunk(@NotNull FillContext context, @NotNull WritableChunk<? super Values> dest,
        @NotNull OrderedKeys orderedKeys) {
        final ChunkFiller filler = dest.getChunkFiller();
        if (orderedKeys.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillByRanges(this, orderedKeys, dest);
        } else {
            filler.fillByIndices(this, orderedKeys, dest);
        }
    }

    @Override
    public void fillPrevChunk(@NotNull FillContext context,
        @NotNull WritableChunk<? super Values> dest, @NotNull OrderedKeys orderedKeys) {
        final ChunkFiller filler = dest.getChunkFiller();
        if (orderedKeys.getAverageRunLengthEstimate() > USE_RANGES_AVERAGE_RUN_LENGTH) {
            filler.fillPrevByRanges(this, orderedKeys, dest);
        } else {
            filler.fillPrevByIndices(this, orderedKeys, dest);
        }
    }

    @Override
    public Chunk<Values> getChunk(@NotNull GetContext context, @NotNull OrderedKeys orderedKeys) {
        return getChunkByFilling(context, orderedKeys);
    }

    @Override
    public Chunk<Values> getPrevChunk(@NotNull GetContext context,
        @NotNull OrderedKeys orderedKeys) {
        return getPrevChunkByFilling(context, orderedKeys);
    }

    @Override
    void fillByUnorderedKeys(@NotNull WritableChunk<? super Values> dest,
        @NotNull LongChunk<? extends Attributes.KeyIndices> keys) {
        final WritableObjectChunk<DBDateTime, ? super Values> objectChunk =
            dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size();) {
            final long firstKey = keys.get(ii);
            if (firstKey == Index.NULL_KEY) {
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
                objectChunk.set(ii++, nanos == NULL_LONG ? null : new DBDateTime(nanos));
            }
        }
        dest.setSize(keys.size());
    }

    void fillPrevByUnorderedKeys(@NotNull WritableChunk<? super Values> dest,
        @NotNull LongChunk<? extends Attributes.KeyIndices> keys) {
        final WritableObjectChunk<DBDateTime, ? super Values> objectChunk =
            dest.asWritableObjectChunk();
        for (int ii = 0; ii < keys.size();) {
            final long firstKey = keys.get(ii);
            if (firstKey == Index.NULL_KEY) {
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
            final long[] prevBlock =
                prevInUse == null ? null : prevBlocks.getInnermostBlockByKeyOrNull(firstKey);
            while (ii <= lastII) {
                final int indexWithinBlock = (int) (keys.get(ii) & INDEX_MASK);
                final int indexWithinInUse = indexWithinBlock >> LOG_INUSE_BITSET_SIZE;
                final long maskWithinInUse = 1L << (indexWithinBlock & IN_USE_MASK);

                final long[] blockToUse =
                    (prevInUse != null && (prevInUse[indexWithinInUse] & maskWithinInUse) != 0)
                        ? prevBlock
                        : block;
                final long nanos = blockToUse == null ? NULL_LONG : blockToUse[indexWithinBlock];
                objectChunk.set(ii++, nanos == NULL_LONG ? null : new DBDateTime(nanos));
            }
        }
        dest.setSize(keys.size());
    }

    @Override
    public void fillFromChunkByRanges(@NotNull OrderedKeys orderedKeys,
        Chunk<? extends Values> src) {
        final ObjectChunk<DBDateTime, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<Attributes.OrderedKeyRanges> ranges = orderedKeys.asKeyRangesChunk();
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

                    final DBDateTime time = chunk.get(offset + jj);
                    block[sIndexWithinBlock + jj] = (time == null) ? NULL_LONG : time.getNanos();
                }

                firstKey += length;
                offset += length;
            }
        }
    }

    @Override
    public void fillFromChunkByKeys(@NotNull OrderedKeys orderedKeys, Chunk<? extends Values> src) {
        final ObjectChunk<DBDateTime, ? extends Values> chunk = src.asObjectChunk();
        final LongChunk<Attributes.OrderedKeyIndices> keys = orderedKeys.asKeyIndicesChunk();

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

                final DBDateTime time = chunk.get(ii++);
                block[indexWithinBlock] = (time == null) ? NULL_LONG : time.getNanos();
            }
        }
    }

    @Override
    public void fillFromChunkUnordered(@NotNull FillFromContext context,
        @NotNull Chunk<? extends Values> src, @NotNull LongChunk<Attributes.KeyIndices> keys) {
        if (keys.size() == 0) {
            return;
        }
        final ObjectChunk<DBDateTime, ? extends Values> chunk = src.asObjectChunk();

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

            // This conditional with its constant condition should be very friendly to the branch
            // predictor.

            long key = keys.get(ii);
            do {
                final int indexWithinBlock = (int) (key & INDEX_MASK);

                if (hasPrev) {
                    final long[] prevBlockInner = shouldRecordPrevious(keys.get(ii));
                    if (prevBlockInner != null) {
                        prevBlockInner[indexWithinBlock] = block[indexWithinBlock];
                    }
                }
                final DBDateTime time = chunk.get(ii++);
                block[indexWithinBlock] = (time == null) ? NULL_LONG : time.getNanos();
            } while (ii < keys.size() && (key = keys.get(ii)) >= minKeyInCurrentBlock
                && key <= maxKeyInCurrentBlock);
        }
    }
}
