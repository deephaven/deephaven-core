//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.FloatChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.chunkfilter.FloatChunkMatchFilterFactory;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SingleColumnSetKernel} for float keys, held as their bits with every NaN alike and -0.0 as 0.0, so that keys
 * match as they do for {@code ==} in the query language.
 */
final class FloatSetKernel extends SingleColumnSetKernel {

    /**
     * The number of set rows holding each key's bits. Must be a fastutil open hash map: see
     * {@link SharedSetKernel#kernel()} for the behavior we rely on when it is concurrently modified.
     */
    private final Int2LongOpenHashMap counts = new Int2LongOpenHashMap();

    FloatSetKernel(@NotNull final ColumnSource<?> keySource) {
        super(keySource);
    }

    @Override
    long size() {
        return counts.size();
    }

    @Override
    void addKeys(@NotNull final Chunk<? extends Values> keys) {
        final FloatChunk<? extends Values> typedKeys = keys.asFloatChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final int sizeBefore = counts.size();
            final long oldCount = counts.addTo(FloatChunkMatchFilterFactory.getBits(typedKeys.get(ii)), 1);
            if (oldCount == 0) {
                // A key at zero is still in the map until finishRemove, so only an insertion grows it.
                if (counts.size() == sizeBefore) {
                    ++revivedKeys;
                } else {
                    ++insertedKeys;
                }
            }
        }
    }

    @Override
    void removeKeys(@NotNull final Chunk<? extends Values> keys) {
        final FloatChunk<? extends Values> typedKeys = keys.asFloatChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final long oldCount = counts.addTo(FloatChunkMatchFilterFactory.getBits(typedKeys.get(ii)), -1);
            Assert.gtZero(oldCount, "oldCount");
            if (oldCount == 1) {
                ++emptiedKeys;
            }
        }
    }

    @Override
    void dropEmptied(@NotNull final Chunk<? extends Values> keys) {
        final FloatChunk<? extends Values> typedKeys = keys.asFloatChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            counts.remove(FloatChunkMatchFilterFactory.getBits(typedKeys.get(ii)), 0L);
        }
    }

    @Override
    void match(
            @NotNull final Chunk<Values> keys,
            @NotNull final LongChunk<OrderedRowKeys> rowKeys,
            @NotNull final WritableLongChunk<OrderedRowKeys> results,
            final boolean inclusion) {
        final FloatChunk<Values> typedKeys = keys.asFloatChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            if (counts.containsKey(FloatChunkMatchFilterFactory.getBits(typedKeys.get(ii))) == inclusion) {
                results.add(rowKeys.get(ii));
            }
        }
    }

    @Override
    Object keyIterator() {
        return counts.keySet().iterator();
    }

    @Override
    boolean exportKeys(@NotNull final ExportContext context, @NotNull final WritableChunk<Values>[] keyChunks) {
        // noinspection unchecked
        final IntIterator keys = ((KeyIteratorContext<IntIterator>) context).keys;
        final WritableFloatChunk<Values> typedKeys = keyChunks[0].asWritableFloatChunk();
        final int capacity = typedKeys.capacity();
        int exported = 0;
        while (exported < capacity && keys.hasNext()) {
            typedKeys.set(exported++, Float.intBitsToFloat(keys.nextInt()));
        }
        typedKeys.setSize(exported);
        return exported > 0;
    }
}
