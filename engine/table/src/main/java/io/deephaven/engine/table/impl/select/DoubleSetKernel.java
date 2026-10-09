//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatSetKernel and run "./gradlew replicateSetKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.DoubleChunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.chunkfilter.DoubleChunkMatchFilterFactory;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SingleColumnSetKernel} for double keys, held as their bits with every NaN alike and -0.0 as 0.0, so that keys
 * match as they do for {@code ==} in the query language.
 */
final class DoubleSetKernel extends SingleColumnSetKernel {

    /**
     * The number of set rows holding each key's bits. Must be a fastutil open hash map: see
     * {@link SharedSetKernel#kernel()} for the behavior we rely on when it is concurrently modified.
     */
    private final Long2LongOpenHashMap counts = new Long2LongOpenHashMap();

    DoubleSetKernel(@NotNull final ColumnSource<?> keySource) {
        super(keySource);
    }

    @Override
    long size() {
        return counts.size();
    }

    @Override
    void addKeys(@NotNull final Chunk<? extends Values> keys) {
        final DoubleChunk<? extends Values> typedKeys = keys.asDoubleChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final int sizeBefore = counts.size();
            final long oldCount = counts.addTo(DoubleChunkMatchFilterFactory.getBits(typedKeys.get(ii)), 1);
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
        final DoubleChunk<? extends Values> typedKeys = keys.asDoubleChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final long oldCount = counts.addTo(DoubleChunkMatchFilterFactory.getBits(typedKeys.get(ii)), -1);
            Assert.gtZero(oldCount, "oldCount");
            if (oldCount == 1) {
                ++emptiedKeys;
            }
        }
    }

    @Override
    void dropEmptied(@NotNull final Chunk<? extends Values> keys) {
        final DoubleChunk<? extends Values> typedKeys = keys.asDoubleChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            counts.remove(DoubleChunkMatchFilterFactory.getBits(typedKeys.get(ii)), 0L);
        }
    }

    @Override
    protected void matchValues(
            @NotNull final Chunk<Values>[] keyChunks,
            @NotNull final LongChunk<OrderedRowKeys> rowKeys,
            @NotNull final WritableLongChunk<OrderedRowKeys> results,
            final boolean inclusion) {
        final DoubleChunk<Values> typedKeys = keyChunks[0].asDoubleChunk();
        final int keysSize = typedKeys.size();
        results.setSize(0);
        for (int ii = 0; ii < keysSize; ++ii) {
            if (counts.containsKey(DoubleChunkMatchFilterFactory.getBits(typedKeys.get(ii))) == inclusion) {
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
        final LongIterator keys = ((KeyIteratorContext<LongIterator>) context).keys;
        final WritableDoubleChunk<Values> typedKeys = keyChunks[0].asWritableDoubleChunk();
        final int capacity = typedKeys.capacity();
        int exported = 0;
        while (exported < capacity && keys.hasNext()) {
            typedKeys.set(exported++, Double.longBitsToDouble(keys.nextLong()));
        }
        typedKeys.setSize(exported);
        return exported > 0;
    }
}
