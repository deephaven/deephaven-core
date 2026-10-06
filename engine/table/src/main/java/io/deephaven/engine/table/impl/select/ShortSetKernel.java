//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetKernel and run "./gradlew replicateSetKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ShortChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import it.unimi.dsi.fastutil.shorts.Short2LongOpenHashMap;
import it.unimi.dsi.fastutil.shorts.ShortIterator;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SingleColumnSetKernel} for short keys.
 */
final class ShortSetKernel extends SingleColumnSetKernel {

    /**
     * The number of set rows holding each key. Must be a fastutil open hash map: see {@link SharedSetKernel#kernel()}
     * for the behavior we rely on when it is concurrently modified.
     */
    private final Short2LongOpenHashMap counts = new Short2LongOpenHashMap();

    ShortSetKernel(@NotNull final ColumnSource<?> keySource) {
        super(keySource);
    }

    @Override
    long size() {
        return counts.size();
    }

    @Override
    void addKeys(@NotNull final Chunk<? extends Values> keys) {
        final ShortChunk<? extends Values> typedKeys = keys.asShortChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final int sizeBefore = counts.size();
            final long oldCount = counts.addTo(typedKeys.get(ii), 1);
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
        final ShortChunk<? extends Values> typedKeys = keys.asShortChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final long oldCount = counts.addTo(typedKeys.get(ii), -1);
            Assert.gtZero(oldCount, "oldCount");
            if (oldCount == 1) {
                ++emptiedKeys;
            }
        }
    }

    @Override
    void dropEmptied(@NotNull final Chunk<? extends Values> keys) {
        final ShortChunk<? extends Values> typedKeys = keys.asShortChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            counts.remove(typedKeys.get(ii), 0L);
        }
    }

    @Override
    void match(
            @NotNull final Chunk<Values> keys,
            @NotNull final LongChunk<OrderedRowKeys> rowKeys,
            @NotNull final WritableLongChunk<OrderedRowKeys> results,
            final boolean inclusion) {
        final ShortChunk<Values> typedKeys = keys.asShortChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            if (counts.containsKey(typedKeys.get(ii)) == inclusion) {
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
        final ShortIterator keys = ((KeyIteratorContext<ShortIterator>) context).keys;
        final WritableShortChunk<Values> typedKeys = keyChunks[0].asWritableShortChunk();
        final int capacity = typedKeys.capacity();
        int exported = 0;
        while (exported < capacity && keys.hasNext()) {
            typedKeys.set(exported++, keys.nextShort());
        }
        typedKeys.setSize(exported);
        return exported > 0;
    }
}
