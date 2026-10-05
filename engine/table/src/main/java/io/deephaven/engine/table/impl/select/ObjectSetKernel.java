//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SingleColumnSetKernel} for Object keys, which match by {@link Object#equals(Object)}.
 */
final class ObjectSetKernel extends SingleColumnSetKernel {

    /**
     * The number of set rows holding each key. Must be a fastutil open hash map, not a {@code java.util.HashMap}: see
     * {@link SharedSetKernel#kernel()} for the behavior we rely on when it is concurrently modified.
     */
    private final Object2IntOpenHashMap<Object> counts = new Object2IntOpenHashMap<>();

    ObjectSetKernel(@NotNull final ColumnSource<?> keySource) {
        super(keySource);
    }

    @Override
    long size() {
        return counts.size();
    }

    @Override
    void addKeys(@NotNull final Chunk<? extends Values> keys) {
        final ObjectChunk<?, ? extends Values> typedKeys = keys.asObjectChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final int sizeBefore = counts.size();
            final int oldCount = counts.addTo(typedKeys.get(ii), 1);
            if (oldCount == 0) {
                // A key at zero is still in the map until finishRemove, so only an insertion grows it.
                if (counts.size() == sizeBefore) {
                    ++revivedKeys;
                } else {
                    ++insertedKeys;
                }
            } else if (oldCount == Integer.MAX_VALUE) {
                throw new UnsupportedOperationException("More than Integer.MAX_VALUE set rows hold one key");
            }
        }
    }

    @Override
    void removeKeys(@NotNull final Chunk<? extends Values> keys) {
        final ObjectChunk<?, ? extends Values> typedKeys = keys.asObjectChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            final int oldCount = counts.addTo(typedKeys.get(ii), -1);
            Assert.gtZero(oldCount, "oldCount");
            if (oldCount == 1) {
                ++emptiedKeys;
            }
        }
    }

    @Override
    void dropEmptied(@NotNull final Chunk<? extends Values> keys) {
        final ObjectChunk<?, ? extends Values> typedKeys = keys.asObjectChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            counts.remove(typedKeys.get(ii), 0);
        }
    }

    @Override
    void match(
            @NotNull final Chunk<Values> keys,
            @NotNull final LongChunk<OrderedRowKeys> rowKeys,
            @NotNull final WritableLongChunk<OrderedRowKeys> results,
            final boolean inclusion) {
        final ObjectChunk<?, Values> typedKeys = keys.asObjectChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            if (counts.containsKey(typedKeys.get(ii)) == inclusion) {
                results.add(rowKeys.get(ii));
            }
        }
    }

    @Override
    CloseableIterator<Object> iterator() {
        return closeable(counts.keySet().iterator());
    }
}
