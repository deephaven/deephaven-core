//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharSetKernel and run "./gradlew replicateSetKernel" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.IntChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.util.type.TypeUtils;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntIterator;
import org.jetbrains.annotations.NotNull;

/**
 * A {@link SingleColumnSetKernel} for int keys.
 */
final class IntSetKernel extends SingleColumnSetKernel {

    /**
     * The number of set rows holding each key. Must be a fastutil open hash map: see {@link SharedSetKernel#kernel()}
     * for the behavior we rely on when it is concurrently modified.
     */
    private final Int2IntOpenHashMap counts = new Int2IntOpenHashMap();

    IntSetKernel(@NotNull final ColumnSource<?> keySource) {
        super(keySource);
    }

    @Override
    long size() {
        return counts.size();
    }

    @Override
    void addKeys(@NotNull final Chunk<? extends Values> keys) {
        final IntChunk<? extends Values> typedKeys = keys.asIntChunk();
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
        final IntChunk<? extends Values> typedKeys = keys.asIntChunk();
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
        final IntChunk<? extends Values> typedKeys = keys.asIntChunk();
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
        final IntChunk<Values> typedKeys = keys.asIntChunk();
        final int keysSize = typedKeys.size();
        for (int ii = 0; ii < keysSize; ++ii) {
            if (counts.containsKey(typedKeys.get(ii)) == inclusion) {
                results.add(rowKeys.get(ii));
            }
        }
    }

    @Override
    CloseableIterator<Object> iterator() {
        final IntIterator keys = counts.keySet().iterator();
        return new CloseableIterator<>() {
            @Override
            public boolean hasNext() {
                return keys.hasNext();
            }

            @Override
            public Object next() {
                return TypeUtils.box(keys.nextInt());
            }
        };
    }
}
