/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

// Note: this is not being auto-generated ATM
final class ObjectRingChunkSource<T> extends AbstractRingChunkSource<T, Object[], ObjectRingChunkSource<T>> {
    public static <T> RingColumnSource<T> columnSource(Class<T> type, int capacity) {
        return new RingColumnSource<>(type, new ObjectRingChunkSource<>(type, capacity), new ObjectRingChunkSource<>(type, capacity));
    }

    public static <T> RingColumnSource<T> columnSource(Class<T> type, Class<?> componentType, int capacity) {
        return new RingColumnSource<>(type, componentType, new ObjectRingChunkSource<>(type, capacity), new ObjectRingChunkSource<>(type, capacity));
    }

    public ObjectRingChunkSource(Class<T> type, int capacity) {
        super(type, capacity);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    T get(long key) {
        if (key == RowSet.NULL_ROW_KEY) {
            return null;
        }
        if (STRICT_KEYS && !containsKey(key)) {
            throw new IllegalArgumentException(String.format("Invalid key %d. available=[%d, %d]", key, firstKey(), lastKey()));
        }
        //noinspection unchecked
        return (T)ring[keyToRingIndex(key)];
    }

    @Override
    Filler filler(@NotNull WritableChunk<? super Values> destination) {
        return new FillerImpl(destination.asWritableObjectChunk());
    }

    private class FillerImpl extends Filler {
        private final WritableObjectChunk<T, ? super Values> dest;

        FillerImpl(WritableObjectChunk<T, ? super Values> dest) {
            this.dest = Objects.requireNonNull(dest);
        }

        @Override
        protected void copyFromRing(int srcRingIx, int destOffset) {
            //noinspection unchecked
            dest.set(destOffset, (T)ring[srcRingIx]);
        }

        @Override
        protected void copyFromRing(int srcRingIx, int destOffset, int size) {
            //noinspection unchecked
            dest.copyFromTypedArray((T[])ring, srcRingIx, destOffset, size);
        }

        @Override
        protected void setSize(int size) {
            dest.setSize(size);
        }
    }
}
