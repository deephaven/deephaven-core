package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

// todo: this is not being auto-generated ATM
final class ObjectRingChunkSource<T> extends AbstractRingChunkSource<T, Object[], ObjectRingChunkSource<T>> {
    public static <T> RingColumnSource<T> columnSource(Class<T> type, int capacity) {
        return new RingColumnSource<>(type, new ObjectRingChunkSource<>(type, capacity), new ObjectRingChunkSource<>(type, capacity));
    }

    public ObjectRingChunkSource(Class<T> type, int capacity) {
        super(type, capacity);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Object;
    }

    @Override
    void clear() {
        // todo: when do we call this?
        Arrays.fill(ring, null);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx) {
        destination.asWritableObjectChunk().set(destOffset, ring[ringIx]);
    }

    @Override
    T get(long key) {
        if (!containsKey(key)) {
            return null;
        }
        //noinspection unchecked
        return (T)ring[keyToRingIndex(key)];
    }
}
