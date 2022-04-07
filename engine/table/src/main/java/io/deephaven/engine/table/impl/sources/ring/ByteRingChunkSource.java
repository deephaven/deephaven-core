/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterRingChunkSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_BYTE;

final class ByteRingChunkSource extends AbstractRingChunkSource<Byte, byte[], ByteRingChunkSource> {
    public static RingColumnSource<Byte> columnSource(int n) {
        return new RingColumnSource<>(byte.class, new ByteRingChunkSource(n), new ByteRingChunkSource(n));
    }

    public ByteRingChunkSource(int capacity) {
        super(byte.class, capacity);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Byte;
    }

    @Override
    void clear() {
        Arrays.fill(ring, NULL_BYTE);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx) {
        destination.asWritableByteChunk().set(destOffset, ring[ringIx]);
    }

    @Override
    Byte get(long key) {
        return TypeUtils.box(getByte(key));
    }

    @Override
    byte getByte(long key) {
        if (!containsKey(key)) {
            return NULL_BYTE;
        }
        return ring[keyToRingIndex(key)];
    }
}
