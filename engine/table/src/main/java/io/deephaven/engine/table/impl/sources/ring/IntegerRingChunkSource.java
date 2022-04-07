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

import static io.deephaven.util.QueryConstants.NULL_INT;

final class IntegerRingChunkSource extends AbstractRingChunkSource<Integer, int[], IntegerRingChunkSource> {
    public static RingColumnSource<Integer> columnSource(int n) {
        return new RingColumnSource<>(int.class, new IntegerRingChunkSource(n), new IntegerRingChunkSource(n));
    }

    public IntegerRingChunkSource(int capacity) {
        super(int.class, capacity);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Int;
    }

    @Override
    void clear() {
        Arrays.fill(ring, NULL_INT);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx) {
        destination.asWritableIntChunk().set(destOffset, ring[ringIx]);
    }

    @Override
    Integer get(long key) {
        return TypeUtils.box(getInt(key));
    }

    @Override
    int getInt(long key) {
        if (!containsKey(key)) {
            return NULL_INT;
        }
        return ring[keyToRingIndex(key)];
    }
}
