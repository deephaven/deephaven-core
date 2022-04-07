package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

final class CharacterRingChunkSource extends AbstractRingChunkSource<Character, char[], CharacterRingChunkSource> {
    public static RingColumnSource<Character> columnSource(int n) {
        return new RingColumnSource<>(char.class, new CharacterRingChunkSource(n), new CharacterRingChunkSource(n));
    }

    public CharacterRingChunkSource(int capacity) {
        super(char.class, capacity);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Char;
    }

    @Override
    void clear() {
        Arrays.fill(ring, NULL_CHAR);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx) {
        destination.asWritableCharChunk().set(destOffset, ring[ringIx]);
    }

    @Override
    Character get(long key) {
        return TypeUtils.box(getChar(key));
    }

    @Override
    char getChar(long key) {
        if (!containsKey(key)) {
            return NULL_CHAR;
        }
        return ring[keyToRingIndex(key)];
    }
}
