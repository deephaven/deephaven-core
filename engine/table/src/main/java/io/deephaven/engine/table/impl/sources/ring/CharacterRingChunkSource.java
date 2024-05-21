//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static io.deephaven.util.QueryConstants.NULL_CHAR;

final class CharacterRingChunkSource extends AbstractRingChunkSource<Character, char[], CharacterRingChunkSource> {
    public static RingColumnSource<Character> columnSource(int n) {
        return new RingColumnSource<>(char.class, new CharacterRingChunkSource(n), new CharacterRingChunkSource(n));
    }

    public CharacterRingChunkSource(int capacity) {
        super(new char[capacity]);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Char;
    }

    @Override
    Character get(long key) {
        return TypeUtils.box(getChar(key));
    }

    @Override
    char getChar(long key) {
        if (key == RowSet.NULL_ROW_KEY) {
            return NULL_CHAR;
        }
        if (STRICT_KEYS && !containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("Invalid key %d. available=[%d, %d]", key, firstKey(), lastKey()));
        }
        return ring[keyToRingIndex(key)];
    }

    @Override
    Filler filler(@NotNull WritableChunk<? super Values> destination) {
        return new FillerImpl(destination.asWritableCharChunk());
    }

    private class FillerImpl extends Filler {
        private final WritableCharChunk<? super Values> dest;

        FillerImpl(WritableCharChunk<? super Values> dest) {
            this.dest = Objects.requireNonNull(dest);
        }

        @Override
        protected void copyFromRing(int srcRingIx, int destOffset) {
            dest.set(destOffset, ring[srcRingIx]);
        }

        @Override
        protected void copyFromRing(int srcRingIx, int destOffset, int size) {
            dest.copyFromTypedArray(ring, srcRingIx, destOffset, size);
        }

        @Override
        protected void setSize(int size) {
            dest.setSize(size);
        }
    }
}
