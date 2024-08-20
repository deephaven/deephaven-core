//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterRingChunkSource and run "./gradlew replicateRingChunkSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static io.deephaven.util.QueryConstants.NULL_SHORT;

final class ShortRingChunkSource extends AbstractRingChunkSource<Short, short[], ShortRingChunkSource> {
    public static RingColumnSource<Short> columnSource(int n) {
        return new RingColumnSource<>(short.class, new ShortRingChunkSource(n), new ShortRingChunkSource(n));
    }

    public ShortRingChunkSource(int capacity) {
        super(new short[capacity]);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Short;
    }

    @Override
    Short get(long key) {
        return TypeUtils.box(getShort(key));
    }

    @Override
    short getShort(long key) {
        if (key == RowSet.NULL_ROW_KEY) {
            return NULL_SHORT;
        }
        if (STRICT_KEYS && !containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("Invalid key %d. available=[%d, %d]", key, firstKey(), lastKey()));
        }
        return ring[keyToRingIndex(key)];
    }

    @Override
    Filler filler(@NotNull WritableChunk<? super Values> destination) {
        return new FillerImpl(destination.asWritableShortChunk());
    }

    private class FillerImpl extends Filler {
        private final WritableShortChunk<? super Values> dest;

        FillerImpl(WritableShortChunk<? super Values> dest) {
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
