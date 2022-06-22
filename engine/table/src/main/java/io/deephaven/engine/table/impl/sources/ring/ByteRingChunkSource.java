/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharacterRingChunkSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

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
    Byte get(long key) {
        return TypeUtils.box(getByte(key));
    }

    @Override
    byte getByte(long key) {
        if (key == RowSet.NULL_ROW_KEY) {
            return NULL_BYTE;
        }
        if (STRICT_KEYS && !containsKey(key)) {
            throw new IllegalArgumentException(String.format("Invalid key %d. available=[%d, %d]", key, firstKey(), lastKey()));
        }
        return ring[keyToRingIndex(key)];
    }

    @Override
    Filler filler(@NotNull WritableChunk<? super Values> destination) {
        return new FillerImpl(destination.asWritableByteChunk());
    }

    private class FillerImpl extends Filler {
        private final WritableByteChunk<? super Values> dest;

        FillerImpl(WritableByteChunk<? super Values> dest) {
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
