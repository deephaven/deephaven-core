//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharacterRingChunkSource and run "./gradlew replicateRingChunkSources" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources.ring;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.type.TypeUtils;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

final class DoubleRingChunkSource extends AbstractRingChunkSource<Double, double[], DoubleRingChunkSource> {
    public static RingColumnSource<Double> columnSource(int n) {
        return new RingColumnSource<>(double.class, new DoubleRingChunkSource(n), new DoubleRingChunkSource(n));
    }

    public DoubleRingChunkSource(int capacity) {
        super(new double[capacity]);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Double;
    }

    @Override
    Double get(long key) {
        return TypeUtils.box(getDouble(key));
    }

    @Override
    double getDouble(long key) {
        if (key == RowSet.NULL_ROW_KEY) {
            return NULL_DOUBLE;
        }
        if (STRICT_KEYS && !containsKey(key)) {
            throw new IllegalArgumentException(
                    String.format("Invalid key %d. available=[%d, %d]", key, firstKey(), lastKey()));
        }
        return ring[keyToRingIndex(key)];
    }

    @Override
    Filler filler(@NotNull WritableChunk<? super Values> destination) {
        return new FillerImpl(destination.asWritableDoubleChunk());
    }

    private class FillerImpl extends Filler {
        private final WritableDoubleChunk<? super Values> dest;

        FillerImpl(WritableDoubleChunk<? super Values> dest) {
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
