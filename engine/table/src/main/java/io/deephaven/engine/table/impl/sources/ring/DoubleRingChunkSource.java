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

import static io.deephaven.util.QueryConstants.NULL_DOUBLE;

final class DoubleRingChunkSource extends AbstractRingChunkSource<Double, double[], DoubleRingChunkSource> {
    public static RingColumnSource<Double> columnSource(int n) {
        return new RingColumnSource<>(double.class, new DoubleRingChunkSource(n), new DoubleRingChunkSource(n));
    }

    public DoubleRingChunkSource(int capacity) {
        super(double.class, capacity);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Double;
    }

    @Override
    void clear() {
        Arrays.fill(ring, NULL_DOUBLE);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx) {
        destination.asWritableDoubleChunk().set(destOffset, ring[ringIx]);
    }

    @Override
    Double get(long key) {
        return TypeUtils.box(getDouble(key));
    }

    @Override
    double getDouble(long key) {
        if (!containsKey(key)) {
            return NULL_DOUBLE;
        }
        return ring[keyToRingIndex(key)];
    }
}
