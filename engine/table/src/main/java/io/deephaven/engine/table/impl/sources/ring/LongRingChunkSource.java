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

import static io.deephaven.util.QueryConstants.NULL_LONG;

final class LongRingChunkSource extends AbstractRingChunkSource<Long, long[], LongRingChunkSource> {
    public static RingColumnSource<Long> columnSource(int n) {
        return new RingColumnSource<>(long.class, new LongRingChunkSource(n), new LongRingChunkSource(n));
    }

    public LongRingChunkSource(int capacity) {
        super(long.class, capacity);
    }

    @Override
    public ChunkType getChunkType() {
        return ChunkType.Long;
    }

    @Override
    void clear() {
        Arrays.fill(ring, NULL_LONG);
    }

    @Override
    void fillKey(@NotNull WritableChunk<? super Values> destination, int destOffset, int ringIx) {
        destination.asWritableLongChunk().set(destOffset, ring[ringIx]);
    }

    @Override
    Long get(long key) {
        return TypeUtils.box(getLong(key));
    }

    @Override
    long getLong(long key) {
        if (!containsKey(key)) {
            return NULL_LONG;
        }
        return ring[keyToRingIndex(key)];
    }
}
