//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

import java.util.function.Function;
import java.util.function.IntFunction;

public abstract class BaseChunkReader<READ_CHUNK_TYPE extends WritableChunk<Values>>
        implements ChunkReader<READ_CHUNK_TYPE> {

    protected static <T extends WritableChunk<Values>> T castOrCreateChunk(
            final WritableChunk<Values> outChunk,
            final int numRows,
            final IntFunction<T> chunkFactory,
            final Function<WritableChunk<Values>, T> castFunction) {
        if (outChunk != null) {
            return castFunction.apply(outChunk);
        }
        final T newChunk = chunkFactory.apply(numRows);
        newChunk.setSize(numRows);
        return newChunk;
    }

    public static ChunkType getChunkTypeFor(final Class<?> dest) {
        if (dest == boolean.class || dest == Boolean.class) {
            // Note: Internally booleans are passed around as bytes, but the wire format is packed bits.
            return ChunkType.Byte;
        } else if (dest != null && !dest.isPrimitive()) {
            return ChunkType.Object;
        }
        return ChunkType.fromElementType(dest);
    }
}
