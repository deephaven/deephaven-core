//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.IOException;
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

    protected static void readValidityBuffer(
            @NotNull final DataInput is,
            final int numValidityLongs,
            final long validityBufferLength,
            @NotNull final WritableLongChunk<Values> isValid,
            @NotNull final String DEBUG_NAME) throws IOException {
        // Read validity buffer:
        int jj = 0;
        for (; jj < Math.min(numValidityLongs, validityBufferLength / 8); ++jj) {
            isValid.set(jj, is.readLong());
        }
        final long valBufRead = jj * 8L;
        if (valBufRead < validityBufferLength) {
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBufferLength - valBufRead));
        }
        // we support short validity buffers
        for (; jj < numValidityLongs; ++jj) {
            isValid.set(jj, -1); // -1 is bit-wise representation of all ones
        }
    }
}
