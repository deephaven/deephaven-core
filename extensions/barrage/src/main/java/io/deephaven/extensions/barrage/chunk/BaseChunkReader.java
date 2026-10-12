//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;

import java.io.DataInput;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.IntFunction;

public abstract class BaseChunkReader<READ_CHUNK_TYPE extends WritableChunk<Values>>
        implements ChunkReader<READ_CHUNK_TYPE> {

    /**
     * Upper bound (in bytes) on a single bulk payload read shared by the fixed-width primitive readers. Wide columns
     * are decoded in windows of this size rather than materializing the entire payload at once. Configure once here
     * (rather than per type) via {@code BaseChunkReader.bulkReadBufferBytes}.
     */
    protected static final int BULK_READ_BUFFER_BYTES = Configuration.getInstance()
            .getIntegerForClassWithDefault(BaseChunkReader.class, "bulkReadBufferBytes", 4096);

    /** Number of {@code int}s decoded per bulk-read window (see {@link #BULK_READ_BUFFER_BYTES}). */
    private static final int BULK_READ_INTS = Math.max(1, BULK_READ_BUFFER_BYTES / Integer.BYTES);

    /**
     * Read {@code numElements} little-endian {@code int}s — an Arrow offsets or lengths buffer — into {@code dest},
     * pulling the payload in bounded windows into a reused buffer rather than making one {@link DataInput} call, i.e.
     * four individual byte reads, per value.
     *
     * @param is the input to read from
     * @param dest the chunk to populate, starting at position zero
     * @param numElements the number of values to read
     */
    protected static void readIntBuffer(
            @NotNull final DataInput is,
            @NotNull final WritableIntChunk<?> dest,
            final int numElements) throws IOException {
        final byte[] buffer = new byte[Math.min(numElements, BULK_READ_INTS) * Integer.BYTES];
        for (int ei = 0; ei < numElements;) {
            final int n = Math.min(BULK_READ_INTS, numElements - ei);
            is.readFully(buffer, 0, n * Integer.BYTES);
            for (int jj = 0; jj < n; ++jj) {
                dest.set(ei + jj, LittleEndianCodec.getInt(buffer, jj * Integer.BYTES));
            }
            ei += n;
        }
    }

    @FunctionalInterface
    public interface ChunkTransformer<READ_CHUNK_TYPE extends Chunk<Values>, DEST_CHUNK_TYPE extends WritableChunk<Values>> {
        void transform(READ_CHUNK_TYPE source, DEST_CHUNK_TYPE dest, int destOffset);
    }

    public static <ATTR extends Any, T extends WritableChunk<ATTR>> T castOrCreateChunk(
            final WritableChunk<ATTR> outChunk,
            final int outOffset,
            final int numRows,
            final IntFunction<T> chunkFactory,
            final Function<WritableChunk<ATTR>, T> castFunction) {
        if (outChunk != null) {
            T castChunk = castFunction.apply(outChunk);
            if (castChunk.size() < outOffset + numRows) {
                castChunk.setSize(outOffset + numRows);
            }
            return castChunk;
        }
        // note this returns an appropriately sized chunk with capacity >= size
        return chunkFactory.apply(numRows);
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
