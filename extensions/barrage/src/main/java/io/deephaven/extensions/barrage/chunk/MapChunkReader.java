package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class MapChunkReader<T> extends BaseChunkReader<WritableObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "MapChunkReader";

    private final ChunkReader<? extends WritableChunk<Values>> keyReader;
    private final ChunkReader<? extends WritableChunk<Values>> valueReader;

    public MapChunkReader(
            final ChunkReader<? extends WritableChunk<Values>> keyReader,
            final ChunkReader<? extends WritableChunk<Values>> valueReader) {
        this.keyReader = keyReader;
        this.valueReader = valueReader;
    }

    @Override
    public WritableObjectChunk<T, Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBufferLength = bufferInfoIter.nextLong();

        if (nodeInfo.numElements == 0) {
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBufferLength));
            try (final WritableChunk<Values> ignored =
                         keyReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                 final WritableChunk<Values> ignored2 =
                         valueReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                return WritableObjectChunk.makeWritableChunk(nodeInfo.numElements);
            }
        }

        final WritableObjectChunk<T, Values> chunk;
        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        final int numOffsets = nodeInfo.numElements;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs)) {
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

            try (final WritableChunk<Values> keys =
                         keyReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                 final WritableChunk<Values> values =
                         valueReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                chunk = castOrCreateChunk(
                        outChunk,
                        Math.max(totalRows, keys.size()),
                        WritableObjectChunk::makeWritableChunk,
                        WritableChunk::asWritableObjectChunk);

                long nextValid = 0;
                for (int ii = 0; ii < nodeInfo.numElements;) {
                    if ((ii % 64) == 0) {
                        nextValid = ~isValid.get(ii / 64);
                    }
                    if ((nextValid & 0x1) == 0x1) {
                        chunk.set(outOffset + ii, null);
                    } else {
                        chunk.set(outOffset + ii, )
                    }
                    final int numToSkip = Math.min(
                            Long.numberOfTrailingZeros(nextValid & (~0x1)),
                            64 - (ii % 64));

                    nextValid >>= numToSkip;
                    ii += numToSkip;
                }
            }
        }

        return chunk;
    }
}
