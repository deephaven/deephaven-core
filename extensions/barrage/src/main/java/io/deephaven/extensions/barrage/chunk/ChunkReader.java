//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.util.annotations.FinalDefault;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * Consumes Flight/Barrage streams and transforms them into WritableChunks.
 */
public interface ChunkReader<ReadChunkType extends WritableChunk<Values>> {

    /**
     * Supports creation of {@link ChunkReader} instances to use when processing a flight stream. JVM implementations
     * for client and server should probably use {@link DefaultChunkReaderFactory#INSTANCE}.
     */
    interface Factory {

        /**
         * Returns a {@link ChunkReader} for the specified arguments.
         *
         * @param typeInfo the type of data to read into a chunk
         * @param options options for reading the stream
         * @return a ChunkReader based on the given options, factory, and type to read
         */
        <T extends WritableChunk<Values>> ChunkReader<T> newReader(
                @NotNull BarrageTypeInfo typeInfo,
                @NotNull BarrageOptions options);
    }

    /**
     * Reads the given DataInput to extract the next Arrow buffer as a Deephaven Chunk.
     *
     * @param fieldNodeIter iterator to read fields from the stream
     * @param bufferInfoIter iterator to read buffers from the stream
     * @param is input stream containing buffers to be read
     * @return a Chunk containing the data from the stream
     * @throws IOException if an error occurred while reading the stream
     */
    @FinalDefault
    default ReadChunkType readChunk(
            @NotNull Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull DataInput is) throws IOException {
        return readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
    }

    /**
     * Reads the given DataInput to extract the next Arrow buffer as a Deephaven Chunk.
     * 
     * @param fieldNodeIter iterator to read fields from the stream
     * @param bufferInfoIter iterator to read buffers from the stream
     * @param is input stream containing buffers to be read
     * @param outChunk chunk to write to
     * @param outOffset offset within the outChunk to begin writing
     * @param totalRows total rows to write to the outChunk
     * @return a Chunk containing the data from the stream
     * @throws IOException if an error occurred while reading the stream
     */
    ReadChunkType readChunk(
            @NotNull Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull DataInput is,
            @Nullable WritableChunk<Values> outChunk,
            int outOffset,
            int totalRows) throws IOException;
}
