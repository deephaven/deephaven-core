//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.util.annotations.FinalDefault;
import org.apache.arrow.flatbuf.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * The {@code ChunkReader} interface provides a mechanism for consuming Flight/Barrage streams and transforming them
 * into {@link WritableChunk} instances for further processing. It facilitates efficient deserialization of columnar
 * data, supporting various data types and logical structures. This interface is part of the Deephaven Barrage
 * extensions for handling streamed data ingestion.
 *
 * @param <READ_CHUNK_TYPE> The type of chunk being read, extending {@link WritableChunk} with {@link Values}.
 */
public interface ChunkReader<READ_CHUNK_TYPE extends WritableChunk<Values>> {

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
                @NotNull BarrageTypeInfo<Field> typeInfo,
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
    default READ_CHUNK_TYPE readChunk(
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
    READ_CHUNK_TYPE readChunk(
            @NotNull Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull DataInput is,
            @Nullable WritableChunk<Values> outChunk,
            int outOffset,
            int totalRows) throws IOException;
}
