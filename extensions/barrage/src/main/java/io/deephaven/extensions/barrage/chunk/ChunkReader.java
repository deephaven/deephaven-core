//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * Consumes Flight/Barrage streams and transforms them into WritableChunks.
 */
public interface ChunkReader {
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
    WritableChunk<Values> readChunk(final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException;
}
