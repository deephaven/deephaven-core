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
     *
     * @param fieldNodeIter
     * @param bufferInfoIter
     * @param is
     * @param outChunk
     * @param outOffset
     * @param totalRows
     * @return
     */
    WritableChunk<Values> read(final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            final PrimitiveIterator.OfLong bufferInfoIter,
            final DataInput is,
            final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException;
}
