//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class NullChunkReader<READ_CHUNK_TYPE extends WritableChunk<Values>> extends BaseChunkReader<READ_CHUNK_TYPE> {

    private final ChunkType resultType;

    public NullChunkReader(Class<?> destType) {
        this.resultType = getChunkTypeFor(destType);
    }

    @Override
    public READ_CHUNK_TYPE readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        // null nodes have no buffers

        final WritableChunk<Values> chunk = castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                resultType::makeWritableChunk,
                c -> c);

        chunk.fillWithNullValue(0, nodeInfo.numElements);

        // noinspection unchecked
        return (READ_CHUNK_TYPE) chunk;
    }
}
