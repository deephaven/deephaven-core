//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * The {@code SingleElementListHeaderReader} is a specialized {@link BaseChunkReader} used to handle singleton
 * list-wrapped columns in Apache Arrow record batches. This implementation ensures compatibility with Apache Arrow's
 * requirement that top-level column vectors must have the same number of rows, even when some columns in a record batch
 * contain varying numbers of modified rows.
 * <p>
 * This reader works by skipping the validity and offset buffers for the singleton list and delegating the reading of
 * the underlying data to a {@link ChunkReader} for the wrapped component type. This approach ensures that Arrow
 * payloads remain compatible with official Arrow implementations while supporting Deephaven's semantics for record
 * batches with varying column modifications.
 * <p>
 * This is used only when {@link BarrageOptions#columnsAsList()} is enabled.
 *
 * @param <READ_CHUNK_TYPE> The type of chunk being read, extending {@link WritableChunk} with {@link Values}.
 */
public class SingleElementListHeaderReader<READ_CHUNK_TYPE extends WritableChunk<Values>>
        extends BaseChunkReader<READ_CHUNK_TYPE> {
    private static final String DEBUG_NAME = "SingleElementListHeaderReader";

    private final ChunkReader<READ_CHUNK_TYPE> componentReader;

    public SingleElementListHeaderReader(
            final ChunkReader<READ_CHUNK_TYPE> componentReader) {
        this.componentReader = componentReader;
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
        final long validityBufferLength = bufferInfoIter.nextLong();
        final long offsetsBufferLength = bufferInfoIter.nextLong();

        if (nodeInfo.numElements == 0) {
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBufferLength + offsetsBufferLength));
            return componentReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
        }

        // skip validity buffer:
        int jj = 0;
        if (validityBufferLength > 0) {
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBufferLength));
        }

        // skip offsets:
        if (offsetsBufferLength > 0) {
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBufferLength));
        }

        return componentReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
    }
}
