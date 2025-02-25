//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

public class ColumnChunksWriter<SOURCE_CHUNK_TYPE extends Chunk<Values>> implements SafeCloseable {
    private final ChunkWriter<SOURCE_CHUNK_TYPE> writer;
    private final ChunkWriter.Context[] contexts;

    public ColumnChunksWriter(
            final ChunkWriter<SOURCE_CHUNK_TYPE> writer,
            final List<SOURCE_CHUNK_TYPE> chunks) {
        this.writer = writer;

        this.contexts = new ChunkWriter.Context[chunks.size()];

        long rowOffset = 0;
        for (int i = 0; i < chunks.size(); ++i) {
            final SOURCE_CHUNK_TYPE valuesChunk = chunks.get(i);
            this.contexts[i] = writer.makeContext(valuesChunk, rowOffset);
            rowOffset += valuesChunk.size();
        }
    }

    public ChunkWriter<SOURCE_CHUNK_TYPE> writer() {
        return writer;
    }

    public ChunkWriter.Context[] chunks() {
        return contexts;
    }

    public ChunkWriter.DrainableColumn empty(@NotNull final BarrageOptions options) throws IOException {
        return writer.getEmptyInputStream(options);
    }

    @Override
    public void close() {
        for (final ChunkWriter.Context context : contexts) {
            context.close();
        }
    }
}
