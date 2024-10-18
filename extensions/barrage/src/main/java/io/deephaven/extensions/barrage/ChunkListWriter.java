//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.List;

public class ChunkListWriter<SourceChunkType extends Chunk<Values>> implements SafeCloseable {
    private final ChunkWriter<SourceChunkType> writer;
    private final ChunkWriter.Context<SourceChunkType>[] contexts;

    public ChunkListWriter(
            final ChunkWriter<SourceChunkType> writer,
            final List<SourceChunkType> chunks) {
        this.writer = writer;

        // noinspection unchecked
        this.contexts = (ChunkWriter.Context<SourceChunkType>[]) new ChunkWriter.Context[chunks.size()];

        long rowOffset = 0;
        for (int i = 0; i < chunks.size(); ++i) {
            final SourceChunkType valuesChunk = chunks.get(i);
            this.contexts[i] = writer.makeContext(valuesChunk, rowOffset);
            rowOffset += valuesChunk.size();
        }
    }

    public ChunkWriter<SourceChunkType> writer() {
        return writer;
    }

    public ChunkWriter.Context<SourceChunkType>[] chunks() {
        return contexts;
    }

    public ChunkWriter.DrainableColumn empty(@NotNull final BarrageOptions options) throws IOException {
        return writer.getEmptyInputStream(options);
    }

    @Override
    public void close() {
        for (final ChunkWriter.Context<SourceChunkType> context : contexts) {
            context.decrementReferenceCount();
        }
    }
}
