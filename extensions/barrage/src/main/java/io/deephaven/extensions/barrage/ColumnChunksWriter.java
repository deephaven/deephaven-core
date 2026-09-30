//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.DictionaryWriterRegistry;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
            // We must latch the size of the chunk here. Making a context may transform the chunk to a new type and
            // release the original chunk. Releasing resets the size to the chunk capacity and will break the row
            // offset calculation.
            final int chunkSize = valuesChunk.size();
            this.contexts[i] = writer.makeContext(valuesChunk, rowOffset);
            rowOffset += chunkSize;
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

    /**
     * Like {@link #empty(BarrageOptions)}, but threads {@code dictionaryRegistry} down to a dictionary-encoded writer
     * nested within this column's writer (e.g. the {@code values} child of a run-end-encoded column), so it still
     * registers its state -- and emits its initial isDelta=false DictionaryBatch -- even when this column's very first
     * batch carries no rows.
     */
    public ChunkWriter.DrainableColumn empty(
            @NotNull final BarrageOptions options,
            @Nullable final DictionaryWriterRegistry dictionaryRegistry) throws IOException {
        return writer.getEmptyInputStream(options, dictionaryRegistry);
    }

    @Override
    public void close() {
        for (final ChunkWriter.Context context : contexts) {
            context.close();
        }
    }
}
