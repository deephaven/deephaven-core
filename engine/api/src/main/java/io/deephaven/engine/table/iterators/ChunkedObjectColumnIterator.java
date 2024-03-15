//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.function.Consumer;

/**
 * {@link ObjectColumnIterator} implementation for {@link ChunkSource chunk sources} of {@link Object objects}.
 */
public final class ChunkedObjectColumnIterator<DATA_TYPE>
        extends ChunkedColumnIterator<DATA_TYPE, ObjectChunk<DATA_TYPE, ? extends Any>>
        implements ObjectColumnIterator<DATA_TYPE> {

    /**
     * Create a new ObjectColumnIterator.
     * 
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType() *
     *        chunk type} of {@link ChunkType#Object}
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The internal buffer size to use when fetching data
     * @param firstRowKey The first row key from {@code rowSequence} to iterate
     * @param length The total number of rows to iterate
     */
    public ChunkedObjectColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize,
            final long firstRowKey,
            final long length) {
        super(validateChunkType(chunkSource, ChunkType.Object), rowSequence, chunkSize, firstRowKey, length);
    }

    /**
     * Create a new ObjectColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType() *
     *        chunk type} of {@link ChunkType#Object}
     * @param rowSequence The {@link RowSequence} to iterate over
     */
    public ChunkedObjectColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(validateChunkType(chunkSource, ChunkType.Object), rowSequence, DEFAULT_CHUNK_SIZE,
                rowSequence.firstRowKey(), rowSequence.size());
    }

    @Override
    ObjectChunk<DATA_TYPE, ? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asObjectChunk().asTypedObjectChunk();
    }

    @Override
    public DATA_TYPE next() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public void forEachRemaining(@NotNull final Consumer<? super DATA_TYPE> action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(currentData.get(currentOffset++));
            }
        });
    }
}
