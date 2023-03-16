/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.iterators;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link ColumnIterator} implementation for {@link ChunkSource chunk sources} of objects.
 */
public final class ObjectColumnIterator<TYPE> extends ColumnIterator<TYPE, ObjectChunk<TYPE, ? extends Any>> {

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
    public ObjectColumnIterator(
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
    public ObjectColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(validateChunkType(chunkSource, ChunkType.Object), rowSequence, DEFAULT_CHUNK_SIZE,
                rowSequence.firstRowKey(), rowSequence.size());
    }

    @Override
    ObjectChunk<TYPE, ? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asObjectChunk().asTypedObjectChunk();
    }

    @Override
    public TYPE next() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public void forEachRemaining(@NotNull final Consumer<? super TYPE> action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(currentData.get(currentOffset++));
            }
        });
    }

    /**
     * Create a {@link Stream} over the remaining elements of this ObjectColumnIterator. The result <em>must</em> be
     * {@link java.util.stream.BaseStream#close() closed} in order to ensure resources are released. A
     * try-with-resources block is strongly encouraged.
     *
     * @return A {@link Stream} over the remaining contents of this iterator. Must be {@link Stream#close() closed}.
     */
    @Override
    public Stream<TYPE> stream() {
        return StreamSupport.stream(
                Spliterators.spliterator(
                        this,
                        remaining(),
                        Spliterator.IMMUTABLE | Spliterator.ORDERED),
                false)
                .onClose(this::close);
    }
}
