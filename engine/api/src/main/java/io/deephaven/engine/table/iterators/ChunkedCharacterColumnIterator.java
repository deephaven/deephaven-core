//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

// region ConsumerInterface
import io.deephaven.engine.primitive.function.CharConsumer;
// endregion ConsumerInterface
import io.deephaven.chunk.CharChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

/**
 * Chunked {@link CharacterColumnIterator} implementation for {@link ChunkSource chunk sources} of primitive chars.
 */
public final class ChunkedCharacterColumnIterator
        extends ChunkedColumnIterator<Character, CharChunk<? extends Any>>
        implements CharacterColumnIterator {

    /**
     * Create a new ChunkedCharacterColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Char}
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The buffer size to use when fetching data
     * @param firstRowKey The first row key from {@code rowSequence} to iterate
     * @param length The total number of rows to iterate
     */
    public ChunkedCharacterColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize,
            final long firstRowKey,
            final long length) {
        super(validateChunkType(chunkSource, ChunkType.Char), rowSequence, chunkSize, firstRowKey, length);
    }

    /**
     * Create a new ChunkedCharacterColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Char}
     * @param rowSequence The {@link RowSequence} to iterate over
     */
    public ChunkedCharacterColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE, rowSequence.firstRowKey(), rowSequence.size());
    }

    @Override
    CharChunk<? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asCharChunk();
    }

    @Override
    public char nextChar() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public void forEachRemaining(@NotNull final CharConsumer action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(currentData.get(currentOffset++));
            }
        });
    }
}
