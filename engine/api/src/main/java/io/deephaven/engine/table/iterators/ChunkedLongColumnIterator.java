//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit ChunkedCharacterColumnIterator and run "./gradlew replicateColumnIterators" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.iterators;

// region ConsumerInterface
import java.util.function.LongConsumer;
// endregion ConsumerInterface
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import org.jetbrains.annotations.NotNull;

/**
 * Chunked {@link LongColumnIterator} implementation for {@link ChunkSource chunk sources} of primitive longs.
 */
public final class ChunkedLongColumnIterator
        extends ChunkedColumnIterator<Long, LongChunk<? extends Any>>
        implements LongColumnIterator {

    /**
     * Create a new ChunkedLongColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Long}
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The buffer size to use when fetching data
     * @param firstRowKey The first row key from {@code rowSequence} to iterate
     * @param length The total number of rows to iterate
     */
    public ChunkedLongColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize,
            final long firstRowKey,
            final long length) {
        super(validateChunkType(chunkSource, ChunkType.Long), rowSequence, chunkSize, firstRowKey, length);
    }

    /**
     * Create a new ChunkedLongColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from; must have {@link ChunkSource#getChunkType()
     *        chunk type} of {@link ChunkType#Long}
     * @param rowSequence The {@link RowSequence} to iterate over
     */
    public ChunkedLongColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        this(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE, rowSequence.firstRowKey(), rowSequence.size());
    }

    @Override
    LongChunk<? extends Any> castChunk(@NotNull final Chunk<? extends Any> chunk) {
        return chunk.asLongChunk();
    }

    @Override
    public long nextLong() {
        maybeAdvance();
        return currentData.get(currentOffset++);
    }

    @Override
    public void forEachRemaining(@NotNull final LongConsumer action) {
        consumeRemainingByChunks(() -> {
            final int currentSize = currentData.size();
            while (currentOffset < currentSize) {
                action.accept(currentData.get(currentOffset++));
            }
        });
    }
}
