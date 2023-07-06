/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stream;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

/**
 * Chunk-oriented consumer for streams of data.
 */
public interface StreamConsumer extends StreamFailureConsumer {

    /**
     * Register a {@link StreamPublisher publisher} whose {@link StreamPublisher#flush() flush} method may be used to
     * ensure available data has been passed to this StreamConsumer.
     * <p>
     * {@code publisher} must typically be primed to produce the same {@link io.deephaven.chunk.ChunkType chunk types}
     * that this consumes, in the same order.
     * <p>
     * Implementations should ensure that {@code publisher} is {@link StreamPublisher#shutdown() shutdown} when it is
     * no longer needed.
     *
     * @param publisher The publisher
     * @throws IllegalStateException If a publisher has already been registered for this consumer
     */
    void register(@NotNull StreamPublisher publisher);

    /**
     * Get the {@link ChunkType} that will be used (returned by {@link #getChunksToFill()}) for the specified column
     * index.
     * <p>
     * This is primarily useful for compatibility validation.
     *
     * @param columnIndex The column index
     * @return The chunk type
     */
    ChunkType chunkType(int columnIndex);

    /**
     * Get an array of per-column {@link WritableChunk chunks} to fill and pass to {@link #accept(WritableChunk[])}.
     * <p>
     * Ownership of the returned chunks passes to the caller.
     *
     * @return An array of per-column {@link WritableChunk chunks} to fill
     */
    WritableChunk<Values>[] getChunksToFill();

    /**
     * Get an array of per-column {@link WritableChunk chunks} to fill and pass to {@link #accept(WritableChunk[])}.
     * <p>
     * Ownership of the returned chunks passes to the caller.
     *
     * @param capacity The minimum capacity required by the caller
     * @return An array of per-column {@link WritableChunk chunks} to fill
     */
    WritableChunk<Values>[] getChunksToFill(int capacity);

    /**
     * Accept a batch of rows splayed across per-column {@link WritableChunk chunks} of {@link Values values}.
     * <p>
     * Ownership of {@code data} passes to the consumer, which must be sure to
     * {@link io.deephaven.chunk.util.pools.PoolableChunk#close close} each chunk when it's no longer needed.
     * <p>
     * Implementations will generally have a mechanism for determining the expected number and type of input chunks, but
     * this is not dictated at the interface level.
     *
     * @param data Per-column {@link WritableChunk chunks} of {@link Values values}. Must all have the same
     *        {@link WritableChunk#size() size}.
     */
    @SuppressWarnings("unchecked") // There's no actual possibility of heap-pollution, here.
    void accept(@NotNull WritableChunk<Values>... data);
}
