package io.deephaven.stream;

import io.deephaven.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

/**
 * Chunk-oriented producer for streams of data.
 */
public interface StreamPublisher {

    /**
     * <p>
     * Register a {@link StreamConsumer consumer} whose {@link StreamConsumer#accept(WritableChunk[]) accept} method
     * will be used when sufficient data is accumulated or on {@link #flush()}.
     *
     * <p>
     * {@code consumer} must typically be primed to expect the same {@link io.deephaven.chunk.ChunkType chunk types}
     * that this produces, in the same order.
     *
     * @param consumer The consumer
     * @throws IllegalStateException If a consumer has already been registered for this producer
     */
    void register(@NotNull StreamConsumer consumer);

    /**
     * Flush any accumulated data in this publisher to the {@link StreamConsumer consumer}, by invoking its
     * {@link StreamConsumer#accept(WritableChunk[]) accept} method.
     */
    void flush();
}
