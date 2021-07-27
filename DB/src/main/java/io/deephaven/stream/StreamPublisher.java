package io.deephaven.stream;

import io.deephaven.db.v2.sources.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

/**
 * Chunk-oriented producer for streams of data.
 */
@FunctionalInterface
public interface StreamPublisher {

    /**
     * Flush any accumulated data in this publisher to a {@link StreamConsumer consumer}, by invoking its
     * {@link StreamConsumer#accept(WritableChunk[]) accept} method.
     *
     * @param consumer The consumer
     */
    void flush(@NotNull StreamConsumer consumer);
}
