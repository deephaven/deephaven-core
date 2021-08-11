package io.deephaven.stream;

import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

/**
 * Chunk-oriented consumer for streams of data.
 */
@FunctionalInterface
public interface StreamConsumer {

    /**
     * <p>Accept a batch of rows splayed across per-column {@link WritableChunk chunks} of {@link Values values}.
     *
     * <p>Ownership of {@code data} passes to the consumer, which must be sure to
     * {@link io.deephaven.db.v2.sources.chunk.util.pools.PoolableChunk#close close} each chunk when it's no longer
     * needed.
     *
     * <p>Implementations will generally have a mechanism for determining the expected number and type of input
     * chunks, but this is not dictated at the interface level.
     *
     * @param data Per-column {@link WritableChunk chunks} of {@link Values values}.
     *             Must all have the same {@link WritableChunk#size() size}.
     */
    @SuppressWarnings("unchecked") // There's no actual possibility of heap-pollution, here.
    void accept(@NotNull WritableChunk<Values>... data);
}
