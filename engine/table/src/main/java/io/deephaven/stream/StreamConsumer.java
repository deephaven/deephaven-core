/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.stream;

import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.WritableChunk;
import org.jetbrains.annotations.NotNull;

import java.util.Collection;

/**
 * Chunk-oriented consumer for streams of data.
 */
public interface StreamConsumer extends StreamFailureConsumer {

    /**
     * Accept a batch of rows splayed across per-column {@link WritableChunk chunks} of {@link Values values}.
     *
     * <p>
     * Ownership of {@code data} passes to the consumer, which must be sure to
     * {@link io.deephaven.chunk.util.pools.PoolableChunk#close close} each chunk when it's no longer needed.
     *
     * <p>
     * Implementations will generally have a mechanism for determining the expected number and type of input chunks, but
     * this is not dictated at the interface level.
     *
     * @param data Per-column {@link WritableChunk chunks} of {@link Values values}. Must all have the same
     *        {@link WritableChunk#size() size}.
     */
    @SuppressWarnings("unchecked") // There's no actual possibility of heap-pollution, here.
    void accept(@NotNull WritableChunk<Values>... data);

    /**
     * Accept a collection of batches of rows splayed across per-column {@link WritableChunk chunks} of {@link Values
     * values}.
     *
     * <p>
     * Ownership of all the chunks contained within {@code data} passes to the consumer, which must be sure to
     * {@link io.deephaven.chunk.util.pools.PoolableChunk#close close} each chunk when it's no longer needed.
     *
     * <p>
     * Implementations will generally have a mechanism for determining the expected number and type of input chunks for
     * each element, but this is not dictated at the interface level.
     *
     * <p>
     * Implementations may provide more specific semantics about this method in comparison to repeated invocations of
     * {@link #accept(WritableChunk[])}.
     *
     * @param data A collection of per-column {@link WritableChunk chunks} of {@link Values values}. All chunks in each
     *        element must have the same {@link WritableChunk#size() size}, but different elements may have differing
     *        chunk sizes.
     */
    void accept(@NotNull Collection<WritableChunk<Values>[]> data);
}
