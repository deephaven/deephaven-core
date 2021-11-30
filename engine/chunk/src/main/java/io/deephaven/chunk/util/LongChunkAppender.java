package io.deephaven.chunk.util;

import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.WritableLongChunk;
import org.jetbrains.annotations.NotNull;

import java.util.function.LongConsumer;

/**
 * {@link LongConsumer} that appends successive longs to a {@link WritableLongChunk} without mutating the destination's
 * size.
 */
public final class LongChunkAppender implements LongConsumer {

    private final WritableLongChunk<? extends Any> destination;

    private int nextPosition;

    /**
     * Construct a LongChunkAppender to append to the specified {@code destination} beginning at {@code offset}.
     *
     * @param destination The destination {@link WritableLongChunk}
     * @param offset The initial position to append to
     */
    public LongChunkAppender(@NotNull final WritableLongChunk<? extends Any> destination, final int offset) {
        this.destination = destination;
        nextPosition = offset;
    }

    /**
     * Construct a LongChunkAppender for the specified {@code destination} beginning at 0.
     *
     * @param destination The destination {@link WritableLongChunk}
     */
    public LongChunkAppender(@NotNull final WritableLongChunk<? extends Any> destination) {
        this(destination, 0);
    }

    @Override
    public final void accept(final long value) {
        destination.set(nextPosition++, value);
    }
}
