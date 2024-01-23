package io.deephaven.extensions.s3;

import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;

public interface BufferPool {

    interface BufferHolder extends AutoCloseable {

        /**
         * @return The buffer if available, else {@code null}
         */
        @Nullable
        ByteBuffer get();

        /**
         * Return the held buffer to its pool, and cause subsequent calls to {@link #get()} to return {@code null}
         */
        void close();
    }

    /**
     * Returns a {@link BufferHolder} that will hold a buffer of at least the requested size.
     */
    BufferHolder take(int size);
}
