//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.pool.Pool;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

class S3WriteRequest {

    private Pool<ByteBuffer> pool;

    /**
     * The buffer for this request
     */
    ByteBuffer buffer;

    S3WriteRequest(@NotNull final Pool<ByteBuffer> pool, final int writePartSize) {
        this.pool = pool;
        buffer = pool.take();
        if (buffer.position() != 0) {
            throw new IllegalStateException("Buffer from pool has incorrect position, position = " + buffer.position() +
                    ", expected 0");
        }
        if (buffer.limit() != writePartSize) {
            throw new IllegalStateException("Buffer from pool has incorrect limit, limit = " + buffer.limit() +
                    ", expected " + writePartSize);
        }

        // Register this request with the cleanup reference processor so that we can release the buffer (if not already
        // released) when it is garbage collected.
        CleanupReferenceProcessor.getDefault().registerPhantom(this, this::releaseBuffer);
    }

    final void releaseBuffer() {
        if (buffer != null) {
            pool.give(buffer);
            buffer = null;
        }
    }
}
