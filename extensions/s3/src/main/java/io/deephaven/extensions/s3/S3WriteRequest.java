//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.pool.Pool;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

class S3WriteRequest {

    static S3WriteRequest create(
            @NotNull final Pool<ByteBuffer> pool,
            final int writePartSize) {
        final ByteBuffer buffer = pool.take();
        if (buffer.position() != 0) {
            throw new IllegalStateException("Buffer from pool has incorrect position, position = " + buffer.position() +
                    ", expected 0");
        }
        if (buffer.limit() != writePartSize) {
            throw new IllegalStateException("Buffer from pool has incorrect limit, limit = " + buffer.limit() +
                    ", expected " + writePartSize);
        }

        final AtomicBoolean released = new AtomicBoolean(false);
        final Runnable cleanup = () -> {
            if (released.compareAndSet(false, true)) {
                pool.give(buffer);
            }
        };
        final S3WriteRequest request = new S3WriteRequest(buffer, cleanup);

        // Register this request with the cleanup reference processor so that we can release the buffer (if not already
        // released) when the request becomes phantom reachable.
        CleanupReferenceProcessor.getDefault().registerPhantom(request, cleanup);
        return request;
    }

    /**
     * The buffer for this request
     */
    ByteBuffer buffer;

    /**
     * The runnable to clean up the buffer when this request is no longer reachable.
     */
    final Runnable cleanup;

    private S3WriteRequest(
            @NotNull final ByteBuffer buffer,
            @NotNull final Runnable cleanup) {
        this.buffer = buffer;
        this.cleanup = cleanup;
    }

    final void releaseBuffer() {
        if (buffer != null) {
            cleanup.run();
            buffer = null;
        }
    }
}
