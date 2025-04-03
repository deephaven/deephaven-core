//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

class S3WriteRequest {

    /**
     * The context for this request, used to allocate and release buffers.
     */
    private final S3WriteContext writeContext;

    /**
     * The buffer for this request
     */
    ByteBuffer buffer;

    S3WriteRequest(
            @NotNull final S3WriteContext writeContext,
            final int writePartSize) throws InterruptedException {
        this.writeContext = writeContext;
        this.buffer = writeContext.take();
        if (buffer.position() != 0) {
            throw new IllegalStateException("Buffer from pool has incorrect position, position = "
                    + buffer.position() + ", expected 0");
        }
        if (buffer.limit() != writePartSize) {
            throw new IllegalStateException("Buffer from pool has incorrect limit, limit = " + buffer.limit() +
                    ", expected " + writePartSize);
        }
    }

    final void releaseBuffer() {
        if (buffer == null) {
            throw new IllegalStateException("Buffer has already been released");
        }
        writeContext.give(buffer);
        buffer = null;
    }
}
