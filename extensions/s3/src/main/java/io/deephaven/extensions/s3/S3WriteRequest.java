//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.reference.CleanupReference;
import io.deephaven.util.reference.CleanupReferenceProcessor;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

class S3WriteRequest {

    /**
     * A {@link Runnable} that releases a {@link ByteBuffer} back to a provided {@link S3WriteContext} when executed.
     */
    private static class BufferReleaser implements Runnable {
        private final S3WriteContext writeContext;
        private final ByteBuffer buffer;

        BufferReleaser(@NotNull final S3WriteContext writeContext, @NotNull final ByteBuffer buffer) {
            this.writeContext = writeContext;
            this.buffer = buffer;
        }

        @Override
        public void run() {
            try {
                writeContext.returnBuffer(buffer);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UncheckedDeephavenException("Thread interrupted while returning buffer to the pool", e);
            }
        }
    }

    /**
     * The buffer for this request
     */
    ByteBuffer buffer;

    /**
     * The cleanup reference for this request, used to release the buffer when the request is no longer reachable
     */
    private final CleanupReference<?> cleanup;

    S3WriteRequest(
            @NotNull final S3WriteContext writeContext,
            final int writePartSize) {
        final ByteBuffer allocatedBuffer;
        try {
            allocatedBuffer = writeContext.getBuffer();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedDeephavenException("Thread interrupted while allocating buffer from the pool", e);
        }
        if (allocatedBuffer.position() != 0) {
            throw new IllegalStateException("Buffer from pool has incorrect position, position = "
                    + allocatedBuffer.position() + ", expected 0");
        }
        if (allocatedBuffer.limit() != writePartSize) {
            throw new IllegalStateException("Buffer from pool has incorrect limit, limit = " + allocatedBuffer.limit() +
                    ", expected " + writePartSize);
        }

        // Note: It's important that we don't capture "this" in the cleanup logic, as this will create a strong
        // reference to this object and prevent it from being garbage collected.
        this.cleanup = CleanupReferenceProcessor.getDefault().registerPhantom(this,
                new BufferReleaser(writeContext, allocatedBuffer));
        this.buffer = allocatedBuffer;
    }

    final void releaseBuffer() {
        cleanup.cleanup();
        buffer = null;
    }
}
