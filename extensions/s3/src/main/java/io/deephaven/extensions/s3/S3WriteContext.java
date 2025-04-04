//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Context object used to store buffer pool for write requests.
 */
final class S3WriteContext implements SeekableChannelsProvider.WriteContext {
    private static final Logger log = LoggerFactory.getLogger(S3WriteContext.class);

    /**
     * The number of bytes in each part of the write request, used to allocate buffers.
     */
    private final int writePartSize;

    /**
     * The number of concurrent write parts that can be in-flight at any given time, used to limit the total number of
     * buffers that are allocated.
     */
    private final int numConcurrentWriteParts;

    /**
     * Used to pool buffers for writing to S3.
     */
    private final ArrayBlockingQueue<ByteBuffer> bufferQueue;

    /**
     * The number of buffers that have been allocated
     */
    private volatile int createdCount;

    private static final AtomicIntegerFieldUpdater<S3WriteContext> CREATED_COUNT_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(S3WriteContext.class, "createdCount");

    S3WriteContext(@NotNull final S3Instructions instructions) {
        this.writePartSize = instructions.writePartSize();
        this.numConcurrentWriteParts = instructions.numConcurrentWriteParts();
        this.bufferQueue = new ArrayBlockingQueue<>(numConcurrentWriteParts);
        this.createdCount = 0;
        if (log.isDebugEnabled()) {
            log.debug().append("Creating output stream context").endl();
        }
    }

    /**
     * Get a buffer from the pool. This method will block until a buffer is available.
     */
    ByteBuffer take() throws InterruptedException {
        // Try to get a buffer without blocking
        final ByteBuffer buffer = bufferQueue.poll();
        if (buffer != null) {
            return buffer;
        }

        // Allocate a new buffer if we haven't reached the max size yet
        int current;
        while ((current = createdCount) < numConcurrentWriteParts) {
            if (CREATED_COUNT_UPDATER.compareAndSet(this, current, current + 1)) {
                return ByteBuffer.allocate(writePartSize);
            }
        }

        // Wait for a buffer to be available
        return bufferQueue.take();
    }

    /**
     * Return a buffer to the pool, clearing its contents. This method will block until the buffer is accepted.
     */
    void give(@NotNull final ByteBuffer buffer) {
        buffer.clear();
        if (!bufferQueue.offer(buffer)) {
            throw new IllegalStateException("Failed to return buffer to queue, queue size expected to be  + " +
                    numConcurrentWriteParts + ", remaining capacity = " + bufferQueue.remainingCapacity());
        }
    }

    @Override
    public void close() {
        bufferQueue.clear();
    }
}
