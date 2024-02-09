package io.deephaven.extensions.s3;

import io.deephaven.util.datastructures.SegmentedSoftPool;

import java.nio.ByteBuffer;

final class BufferPool {

    private static final int POOL_SEGMENT_CAPACITY = 10;
    private final SegmentedSoftPool<ByteBuffer> pool;
    private final int bufferSize;

    /**
     * @param bufferSize Upper limit on size of buffers to be pooled
     */
    BufferPool(final int bufferSize) {
        this.bufferSize = bufferSize;
        this.pool = new SegmentedSoftPool<>(
                POOL_SEGMENT_CAPACITY,
                () -> ByteBuffer.allocate(bufferSize),
                ByteBuffer::clear);
    }

    public ByteBuffer take(final int size) {
        if (size > bufferSize) {
            throw new IllegalArgumentException("Buffer size " + size + " is larger than pool size " + bufferSize);
        }
        return pool.take();
    }

    public void give(ByteBuffer buffer) {
        pool.give(buffer);
    }
}
