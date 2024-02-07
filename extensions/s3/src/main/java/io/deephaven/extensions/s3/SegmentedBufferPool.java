package io.deephaven.extensions.s3;

import io.deephaven.util.datastructures.SegmentedSoftPool;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

final class SegmentedBufferPool implements BufferPool {

    private static final int POOL_SEGMENT_CAPACITY = 10;
    private final SegmentedSoftPool<ByteBuffer> pool;
    private final int bufferSize;

    /**
     * @param bufferSize Upper limit on size of buffers to be pooled
     */
    SegmentedBufferPool(final int bufferSize) {
        this.bufferSize = bufferSize;
        this.pool = new SegmentedSoftPool<>(
                POOL_SEGMENT_CAPACITY,
                () -> ByteBuffer.allocate(bufferSize),
                ByteBuffer::clear);
    }

    @Override
    public BufferHolder take(final int size) {
        if (size > bufferSize) {
            throw new IllegalArgumentException("Buffer size " + size + " is larger than pool size " + bufferSize);
        }
        return new SegmentedBufferHolder(pool);
    }

    private static final class SegmentedBufferHolder implements BufferHolder {

        private static final AtomicReferenceFieldUpdater<SegmentedBufferHolder, ByteBuffer> BUFFER_UPDATER =
                AtomicReferenceFieldUpdater.newUpdater(SegmentedBufferHolder.class, ByteBuffer.class, "buffer");

        private final SegmentedSoftPool<ByteBuffer> pool;
        private volatile ByteBuffer buffer;

        private SegmentedBufferHolder(@NotNull final SegmentedSoftPool<ByteBuffer> pool) {
            this.pool = pool;
            this.buffer = pool.take();
        }

        @Override
        public @Nullable ByteBuffer get() {
            return buffer;
        }

        @Override
        public void close() {
            final ByteBuffer localBuffer = buffer;
            if (localBuffer != null && BUFFER_UPDATER.compareAndSet(this, localBuffer, null)) {
                pool.give(localBuffer);
            }
        }
    }
}
