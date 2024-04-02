//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.base.reference.PooledObjectReference;
import io.deephaven.util.datastructures.SegmentedSoftPool;
import org.jetbrains.annotations.NotNull;

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

    public PooledObjectReference<ByteBuffer> take(final int size) {
        if (size > bufferSize) {
            throw new IllegalArgumentException("Buffer size " + size + " is larger than pool size " + bufferSize);
        }
        return new BufferReference(pool.take());
    }

    private void give(ByteBuffer buffer) {
        pool.give(buffer);
    }

    final class BufferReference extends PooledObjectReference<ByteBuffer> {

        BufferReference(@NotNull final ByteBuffer buffer) {
            super(buffer);
        }

        @Override
        protected void returnReferentToPool(@NotNull ByteBuffer referent) {
            give(referent);
        }
    }
}
