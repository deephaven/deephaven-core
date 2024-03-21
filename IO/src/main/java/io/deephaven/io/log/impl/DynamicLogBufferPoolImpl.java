//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log.impl;

import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogBufferPool;

import java.nio.ByteBuffer;

public class DynamicLogBufferPoolImpl extends ThreadSafeLenientFixedSizePool<ByteBuffer> implements LogBufferPool {

    public DynamicLogBufferPoolImpl(String name, final int poolSize, final int byteBufferCapacity) {
        super(name, poolSize, () -> ByteBuffer.allocate(byteBufferCapacity), null);
    }

    @Override
    public ByteBuffer take(int minSize) {
        ByteBuffer buffer = take();
        if (buffer.capacity() < minSize) {
            // leave the old buffer to rot!
            buffer = ByteBuffer.allocate(minSize);
        }
        return buffer;
    }
}
