//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log.impl;

import java.nio.ByteBuffer;

import io.deephaven.base.pool.ThreadSafeFixedSizePool;
import io.deephaven.io.log.LogBufferPool;

public class LogBufferPoolImpl extends ThreadSafeFixedSizePool<ByteBuffer> implements LogBufferPool {

    private final int bufferSize;

    public LogBufferPoolImpl(int bufferCount, final int bufferSize) {
        super(bufferCount, () -> ByteBuffer.allocate(bufferSize), ByteBuffer::clear);
        this.bufferSize = bufferSize;

    }

    @Override
    public ByteBuffer take(int minSize) {
        if (minSize > bufferSize) {
            throw new UnsupportedOperationException("Not Implemented Yet");
        }
        return take();
    }
}
