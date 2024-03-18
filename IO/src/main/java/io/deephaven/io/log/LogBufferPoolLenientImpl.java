//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log;

import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;

import java.nio.ByteBuffer;

class LogBufferPoolLenientImpl extends ThreadSafeLenientFixedSizePool<ByteBuffer> implements LogBufferPool {

    private final int bufferSize;

    public LogBufferPoolLenientImpl(int bufferCount, final int bufferSize) {
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
