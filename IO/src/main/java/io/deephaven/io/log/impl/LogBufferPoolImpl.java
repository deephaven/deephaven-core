/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import java.nio.ByteBuffer;

import io.deephaven.base.Function;
import io.deephaven.base.pool.ThreadSafeFixedSizePool;
import io.deephaven.io.log.LogBufferPool;
import org.apache.log4j.Logger;

public class LogBufferPoolImpl extends ThreadSafeFixedSizePool<ByteBuffer> implements LogBufferPool {

    private final int bufferSize;

    public LogBufferPoolImpl(int bufferCount, final int bufferSize, Logger log, String logPfx) {
        super(bufferCount, new Function.Nullary<ByteBuffer>() {
            public ByteBuffer call() {
                return ByteBuffer.allocate(bufferSize);
            }
        }, null, log, logPfx);
        this.bufferSize = bufferSize;

    }

    public LogBufferPoolImpl(int bufferCount, final int bufferSize) {
        this(bufferCount, bufferSize, null, null);
    }

    @Override
    public ByteBuffer take(int minSize) {
        if (minSize > bufferSize) {
            throw new UnsupportedOperationException("Not Implemented Yet");
        }
        return take();
    }
}
