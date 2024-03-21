//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log;

import io.deephaven.base.pool.Pool;
import io.deephaven.io.log.impl.LogBufferPoolImpl;

import java.nio.ByteBuffer;

public interface LogBufferPool extends Pool<ByteBuffer> {

    /**
     * Creates a log buffer pool with at least {@code bufferCount} items. If all of the buffers are in use, additional
     * buffers will be created on-demand.
     *
     * @param bufferCount the buffer count
     * @param bufferSize the buffer size
     * @return the log buffer pool
     */
    static LogBufferPool of(int bufferCount, int bufferSize) {
        return new LogBufferPoolLenientImpl(bufferCount, bufferSize);
    }

    /**
     * Creates a log buffer pool with at least {@code bufferCount} items. If all of the buffers are in use, additional
     * takes will spin or block until one becomes available.
     *
     * @param bufferCount the buffer count
     * @param bufferSize the buffer size
     * @return the log buffer pool
     */
    static LogBufferPool ofStrict(int bufferCount, int bufferSize) {
        return new LogBufferPoolImpl(bufferCount, bufferSize);
    }

    ByteBuffer take(int minSize);
}
