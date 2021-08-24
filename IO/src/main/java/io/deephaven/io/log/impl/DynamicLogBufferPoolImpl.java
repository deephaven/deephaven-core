/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.Function;
import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogBufferPool;

import java.nio.ByteBuffer;

public class DynamicLogBufferPoolImpl extends ThreadSafeLenientFixedSizePool<ByteBuffer>
    implements LogBufferPool {

    // public DynamicLogBufferPoolImpl(final int poolSize, final int byteBufferCapacity) {
    // super(poolSize,
    // new Function.Nullary<ByteBuffer>() {
    // public ByteBuffer call() { return ByteBuffer.allocate(byteBufferCapacity); }
    // }, null);
    // }

    public DynamicLogBufferPoolImpl(String name, final int poolSize, final int byteBufferCapacity) {
        super(name, poolSize,
            new Function.Nullary<ByteBuffer>() {
                public ByteBuffer call() {
                    return ByteBuffer.allocate(byteBufferCapacity);
                }
            }, null);
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
