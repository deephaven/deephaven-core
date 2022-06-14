/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.io.log;

import io.deephaven.base.pool.Pool;

import java.nio.ByteBuffer;

public interface LogBufferPool extends Pool<ByteBuffer> {
    ByteBuffer take(int minSize);
}
