/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import java.nio.ByteBuffer;
import java.io.IOException;

public class NullByteBufferSink implements ByteBufferSink {

    public ByteBuffer acceptBuffer(ByteBuffer b, int need) throws IOException {
        b.clear();
        if (b.remaining() < need) {
            b = ByteBuffer.allocate(Math.max(b.remaining() * 2, need));
        }
        return b;
    }

    public void close(ByteBuffer b) throws IOException {
        // empty
    }
}
