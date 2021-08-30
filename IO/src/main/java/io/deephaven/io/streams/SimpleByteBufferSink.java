/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.streams;

import java.nio.ByteBuffer;
import java.io.IOException;

public class SimpleByteBufferSink implements CurrentByteBufferSink {

    private ByteBuffer currentBuffer;
    private final boolean direct;

    public SimpleByteBufferSink(ByteBuffer b, boolean direct) {
        this.currentBuffer = b;
        this.direct = direct;
    }

    public SimpleByteBufferSink(ByteBuffer b) {
        this(b, false);
    }

    @Override
    public ByteBuffer getBuffer() {
        return currentBuffer;
    }

    @Override
    public ByteBuffer acceptBuffer(ByteBuffer b, int need) {
        if (b.remaining() < need) {
            b.flip();
            ByteBuffer b2 = direct ? ByteBuffer.allocateDirect(Math.max(b.capacity() * 2, b.remaining() + need))
                    : ByteBuffer.allocate(Math.max(b.capacity() * 2, b.remaining() + need));
            b2.put(b);
            currentBuffer = b = b2;
        }
        return b;
    }

    @Override
    public void close(ByteBuffer b) throws IOException {
        // empty
    }
}
