//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.json.jackson;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Objects;

final class ByteBufferInputStream extends InputStream {

    public static InputStream of(ByteBuffer buffer) {
        if (buffer.hasArray()) {
            return new ByteArrayInputStream(buffer.array(), buffer.arrayOffset() + buffer.position(),
                    buffer.remaining());
        } else {
            return new ByteBufferInputStream(buffer.asReadOnlyBuffer());
        }
    }

    private final ByteBuffer buffer;

    private ByteBufferInputStream(ByteBuffer buf) {
        buffer = Objects.requireNonNull(buf);
    }

    @Override
    public int available() {
        return buffer.remaining();
    }

    @Override
    public int read() {
        return buffer.hasRemaining() ? buffer.get() & 0xFF : -1;
    }

    @Override
    public int read(byte[] bytes, int off, int len) {
        if (!buffer.hasRemaining()) {
            return -1;
        }
        len = Math.min(len, buffer.remaining());
        buffer.get(bytes, off, len);
        return len;
    }

    @Override
    public long skip(long n) {
        n = Math.min(n, buffer.remaining());
        buffer.position(buffer.position() + (int) n);
        return n;
    }
}
