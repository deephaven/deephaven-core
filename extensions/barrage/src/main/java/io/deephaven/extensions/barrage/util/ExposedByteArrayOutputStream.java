//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import java.io.ByteArrayOutputStream;

public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {

    public ExposedByteArrayOutputStream() {}

    /**
     * @param size the initial size of the buffer; a stream that writes exactly this many bytes never grows it, so
     *        {@link #peekBuffer()} is then exactly the bytes written
     */
    public ExposedByteArrayOutputStream(final int size) {
        super(size);
    }

    public byte[] peekBuffer() {
        return buf;
    }
}
