//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.extensions.barrage.util.DefensiveDrainable;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

public class DrainableByteArrayInputStream extends DefensiveDrainable {

    private byte[] buf;
    private final int offset;
    private final int length;

    public DrainableByteArrayInputStream(final byte[] buf, final int offset, final int length) {
        this.buf = Objects.requireNonNull(buf);
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int available() {
        if (buf == null) {
            return 0;
        }
        return length;
    }

    @Override
    public int drainTo(final OutputStream outputStream) throws IOException {
        if (buf != null) {
            try {
                outputStream.write(buf, offset, length);
            } finally {
                buf = null;
            }
            return length;
        }
        return 0;
    }
}
