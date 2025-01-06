//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import java.io.ByteArrayOutputStream;

public class ExposedByteArrayOutputStream extends ByteArrayOutputStream {
    public byte[] peekBuffer() {
        return buf;
    }
}
