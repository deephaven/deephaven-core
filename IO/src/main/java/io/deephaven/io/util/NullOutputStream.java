//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.util;

import java.io.IOException;
import java.io.OutputStream;

public class NullOutputStream extends OutputStream {

    public void write(byte b[]) throws IOException {
        // empty
    }

    public void write(byte b[], int off, int len) throws IOException {
        // empty
    }

    public void flush() throws IOException {
        // empty
    }

    public void close() throws IOException {
        // empty
    }

    public void write(int b) throws IOException {
        // empty
    }
}
