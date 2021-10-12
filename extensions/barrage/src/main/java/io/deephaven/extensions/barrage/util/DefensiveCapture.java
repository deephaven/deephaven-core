/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.util;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;

public class DefensiveCapture extends InputStream {
    private DefensiveDrainable in;
    private InputStream delegate;

    public DefensiveCapture(DefensiveDrainable in) {
        this.in = Objects.requireNonNull(in);
    }

    synchronized InputStream delegate() throws IOException {
        if (delegate != null) {
            return delegate;
        }
        final BarrageProtoUtil.ExposedByteArrayOutputStream out = new BarrageProtoUtil.ExposedByteArrayOutputStream();
        final int size = in.drainTo(out);
        in.close();
        in = null;
        return (delegate = new ByteArrayInputStream(out.peekBuffer(), 0, size));
    }

    @Override
    public int read() throws IOException {
        return delegate().read();
    }

    @Override
    public int read(@NotNull byte[] b) throws IOException {
        return delegate().read(b);
    }

    @Override
    public int read(@NotNull byte[] b, int off, int len) throws IOException {
        return delegate().read(b, off, len);
    }

    @Override
    public long skip(long n) throws IOException {
        return delegate().skip(n);
    }

    @Override
    public int available() throws IOException {
        return delegate().available();
    }

    @Override
    public void close() throws IOException {
        delegate().close();
    }
}
