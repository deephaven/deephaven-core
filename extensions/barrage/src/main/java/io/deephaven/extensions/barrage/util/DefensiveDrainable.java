/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.util;

import io.grpc.Drainable;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A defensive {@link Drainable} implementation that supports the non-read methods of {@link InputStream}. Callers
 * should use {@link #drainTo(OutputStream)} when applicable. If handing off to external code that needs a real
 * {@link InputStream}, use {@link #capture()}.
 */
public abstract class DefensiveDrainable extends InputStream implements Drainable {

    private static UnsupportedOperationException unsupportedUsagePattern() {
        return new UnsupportedOperationException(
                "DefensiveDrainable callers should use #drainTo when applicable,"
                        + " or #capture if handing off to external code that needs a real InputStream.");
    }

    @Override
    public final int read(@NotNull byte[] b) throws IOException {
        throw unsupportedUsagePattern();
    }

    @Override
    public final int read(@NotNull byte[] b, int off, int len) throws IOException {
        throw unsupportedUsagePattern();
    }

    @Override
    public final long skip(long n) throws IOException {
        throw unsupportedUsagePattern();
    }

    @Override
    public final int read() throws IOException {
        throw unsupportedUsagePattern();
    }

    /**
     * A defensive drainable needs to override available. As opposed to the generic {@link InputStream}, this method
     * must return the exact amount available.
     *
     * @return the exact amount available
     * @throws IOException if an I/O exception occurs
     */
    @Override
    public int available() throws IOException {
        throw new UnsupportedOperationException("Implementation should override available()");
    }

    /**
     * Captures {@code this} drainable as a real {@link InputStream}.
     *
     * @return a real input stream
     */
    public final InputStream capture() {
        return new DefensiveCapture(this);
    }
}
