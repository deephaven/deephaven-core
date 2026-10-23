//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import org.junit.Test;

import javax.naming.InterruptedNamingException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileLockInterruptionException;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link CancellationException#isCancellation(Throwable)}.
 */
public class CancellationExceptionTest {

    private static List<Throwable> cancellations() {
        return List.of(
                new CancellationException("cancelled"),
                new java.util.concurrent.CancellationException("cancelled"),
                new InterruptedException(),
                new InterruptedIOException(),
                new ClosedByInterruptException(),
                new FileLockInterruptionException(),
                new InterruptedNamingException());
    }

    @Test
    public void testDirectCancellation() {
        for (final Throwable cancellation : cancellations()) {
            assertTrue(cancellation.toString(), CancellationException.isCancellation(cancellation));
        }
    }

    /**
     * A cancellation is recognized however deeply it is wrapped, as when the query compiler wraps an interrupt.
     */
    @Test
    public void testWrappedCancellation() {
        for (final Throwable cancellation : cancellations()) {
            final Throwable wrapped = new RuntimeException("outer",
                    new UncheckedDeephavenException("Interrupted while compiling class", cancellation));
            assertTrue(cancellation.toString(), CancellationException.isCancellation(wrapped));
        }
    }

    @Test
    public void testNotCancellation() {
        assertFalse(CancellationException.isCancellation(null));
        assertFalse(CancellationException.isCancellation(new IllegalStateException("failed")));
        assertFalse(CancellationException.isCancellation(
                new UncheckedDeephavenException("read failed", new UncheckedIOException(new IOException("io")))));
    }
}
