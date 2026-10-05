//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.Nullable;

import javax.naming.InterruptedNamingException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileLockInterruptionException;

/**
 * {@link UncheckedDeephavenException} used when an action is cancelled or interrupted.
 */
public class CancellationException extends UncheckedDeephavenException {

    public CancellationException(String message) {
        super(message);
    }

    public CancellationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Whether {@code failure}, or any throwable in its cause chain, reports that the work was cancelled or its thread
     * was interrupted. Code that recovers from a failure by falling back to other work uses this to let a cancellation
     * escape rather than carry on, since the throwable that reports it is often wrapped by the time it is caught.
     *
     * @param failure the throwable to examine, or {@code null}
     * @return whether {@code failure} or one of its causes is a cancellation or an interruption
     */
    public static boolean isCancellation(@Nullable final Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof CancellationException
                    || cause instanceof java.util.concurrent.CancellationException
                    || cause instanceof InterruptedException
                    || cause instanceof InterruptedIOException
                    || cause instanceof ClosedByInterruptException
                    || cause instanceof FileLockInterruptionException
                    || cause instanceof InterruptedNamingException) {
                return true;
            }
        }
        return false;
    }
}
