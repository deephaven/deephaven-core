/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.locations;

import io.deephaven.db.exceptions.QueryCancellationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.naming.InterruptedNamingException;
import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileLockInterruptionException;

/**
 * Exception thrown by various sub-systems for data access.
 */
public class TableDataException extends RuntimeException {

    private static final long serialVersionUID = 8599205102694321064L;

    /**
     * Whether this exception was caused by an interrupt.
     */
    private final boolean wasInterrupted;

    public TableDataException(@NotNull final String message, @Nullable final Throwable cause) {
        super(message, cause);
        wasInterrupted = cause != null && ( // This null check isn't strictly necessary, but
                                            // enhances clarity and perceived performance.
        (cause instanceof TableDataException && ((TableDataException) cause).wasInterrupted)
            || cause instanceof InterruptedException
            || cause instanceof ClosedByInterruptException
            || cause instanceof FileLockInterruptionException
            || cause instanceof InterruptedIOException
            || cause instanceof InterruptedNamingException
            || cause instanceof QueryCancellationException);
    }

    public TableDataException(@NotNull final String message) {
        this(message, null);
    }

    /**
     * @return Whether this exception was caused by an interrupt
     */
    public boolean wasInterrupted() {
        return wasInterrupted;
    }
}
