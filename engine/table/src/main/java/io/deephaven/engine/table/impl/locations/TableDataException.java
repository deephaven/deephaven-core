//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.exceptions.CancellationException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Exception thrown by various sub-systems for data access.
 */
public class TableDataException extends RuntimeException {

    private static final long serialVersionUID = 8599205102694321064L;

    /**
     * Whether this exception was caused by an interrupt or a cancellation, anywhere in its cause chain.
     */
    private final boolean wasInterrupted;

    public TableDataException(@NotNull final String message, @Nullable final Throwable cause) {
        super(message, cause);
        wasInterrupted = CancellationException.isCancellation(cause);
    }

    public TableDataException(@NotNull final String message) {
        this(message, null);
    }

    /**
     * @return Whether this exception was caused by an interrupt or a cancellation, anywhere in its cause chain
     * @see CancellationException#isCancellation(Throwable)
     */
    public boolean wasInterrupted() {
        return wasInterrupted;
    }
}
