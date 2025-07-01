//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when a {@link ChunkFilter} cannot compute overlaps with a min and max value.
 */
public class CannotComputeOverlapsException extends UncheckedDeephavenException {
    public CannotComputeOverlapsException(@NotNull final String message) {
        super(message);
    }

    public CannotComputeOverlapsException(@NotNull final String message, @NotNull final Throwable cause) {
        super(message, cause);
    }
}
