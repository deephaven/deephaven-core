//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when {@link io.deephaven.engine.table.impl.remote.ConstructSnapshot} fails to successfully
 * execute the data snapshot function in an otherwise consistent state.
 */
public class SnapshotUnsuccessfulException extends UncheckedDeephavenException {
    public SnapshotUnsuccessfulException(@NotNull final String message) {
        super(message);
    }

    public SnapshotUnsuccessfulException(@NotNull final String message, @NotNull final Throwable cause) {
        super(message, cause);
    }
}
