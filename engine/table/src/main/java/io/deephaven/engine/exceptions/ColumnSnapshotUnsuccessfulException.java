//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when {@link io.deephaven.engine.table.impl.remote.ConstructSnapshot} fails to successfully
 * collect snapshot data for a column, whether the columns were collected serially or in parallel. The cause chain ends
 * at the failure the column fill threw, and includes an instance naming that column.
 */
public class ColumnSnapshotUnsuccessfulException extends UncheckedDeephavenException {
    public ColumnSnapshotUnsuccessfulException(@NotNull final String message, @NotNull final Throwable cause) {
        super(message, cause);
    }
}
