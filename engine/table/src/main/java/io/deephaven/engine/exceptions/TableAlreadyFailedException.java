/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is thrown when an {@link io.deephaven.engine.table.TableUpdateListener update listener} cannot be
 * added to a {@link io.deephaven.engine.table.Table} because it has already failed.
 */
public class TableAlreadyFailedException extends UncheckedDeephavenException {
    public TableAlreadyFailedException(@NotNull final String message) {
        super(message);
    }

    public TableAlreadyFailedException(@NotNull final String message, @NotNull final Throwable cause) {
        super(message, cause);
    }
}
