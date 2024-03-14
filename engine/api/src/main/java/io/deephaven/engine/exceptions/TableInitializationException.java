//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link UncheckedTableException} derivative which indicates a table was unable to be initialized for one reason or
 * another.
 */
public class TableInitializationException extends UncheckedTableException {

    public TableInitializationException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public TableInitializationException(@NotNull String tableDescription, @Nullable String reason) {
        super(makeDescription(tableDescription, reason));
    }

    public TableInitializationException(@NotNull String tableDescription, @Nullable String reason,
            Throwable cause) {
        super(makeDescription(tableDescription, reason), cause);
    }

    private static String makeDescription(@NotNull String tableDescription, @Nullable String reason) {
        final StringBuilder sb = new StringBuilder();

        sb.append("Error while initializing ").append(tableDescription);

        if (reason != null && !reason.isEmpty()) {
            sb.append(": ").append(reason);
        }

        return sb.toString();
    }
}
