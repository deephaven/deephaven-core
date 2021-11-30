package io.deephaven.engine.exceptions;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TableIOException extends UncheckedTableException {

    public TableIOException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public TableIOException(@NotNull String tableDescription, @Nullable String reason) {
        super(makeDescription(tableDescription, reason));
    }

    public TableIOException(@NotNull String tableDescription, @Nullable String reason,
            Throwable cause) {
        super(makeDescription(tableDescription, reason), cause);
    }

    private static String makeDescription(@NotNull String tableDescription, @Nullable String reason) {
        final StringBuilder sb = new StringBuilder();

        sb.append("Error while accessing ").append(tableDescription);

        if (reason != null && !reason.isEmpty()) {
            sb.append(": ").append(reason);
        }

        return sb.toString();
    }
}
