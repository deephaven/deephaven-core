package io.deephaven.db.exceptions;

import io.deephaven.db.util.string.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class TableIOException extends UncheckedTableException {
    public TableIOException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public TableIOException(@NotNull String namespace, @NotNull String tableName,
        @Nullable String reason) {
        super(makeDescription(namespace, tableName, reason));
    }

    public TableIOException(@NotNull String namespace, @NotNull String tableName,
        @Nullable String reason, Throwable cause) {
        super(makeDescription(namespace, tableName, reason), cause);
    }

    private static String makeDescription(@NotNull String namespace, @NotNull String tableName,
        @Nullable String reason) {
        final StringBuilder sb = new StringBuilder();

        sb.append("Error while accessing ").append(namespace).append('.').append(tableName);

        if (!StringUtils.isNullOrEmpty(reason)) {
            sb.append(": ").append(reason);
        }

        return sb.toString();
    }
}
