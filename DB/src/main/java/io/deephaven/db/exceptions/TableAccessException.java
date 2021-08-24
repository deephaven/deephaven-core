package io.deephaven.db.exceptions;

import io.deephaven.db.util.string.StringUtils;
import io.deephaven.util.auth.AuthContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link UncheckedPermissionException} derivative which indicates a table may not be accessed
 * for one reason or another.
 */
public class TableAccessException extends UncheckedPermissionException {
    public TableAccessException(String reason) {
        super(reason);
    }

    public TableAccessException(@Nullable String namespace, @NotNull String tableName,
        @NotNull AuthContext authContext) {
        this(namespace, tableName, authContext, "");
    }

    public TableAccessException(@Nullable String namespace, @NotNull String tableName,
        @NotNull AuthContext authContext, @Nullable String reason) {
        super(makeDescription(namespace, tableName, authContext, reason));
    }

    private static String makeDescription(@Nullable String namespace, @NotNull String tableName,
        @NotNull AuthContext authContext, @Nullable String reason) {
        final StringBuilder sb = new StringBuilder();

        sb.append(authContext.getLogRepresentation()).append(" may not access: ");
        if (!StringUtils.isNullOrEmpty(namespace)) {
            sb.append(namespace).append('.');
        }

        sb.append(tableName);

        if (!StringUtils.isNullOrEmpty(reason)) {
            sb.append(": ").append(reason);
        }

        return sb.toString();
    }
}
