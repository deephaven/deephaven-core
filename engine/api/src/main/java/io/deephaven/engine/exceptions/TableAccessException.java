package io.deephaven.engine.exceptions;

import io.deephaven.util.auth.AuthContext;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An {@link UncheckedPermissionException} derivative which indicates a table may not be accessed for one reason or
 * another.
 */
public class TableAccessException extends UncheckedPermissionException {

    public TableAccessException(String reason) {
        super(reason);
    }

    public TableAccessException(@NotNull String tableDescription, @NotNull AuthContext authContext) {
        this(tableDescription, authContext, "");
    }

    public TableAccessException(@NotNull String tableDescription, @NotNull AuthContext authContext,
            @Nullable String reason) {
        super(makeDescription(tableDescription, authContext, reason));
    }

    private static String makeDescription(@NotNull String tableDescription,
            @NotNull AuthContext authContext, @Nullable String reason) {
        final StringBuilder sb = new StringBuilder();

        sb.append(authContext.getLogRepresentation()).append(" may not access: ");
        sb.append(tableDescription);

        if (reason != null && !reason.isEmpty()) {
            sb.append(": ").append(reason);
        }

        return sb.toString();
    }
}
