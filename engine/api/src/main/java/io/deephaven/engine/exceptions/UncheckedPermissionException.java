/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AuthContext;

/**
 * An {@link UncheckedDeephavenException} that indicates an issue with permissions.
 */
public class UncheckedPermissionException extends UncheckedDeephavenException {

    public UncheckedPermissionException(String reason) {
        super(reason);
    }

    public UncheckedPermissionException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public UncheckedPermissionException(Throwable cause) {
        super(cause);
    }

    public UncheckedPermissionException(AuthContext context, String reason) {
        super(context.toString() + ": " + reason);
    }

    public UncheckedPermissionException(AuthContext context, String reason, Throwable cause) {
        super(context.toString() + ": " + reason, cause);
    }

    public UncheckedPermissionException(AuthContext context, Throwable cause) {
        super(context.toString(), cause);
    }
}
