//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * This exception is used when a result cannot be returned because the request was cancelled.
 */
public class RequestCancelledException extends UncheckedDeephavenException {
    public RequestCancelledException(@NotNull final String message) {
        super(message);
    }

    public RequestCancelledException(@NotNull final String message, @NotNull final Throwable cause) {
        super(message, cause);
    }
}
