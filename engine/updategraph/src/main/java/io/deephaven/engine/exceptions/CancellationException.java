/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * {@link UncheckedDeephavenException} used when an action is cancelled or interrupted.
 */
public class CancellationException extends UncheckedDeephavenException {

    public CancellationException(String message) {
        super(message);
    }

    public CancellationException(String message, Throwable cause) {
        super(message, cause);
    }
}
