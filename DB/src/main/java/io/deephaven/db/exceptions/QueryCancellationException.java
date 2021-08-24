/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.exceptions;

import io.deephaven.UncheckedDeephavenException;

public class QueryCancellationException extends UncheckedDeephavenException {
    public QueryCancellationException(String message) {
        super(message);
    }

    public QueryCancellationException(String message, Throwable cause) {
        super(message, cause);
    }
}
