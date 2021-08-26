package io.deephaven.db.exceptions;

import io.deephaven.UncheckedDeephavenException;

public class ArgumentException extends UncheckedDeephavenException {
    public ArgumentException(String reason) {
        super(reason);
    }

    public ArgumentException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public ArgumentException(Throwable cause) {
        super(cause);
    }

    public ArgumentException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public ArgumentException() {
        super();
    }
}
