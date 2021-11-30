package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

public class OperationException extends UncheckedDeephavenException {

    public OperationException(String reason) {
        super(reason);
    }

    public OperationException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public OperationException(Throwable cause) {
        super(cause);
    }

    public OperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public OperationException() {
        super();
    }
}
