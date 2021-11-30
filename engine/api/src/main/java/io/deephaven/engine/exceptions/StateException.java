package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

public class StateException extends UncheckedDeephavenException {

    public StateException(String reason) {
        super(reason);
    }

    public StateException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public StateException(Throwable cause) {
        super(cause);
    }

    public StateException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public StateException() {
        super();
    }
}
