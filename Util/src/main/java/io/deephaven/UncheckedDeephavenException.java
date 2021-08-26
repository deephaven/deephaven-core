package io.deephaven;

/**
 * The root of all Unchecked Deephaven exceptions
 */
public class UncheckedDeephavenException extends RuntimeException {
    public UncheckedDeephavenException(String reason) {
        super(reason);
    }

    public UncheckedDeephavenException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public UncheckedDeephavenException(Throwable cause) {
        super(cause);
    }

    public UncheckedDeephavenException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

    public UncheckedDeephavenException() {
        super();
    }
}
