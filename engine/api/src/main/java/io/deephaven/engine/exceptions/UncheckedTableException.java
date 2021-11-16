package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * A generic unchecked exception for problems related to {@link io.deephaven.engine.table.Table} operations.
 */
public class UncheckedTableException extends UncheckedDeephavenException {

    public UncheckedTableException(String reason) {
        super(reason);
    }

    public UncheckedTableException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public UncheckedTableException(Throwable cause) {
        super(cause);
    }
}
