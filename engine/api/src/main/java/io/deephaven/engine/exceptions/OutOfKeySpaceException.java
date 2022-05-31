package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * Some operations can pre-compute the necessary key-space needed to correctly maintain the intended result. If the
 * key-space exceeds Long.MAX_VALUE then it may throw this exception with additional details and suggestions to
 * work-around this limit.
 */
public class OutOfKeySpaceException extends UncheckedDeephavenException {
    public OutOfKeySpaceException(String reason) {
        super(reason);
    }
}
