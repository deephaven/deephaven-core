package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;
import org.jetbrains.annotations.NotNull;

/**
 * Operations that require input to conform to ordering requirements may throw this exception when out of order data is
 * encountered.
 */
public class OutOfOrderException extends UncheckedDeephavenException {

    public OutOfOrderException(@NotNull final String reason) {
        super(reason);
    }
}
