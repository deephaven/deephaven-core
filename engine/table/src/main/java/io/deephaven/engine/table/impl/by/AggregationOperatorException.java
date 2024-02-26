package io.deephaven.engine.table.impl.by;

import io.deephaven.UncheckedDeephavenException;

/**
 * This exception provides more context when an aggregation operator throws an Exception.
 *
 * <p>
 * When an aggregation operator results in an Error, this exception is added as a suppressed exception.
 * </p>
 */
public class AggregationOperatorException extends UncheckedDeephavenException {
    public AggregationOperatorException(String reason, Throwable cause) {
        super(reason, cause);
    }

    public AggregationOperatorException(String reason) {
        super(reason);
    }
}
