package io.deephaven.engine.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * An exception denoting a problem with a {@link io.deephaven.engine.table.impl.TableMap} operation
 */
public class TableMapException extends UncheckedDeephavenException {

    public TableMapException(String description) {
        super(description);
    }

    public TableMapException(String description, Throwable cause) {
        super(description, cause);
    }
}
