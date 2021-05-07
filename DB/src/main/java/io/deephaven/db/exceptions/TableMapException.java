package io.deephaven.db.exceptions;

import io.deephaven.UncheckedDeephavenException;

/**
 * An exception denoting a problem with a {@link io.deephaven.db.v2.TableMap} operation
 */
public class TableMapException extends UncheckedDeephavenException {
    public TableMapException(String description) {
        super(description);
    }

    public TableMapException(String description, Throwable cause) {
        super(description, cause);
    }
}
