//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

/**
 * Exception thrown when a column is not found.
 */
public class NotSortableColumnException extends InvalidColumnException {

    /**
     * Thrown when a given column is not sortable.
     *
     * @param message the exception message
     */
    public NotSortableColumnException(final String message) {
        super(message);
    }
}
