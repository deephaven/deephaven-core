//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;


/**
 * Exception thrown when a column is not found.
 */
public class InvalidColumnException extends IllegalArgumentException {

    /**
     * Thrown when an operation cannot be applied to a given column(s).
     *
     * @param message the message
     */
    public InvalidColumnException(final String message) {
        super(message);
    }
}
