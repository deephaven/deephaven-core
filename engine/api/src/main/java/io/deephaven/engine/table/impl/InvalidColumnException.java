//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
