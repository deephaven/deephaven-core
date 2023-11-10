/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import java.util.Collection;
import java.util.Collections;

/**
 * Exception thrown when a column is not found.
 */
public class NoSuchColumnException extends IllegalArgumentException {
    /**
     * Thrown when an operation can not find a required column.
     *
     * @param presentColumns the column names present in the table
     * @param requestedColumns the request column names that were not found
     */
    public NoSuchColumnException(Collection<String> presentColumns, Collection<String> requestedColumns) {
        super(String.format("Unknown column names [%s], available column names are [%s]",
                String.join(",", requestedColumns),
                String.join(",", presentColumns)));
    }

    /**
     * Thrown when an operation can not find a required column.
     *
     * @param presentColumns the column names present in the table
     * @param requestedColumn the request column name that was not found
     */
    public NoSuchColumnException(Collection<String> presentColumns, String requestedColumn) {
        this(presentColumns, Collections.singleton(requestedColumn));
    }
}
