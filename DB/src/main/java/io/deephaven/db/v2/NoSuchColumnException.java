package io.deephaven.db.v2;

import io.deephaven.base.StringUtils;

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
    public NoSuchColumnException(Collection<String> presentColumns,
        Collection<String> requestedColumns) {
        super("Unknown column names [" + StringUtils.joinStrings(requestedColumns, ",")
            + "], available column names are [" + StringUtils.joinStrings(presentColumns, ",")
            + "]");
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
