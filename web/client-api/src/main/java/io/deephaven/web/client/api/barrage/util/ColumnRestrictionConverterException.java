//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage.util;

/**
 * Exception thrown when column restriction protobuf data cannot be converted to a ColumnRestriction.
 */
public class ColumnRestrictionConverterException extends Exception {
    public ColumnRestrictionConverterException(String message) {
        super(message);
    }

    public ColumnRestrictionConverterException(String message, Throwable cause) {
        super(message, cause);
    }
}
