/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.configuration;

/**
 * Standardized runtime exception type for PropertyFile and related utilities.
 */
public class PropertyException extends RuntimeException {

    public PropertyException(String message) {
        super(message);
    }

    public PropertyException(String message, Throwable cause) {
        super(message, cause);
    }
}
