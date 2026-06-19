//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.jdbc;

/**
 * String formatting styles for use when standardizing externally supplied column names. Casing of the enum members
 * indicates the resultant format. {@link #None} means no change to the casing of the source string.
 */
public enum CasingStyle {
    /**
     * UpperCamelCase (e.g., "MyColumnName")
     */
    UpperCamel,

    /**
     * lowerCamelCase (e.g., "myColumnName")
     */
    lowerCamel,

    /**
     * UPPERCASE (e.g., "MY_COLUMN_NAME")
     */
    UPPERCASE,

    /**
     * lowercase (e.g., "my_column_name")
     */
    lowercase,

    /**
     * No change to casing
     */
    None
}
