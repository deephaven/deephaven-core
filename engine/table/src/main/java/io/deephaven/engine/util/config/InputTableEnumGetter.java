/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.util.config;

/**
 * Accessor interface for enumeration constants for an input table column.
 */
public interface InputTableEnumGetter {
    Object[] getEnumsForColumn(String columnName);
}
