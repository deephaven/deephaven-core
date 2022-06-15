/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jdbc;

import java.sql.JDBCType;

public class JdbcTypeMapperException extends RuntimeException {
    private final int sqlType;
    private final Class<?> deephavenType;

    public JdbcTypeMapperException(int sqlType, Class<?> deephavenType) {
        this.sqlType = sqlType;
        this.deephavenType = deephavenType;
    }

    @SuppressWarnings("unused")
    public int getSqlType() {
        return sqlType;
    }

    @SuppressWarnings("unused")
    public Class<?> getDeephavenType() {
        return deephavenType;
    }

    public String getSqlTypeString() {
        try {
            return JDBCType.valueOf(sqlType).name();
        } catch (final IllegalArgumentException ignored) {
            return "N/A";
        }
    }

    @Override
    public String toString() {
        if (deephavenType != null) {
            return String.format("No JDBC type mapping for SQL type %s (%s) to Deephaven type \"%s\"", sqlType,
                    getSqlTypeString(), deephavenType);
        } else {
            return String.format("No JDBC type mapping for SQL type %s (%s) to any Deephaven type", sqlType,
                    getSqlTypeString());
        }
    }
}
