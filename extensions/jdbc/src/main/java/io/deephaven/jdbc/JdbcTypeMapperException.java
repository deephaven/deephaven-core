package io.deephaven.jdbc;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Arrays;

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
        return Arrays.stream(Types.class.getFields()).filter(f -> {
            try {
                return (Integer) f.get(null) == sqlType;
            } catch (IllegalAccessException ex) {
                throw new RuntimeException("Unexpected error looking up SQL type information:", ex);
            }
        }).map(Field::getName).findFirst().orElse("N/A");
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
