//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import java.util.Objects;

public class UnsupportedSqlOperation extends UnsupportedOperationException {
    private final Class<?> clazz;

    public UnsupportedSqlOperation(Class<?> clazz) {
        this.clazz = Objects.requireNonNull(clazz);
    }

    public UnsupportedSqlOperation(String message, Class<?> clazz) {
        super(message);
        this.clazz = Objects.requireNonNull(clazz);
    }

    public Class<?> clazz() {
        return clazz;
    }
}
