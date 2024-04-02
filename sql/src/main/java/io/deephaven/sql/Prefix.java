//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.sql;

import java.util.Objects;

enum Prefix {
    AGGREGATE("__a_"), PROJECT("__p_");

    private final String prefix;

    Prefix(String prefix) {
        this.prefix = Objects.requireNonNull(prefix);
    }

    public String prefix() {
        return prefix;
    }
}
