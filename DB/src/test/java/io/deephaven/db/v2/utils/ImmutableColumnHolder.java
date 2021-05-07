/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

public class ImmutableColumnHolder extends ColumnHolder {
    @SuppressWarnings("unchecked")
    public <T> ImmutableColumnHolder(String name, boolean grouped, T... data) {
        super(name, grouped, data);
    }
}
