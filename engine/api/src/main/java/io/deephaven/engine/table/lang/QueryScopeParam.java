/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.lang;

public class QueryScopeParam<T> {

    public static final QueryScopeParam<?>[] ZERO_LENGTH_PARAM_ARRAY = new QueryScopeParam[0];

    private final String name;
    private final T value;

    public String getName() {
        return name;
    }

    public T getValue() {
        return value;
    }

    public QueryScopeParam(String name, T value) {
        this.name = name;
        this.value = value;
    }
}
