//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.context;

public class QueryScopeParam<T> {

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
