//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.expression;

public abstract class AbstractExpressionFactory<T> implements ExpressionFactory<T> {
    private final String pattern;

    public AbstractExpressionFactory(String pattern) {
        this.pattern = pattern;
    }

    @Override
    public String getPattern() {
        return pattern;
    }
}
