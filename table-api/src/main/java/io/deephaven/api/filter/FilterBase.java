/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.filter;

import io.deephaven.api.expression.Expression;

public abstract class FilterBase implements Filter {

    @Override
    public final FilterNot not() {
        return FilterNot.of(this);
    }

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
