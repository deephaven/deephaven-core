package io.deephaven.api.value;

import io.deephaven.api.expression.Expression;

public abstract class ValueBase implements Value {

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
