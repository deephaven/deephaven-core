/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.value;

import io.deephaven.api.expression.Expression;
import io.deephaven.api.expression.NullaryExpression;

public abstract class LiteralBase implements Literal {

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final <V extends NullaryExpression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
