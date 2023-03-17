/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.literal;

import io.deephaven.api.expression.Expression;

public abstract class LiteralBase implements Literal {

    @Override
    public final <V extends Expression.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }
}
