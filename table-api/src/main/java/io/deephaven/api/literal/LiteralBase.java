//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.literal;

import io.deephaven.api.expression.Expression;

public abstract class LiteralBase implements Literal {

    @Override
    public final <T> T walk(Expression.Visitor<T> visitor) {
        return visitor.visit(this);
    }
}
