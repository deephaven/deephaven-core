/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.value;

import io.deephaven.api.expression.Expression;

/**
 * Represents a literal value.
 *
 * @see Expression
 */
public interface Literal extends Expression {
    static LiteralLong of(long value) {
        return LiteralLong.of(value);
    }

    static LiteralBool of(boolean value) {
        return LiteralBool.of(value);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        // TODO (deephaven-core#831): Add more table api Value structuring
        void visit(long literal);

        void visit(boolean literal);
    }
}
