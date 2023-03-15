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

    static LiteralFilter of(boolean value) {
        return LiteralBool.of(value);
    }

    static Literal of(int value) {
        return LiteralInt.of(value);
    }

    static Literal of(long value) {
        return LiteralLong.of(value);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {

        void visit(boolean literal);

        void visit(int literal);

        void visit(long literal);
    }
}
