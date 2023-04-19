/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.literal;

import io.deephaven.api.expression.Expression;

/**
 * Represents a literal value.
 */
public interface Literal extends Expression {

    /**
     * Creates a literal / filter boolean from {@code value}.
     *
     * @param value the value
     * @return the literal / filter
     */
    static LiteralFilter of(boolean value) {
        return LiteralBool.of(value);
    }

    /**
     * Creates a literal int from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(int value) {
        return LiteralInt.of(value);
    }

    /**
     * Creates a literal long from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(long value) {
        return LiteralLong.of(value);
    }

    /**
     * Creates a literal String from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(String value) {
        return LiteralString.of(value);
    }

    <T> T walk(Visitor<T> visitor);

    interface Visitor<T> {

        T visit(boolean literal);

        T visit(int literal);

        T visit(long literal);

        T visit(String literal);
    }
}
