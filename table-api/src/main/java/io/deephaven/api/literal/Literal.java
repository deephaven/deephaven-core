//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
     * Creates a literal char from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(char value) {
        return LiteralChar.of(value);
    }

    /**
     * Creates a literal byte from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(byte value) {
        return LiteralByte.of(value);
    }

    /**
     * Creates a literal short from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(short value) {
        return LiteralShort.of(value);
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
     * Creates a literal float from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(float value) {
        return LiteralFloat.of(value);
    }

    /**
     * Creates a literal double from {@code value}.
     *
     * @param value the value
     * @return the literal
     */
    static Literal of(double value) {
        return LiteralDouble.of(value);
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

        T visit(char literal);

        T visit(byte literal);

        T visit(short literal);

        T visit(int literal);

        T visit(long literal);

        T visit(float literal);

        T visit(double literal);

        T visit(String literal);
    }
}
