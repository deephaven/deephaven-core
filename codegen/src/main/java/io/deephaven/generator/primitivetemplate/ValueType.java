//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.generator.primitivetemplate;

/**
 * Type of a value.
 */
public enum ValueType {

    CHARACTER(true, false, false),

    INTEGER(false, true, false),

    FLOATING_POINT(false, false, true);

    private final boolean isChar;
    private final boolean isInteger;
    private final boolean isFloat;

    ValueType(boolean isChar, boolean isInteger, boolean isFloat) {
        this.isChar = isChar;
        this.isInteger = isInteger;
        this.isFloat = isFloat;
    }

    /**
     * Is the value a character.
     *
     * @return true for characters, and false otherwise.
     */
    public boolean getIsChar() {
        return isChar;
    }

    /**
     * Is the value an integer.
     *
     * @return true for integers, and false otherwise.
     */
    public boolean getIsInteger() {
        return isInteger;
    }

    /**
     * Is the value a floating point.
     *
     * @return true for floating points, and false otherwise.
     */
    public boolean getIsFloat() {
        return isFloat;
    }

    /**
     * Is the value a number.
     *
     * @return true for numbers, and false otherwise.
     */
    public boolean getIsNumber() {
        return isInteger || isFloat;
    }
}
