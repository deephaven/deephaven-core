/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.generator.primitivetemplate;

/**
 * Type of a value.
 */
public enum ValueType {
    BOOLEAN(true, false, false, false),

    CHARACTER(false, true, false, false),

    INTEGER(false, false, true, false),

    FLOATING_POINT(false, false, false, true);

    private final boolean isBoolean;
    private final boolean isChar;
    private final boolean isInteger;
    private final boolean isFloat;

    ValueType(boolean isBoolean, boolean isChar, boolean isInteger, boolean isFloat) {
        this.isBoolean = isBoolean;
        this.isChar = isChar;
        this.isInteger = isInteger;
        this.isFloat = isFloat;
    }

    /**
     * Is the value a boolean.
     *
     * @return true for booleans, and false otherwise.
     */
    public boolean getIsBoolean() {
        return isBoolean;
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
