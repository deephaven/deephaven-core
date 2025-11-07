//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.type;

import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;

public class NumericTypeUtils {
    private NumericTypeUtils() {}

    /**
     * Whether the class is an instance of {@link Number}.
     *
     * @param c class
     * @return true if Number.class is assignable from {@code c}, false otherwise
     */
    public static boolean isBoxedNumeric(@NotNull final Class<?> c) {
        return Number.class.isAssignableFrom(c);
    }

    /**
     * Whether the class is {@link NumericTypeUtils#isPrimitiveNumeric(Class)} or {@link #isBoxedNumeric(Class)}
     *
     * @param c class
     * @return true if {@code c} is numeric, false otherwise
     */
    public static boolean isNumeric(@NotNull final Class<?> c) {
        return isPrimitiveNumeric(c) || isBoxedNumeric(c);
    }

    /**
     * Whether the class is a {@link BigInteger} or {@link BigDecimal}
     *
     * @param type the class
     * @return true if the type is BigInteger or BigDecimal, false otherwise
     */
    public static boolean isBigNumeric(Class<?> type) {
        return BigInteger.class.isAssignableFrom(type) || BigDecimal.class.isAssignableFrom(type);
    }

    /**
     * Whether the class is equal to the char/ Character types.
     *
     * @param c class
     * @return true if {@code c} is a numeric primitive, false otherwise
     */
    public static boolean isChar(@NotNull final Class<?> c) {
        return c == char.class || c == Character.class;
    }

    /**
     * Whether the class is equal to one of the 4 integral types: int, long, short, or byte.
     *
     * @param c class
     * @return true if {@code c} is a numeric primitive, false otherwise
     */
    public static boolean isIntegral(@NotNull final Class<?> c) {
        return c == int.class || c == Integer.class
                || c == long.class || c == Long.class
                || c == short.class || c == Short.class
                || c == byte.class || c == Byte.class;
    }

    /**
     * Whether the class is equal to one of the 2 float primitives: float, double.
     *
     * @param c class
     * @return true if {@code c} is a numeric primitive, false otherwise
     */
    public static boolean isFloat(@NotNull final Class<?> c) {
        return c == double.class || c == Double.class
                || c == float.class || c == Float.class;
    }

    /**
     * Whether the class is an integral or char.
     *
     * @param c class
     * @return true if {@code c} is a numeric primitive, false otherwise
     */
    public static boolean isIntegralOrChar(@NotNull final Class<?> c) {
        return isIntegral(c) || isChar(c);
    }

    /**
     * Whether the class is equal to one of the six numeric primitives: float, double, int, long, short, or byte.
     *
     * @param c class
     * @return true if {@code c} is a numeric primitive, false otherwise
     */
    public static boolean isPrimitiveNumeric(@NotNull final Class<?> c) {
        return c == double.class || c == float.class
                || c == int.class || c == long.class || c == short.class || c == byte.class;
    }
}
