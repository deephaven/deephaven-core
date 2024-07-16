//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
     * Whether the class is {@link TypeUtils#isPrimitiveNumeric(Class)} or {@link #isBoxedNumeric(Class)}
     *
     * @param c class
     * @return true if {@code c} is numeric, false otherwise
     */
    public static boolean isNumeric(@NotNull final Class<?> c) {
        return TypeUtils.isPrimitiveNumeric(c) || isBoxedNumeric(c);
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
}
