//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Converts a numeric filter value -- a query-scope parameter or a directly supplied match value -- to the type of the
 * column it is compared against, so that the filter selects exactly the rows a Java comparison between the column and
 * the value would select.
 *
 * <p>
 * Java compares mixed numeric types after binary numeric promotion. When the value's type widens to the column's type
 * (an {@code int} value against a {@code long} or {@code float} column, say) the comparison happens in the column's
 * type, so the widened value is used as is, even where widening rounds. When the value's type is the wider one (a
 * {@code double} value against an {@code int} or {@code float} column) the comparison happens in the value's type, and
 * the value is accepted only when exactly one value of the column type compares equal to it; the filter is then
 * equivalent to matching, or comparing against, that one value. Anything else -- a fraction against an integral column,
 * a value out of the column type's range, a {@code double} with no exact {@code float} equivalent -- has no such
 * equivalent, and is rejected with an {@link IllegalArgumentException} rather than truncated or wrapped. Callers that
 * can evaluate the comparison another way (a {@link ConditionFilter} failover, for instance) catch it and do so.
 *
 * <p>
 * Null sentinels are values of Deephaven's type system rather than of Java's: a boxed null sentinel of any type becomes
 * the column type's null sentinel, and a value that would convert to the column type's null sentinel without being null
 * itself is rejected.
 */
final class FilterValueCoercion {

    private FilterValueCoercion() {} // static use only

    /**
     * Convert {@code value} to the boxed type of the primitive {@code columnType}.
     *
     * @param value the value to convert; a boxed primitive number, a {@link Character}, a {@link BigInteger} or a
     *        {@link BigDecimal}
     * @param columnType the primitive column type: {@code byte}, {@code short}, {@code int}, {@code long},
     *        {@code float}, {@code double} or {@code char}
     * @return the boxed converted value
     * @throws IllegalArgumentException if {@code value} has no equivalent in {@code columnType}
     */
    static Object toPrimitive(@NotNull final Object value, @NotNull final Class<?> columnType) {
        if (isNullSentinel(value)) {
            return boxedNull(columnType);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return fromIntegral(value, ((Number) value).longValue(), columnType);
        }
        if (value instanceof Character) {
            // char promotes to int
            return fromIntegral(value, (Character) value, columnType);
        }
        if (value instanceof Float) {
            return fromFloatingPoint(value, (Float) value, true, columnType);
        }
        if (value instanceof Double) {
            return fromFloatingPoint(value, (Double) value, false, columnType);
        }
        if (value instanceof BigInteger) {
            return fromBigDecimal(value, new BigDecimal((BigInteger) value), columnType);
        }
        if (value instanceof BigDecimal) {
            return fromBigDecimal(value, (BigDecimal) value, columnType);
        }
        throw cannotConvert(value, columnType, "it is not a number");
    }

    /**
     * Convert {@code value} to a {@link BigInteger}.
     *
     * @param value the value to convert; a boxed primitive number, a {@link Character}, a {@link BigInteger} or a
     *        {@link BigDecimal}
     * @return the converted value, or {@code null} for a boxed null sentinel
     * @throws IllegalArgumentException if {@code value} is not an integer
     */
    static BigInteger toBigInteger(@NotNull final Object value) {
        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        if (isNullSentinel(value)) {
            return null;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigInteger.valueOf(((Number) value).longValue());
        }
        if (value instanceof Character) {
            return BigInteger.valueOf((Character) value);
        }
        final BigDecimal decimal;
        if (value instanceof Float || value instanceof Double) {
            final double doubleValue = ((Number) value).doubleValue();
            if (!Double.isFinite(doubleValue)) {
                throw cannotConvert(value, BigInteger.class, "it is not finite");
            }
            decimal = new BigDecimal(doubleValue);
        } else if (value instanceof BigDecimal) {
            decimal = (BigDecimal) value;
        } else {
            throw cannotConvert(value, BigInteger.class, "it is not a number");
        }
        try {
            return decimal.toBigIntegerExact();
        } catch (final ArithmeticException e) {
            throw cannotConvert(value, BigInteger.class, "it is not an integer");
        }
    }

    /**
     * Convert {@code value} to a {@link BigDecimal}. Floating-point values are converted through their canonical string
     * representation, as {@link BigDecimal#valueOf(double)} does.
     *
     * @param value the value to convert; a boxed primitive number, a {@link Character}, a {@link BigInteger} or a
     *        {@link BigDecimal}
     * @return the converted value, or {@code null} for a boxed null sentinel
     * @throws IllegalArgumentException if {@code value} is not a finite number
     */
    static BigDecimal toBigDecimal(@NotNull final Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        if (isNullSentinel(value)) {
            return null;
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof Character) {
            return BigDecimal.valueOf((Character) value);
        }
        if (value instanceof Float || value instanceof Double) {
            final double doubleValue = ((Number) value).doubleValue();
            if (!Double.isFinite(doubleValue)) {
                throw cannotConvert(value, BigDecimal.class, "it is not finite");
            }
            return BigDecimal.valueOf(doubleValue);
        }
        throw cannotConvert(value, BigDecimal.class, "it is not a number");
    }

    private static boolean isNullSentinel(@NotNull final Object value) {
        return value.equals(QueryConstants.NULL_BYTE_BOXED)
                || value.equals(QueryConstants.NULL_SHORT_BOXED)
                || value.equals(QueryConstants.NULL_INT_BOXED)
                || value.equals(QueryConstants.NULL_LONG_BOXED)
                || value.equals(QueryConstants.NULL_FLOAT_BOXED)
                || value.equals(QueryConstants.NULL_DOUBLE_BOXED)
                || value.equals(QueryConstants.NULL_CHAR_BOXED);
    }

    private static Object boxedNull(@NotNull final Class<?> columnType) {
        if (columnType == byte.class) {
            return QueryConstants.NULL_BYTE_BOXED;
        }
        if (columnType == short.class) {
            return QueryConstants.NULL_SHORT_BOXED;
        }
        if (columnType == int.class) {
            return QueryConstants.NULL_INT_BOXED;
        }
        if (columnType == long.class) {
            return QueryConstants.NULL_LONG_BOXED;
        }
        if (columnType == float.class) {
            return QueryConstants.NULL_FLOAT_BOXED;
        }
        if (columnType == double.class) {
            return QueryConstants.NULL_DOUBLE_BOXED;
        }
        if (columnType == char.class) {
            return QueryConstants.NULL_CHAR_BOXED;
        }
        throw new IllegalArgumentException("Unsupported column type " + columnType.getName());
    }

    /** The smallest non-null value of the integral {@code columnType}. */
    private static long minNonNull(@NotNull final Class<?> columnType) {
        if (columnType == byte.class) {
            return QueryConstants.MIN_BYTE;
        }
        if (columnType == short.class) {
            return QueryConstants.MIN_SHORT;
        }
        if (columnType == int.class) {
            return QueryConstants.MIN_INT;
        }
        if (columnType == long.class) {
            return QueryConstants.MIN_LONG;
        }
        if (columnType == char.class) {
            return QueryConstants.MIN_CHAR;
        }
        throw new IllegalArgumentException("Unsupported column type " + columnType.getName());
    }

    /** The largest non-null value of the integral {@code columnType}. */
    private static long maxNonNull(@NotNull final Class<?> columnType) {
        if (columnType == byte.class) {
            return QueryConstants.MAX_BYTE;
        }
        if (columnType == short.class) {
            return QueryConstants.MAX_SHORT;
        }
        if (columnType == int.class) {
            return QueryConstants.MAX_INT;
        }
        if (columnType == long.class) {
            return QueryConstants.MAX_LONG;
        }
        if (columnType == char.class) {
            return QueryConstants.MAX_CHAR;
        }
        throw new IllegalArgumentException("Unsupported column type " + columnType.getName());
    }

    private static Object boxIntegral(final long value, @NotNull final Class<?> columnType) {
        if (columnType == byte.class) {
            return (byte) value;
        }
        if (columnType == short.class) {
            return (short) value;
        }
        if (columnType == int.class) {
            return (int) value;
        }
        if (columnType == long.class) {
            return value;
        }
        if (columnType == char.class) {
            return (char) value;
        }
        throw new IllegalArgumentException("Unsupported column type " + columnType.getName());
    }

    private static Object fromIntegral(
            @NotNull final Object value,
            final long longValue,
            @NotNull final Class<?> columnType) {
        if (columnType == float.class) {
            // widening; Java compares in float, rounding included
            return (float) longValue;
        }
        if (columnType == double.class) {
            // widening; Java compares in double, rounding included
            return (double) longValue;
        }
        if (longValue < minNonNull(columnType) || longValue > maxNonNull(columnType)) {
            throw cannotConvert(value, columnType, "it is outside the range of the column type");
        }
        return boxIntegral(longValue, columnType);
    }

    private static Object fromFloatingPoint(
            @NotNull final Object value,
            final double doubleValue,
            final boolean isFloat,
            @NotNull final Class<?> columnType) {
        if (columnType == double.class) {
            // identity or widening; float to double is exact
            return doubleValue;
        }
        if (columnType == float.class) {
            if (isFloat) {
                return value;
            }
            final float floatValue = (float) doubleValue;
            if (!Double.isNaN(doubleValue) && floatValue != doubleValue) {
                throw cannotConvert(value, columnType, "it has no exact float equivalent");
            }
            if (floatValue == QueryConstants.NULL_FLOAT) {
                throw cannotConvert(value, columnType, "it is equal to the column type's null value");
            }
            return floatValue;
        }

        // integral column
        if (!Double.isFinite(doubleValue)) {
            throw cannotConvert(value, columnType, "it is not finite");
        }
        if (doubleValue != Math.rint(doubleValue)) {
            throw cannotConvert(value, columnType, "it is not an integer");
        }
        // every integer in [-2^63, 2^63) converts to long exactly
        if (doubleValue < -0x1p63 || doubleValue >= 0x1p63) {
            throw cannotConvert(value, columnType, "it is outside the range of the column type");
        }
        final long longValue = (long) doubleValue;
        final long min = minNonNull(columnType);
        final long max = maxNonNull(columnType);
        if (longValue < min || longValue > max) {
            throw cannotConvert(value, columnType, "it is outside the range of the column type");
        }
        // Java compares in the floating-point type, where large integers round; unless the value's neighbors round to
        // something else, more than one value of the column type compares equal to it.
        if ((longValue > min && roundsTo(longValue - 1, doubleValue, isFloat))
                || (longValue < max && roundsTo(longValue + 1, doubleValue, isFloat))) {
            throw cannotConvert(value, columnType,
                    "more than one value of the column type compares equal to it");
        }
        return boxIntegral(longValue, columnType);
    }

    private static boolean roundsTo(final long candidate, final double doubleValue, final boolean isFloat) {
        return isFloat ? (float) candidate == (float) doubleValue : (double) candidate == doubleValue;
    }

    private static Object fromBigDecimal(
            @NotNull final Object value,
            @NotNull final BigDecimal decimal,
            @NotNull final Class<?> columnType) {
        if (columnType == float.class || columnType == double.class) {
            final double doubleValue =
                    columnType == float.class ? decimal.floatValue() : decimal.doubleValue();
            if (!Double.isFinite(doubleValue) || new BigDecimal(doubleValue).compareTo(decimal) != 0) {
                throw cannotConvert(value, columnType, "it has no exact equivalent in the column type");
            }
            if (columnType == float.class) {
                final float floatValue = (float) doubleValue;
                if (floatValue == QueryConstants.NULL_FLOAT) {
                    throw cannotConvert(value, columnType, "it is equal to the column type's null value");
                }
                return floatValue;
            }
            if (doubleValue == QueryConstants.NULL_DOUBLE) {
                throw cannotConvert(value, columnType, "it is equal to the column type's null value");
            }
            return doubleValue;
        }
        final BigInteger integer;
        try {
            integer = decimal.toBigIntegerExact();
        } catch (final ArithmeticException e) {
            throw cannotConvert(value, columnType, "it is not an integer");
        }
        if (integer.bitLength() > 63) {
            throw cannotConvert(value, columnType, "it is outside the range of the column type");
        }
        return fromIntegral(value, integer.longValue(), columnType);
    }

    private static IllegalArgumentException cannotConvert(
            @NotNull final Object value,
            @NotNull final Class<?> columnType,
            @NotNull final String reason) {
        return new IllegalArgumentException(String.format(
                "Cannot convert value <%s> of type %s to column type %s: %s",
                value, value.getClass().getName(), columnType.getName(), reason));
    }
}
