//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg.util;

import io.deephaven.util.QueryConstants;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Objects;

/**
 * The types of counts that can be performed.
 */
public enum AggCountType {
    /**
     * Count all values, including null
     */
    ALL,

    /**
     * Count non-null values
     */
    NON_NULL,

    /**
     * Count null values
     */
    NULL,

    /**
     * Count non-null negative values. For floating point types, this includes -inf but excludes +/-0.0
     */
    NEGATIVE,

    /**
     * Count non-null positive values. For floating point types, this includes +inf but excludes +/-0.0
     */
    POSITIVE,

    /**
     * Count zero values. For floating point types, this includes both -0.0 and +0.0
     */
    ZERO,

    /**
     * Count NaN values. For non-floating point types, this count is always 0
     */
    NAN,

    /**
     * Count +/-inf values. For non-floating point types, this count is always 0
     */
    INFINITE,

    /**
     * Count finite values. This excludes NaN, +/-inf, and null values
     */
    FINITE,

    /**
     * Count non-null non-zero values. For floating point types, this excludes +/-0.0 but includes +/- inf
     */
    NON_ZERO,

    /**
     * Count non-null non-negative values. For floating point types, this excludes -inf but includes +/-0.0 and +inf
     */
    NON_NEGATIVE,

    /**
     * Count non-null non-positive values. For floating point types, this excludes +inf but includes +/-0.0 and -inf
     */
    NON_POSITIVE;

    // This class leverages assumptions about null values for the various primitive types to optimize count operations.
    // This assert ensures that the assumptions are regularly verified against future changes.
    static {
        assert QueryConstants.NULL_BYTE < 0;
        assert QueryConstants.NULL_CHAR > 0; // null char is the only positive value
        assert QueryConstants.NULL_SHORT < 0;
        assert QueryConstants.NULL_INT < 0;
        assert QueryConstants.NULL_LONG < 0;
        assert QueryConstants.NULL_FLOAT < 0;
        assert QueryConstants.NULL_DOUBLE < 0;
    }

    // region count-interfaces-functions

    // The following functions are used to count values of a given type based on a specific count type.

    @FunctionalInterface
    public interface ByteCountFunction {
        boolean count(byte value);
    }

    @FunctionalInterface
    public interface CharCountFunction {
        boolean count(char value);
    }

    @FunctionalInterface
    public interface ShortCountFunction {
        boolean count(short value);
    }

    @FunctionalInterface
    public interface IntCountFunction {
        boolean count(int value);
    }

    @FunctionalInterface
    public interface LongCountFunction {
        boolean count(long value);
    }

    @FunctionalInterface
    public interface FloatCountFunction {
        boolean count(float value);
    }

    @FunctionalInterface
    public interface DoubleCountFunction {
        boolean count(double value);
    }

    @FunctionalInterface
    public interface ObjectCountFunction {
        boolean count(Object value);
    }

    @FunctionalInterface
    public interface BigDecimalCountFunction {
        boolean count(BigDecimal value);
    }

    @FunctionalInterface
    public interface BigIntegerCountFunction {
        boolean count(BigInteger value);
    }

    public static ByteCountFunction getByteCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_BYTE;
            case NULL:
                return value -> value == QueryConstants.NULL_BYTE;
            case POSITIVE:
                return value -> value > 0; // NULL_BYTE is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_BYTE && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            case NON_ZERO:
                return value -> value != QueryConstants.NULL_BYTE && value != 0;
            case NON_NEGATIVE:
                return value -> value >= 0; // NULL_BYTE is negative
            case NON_POSITIVE:
                return value -> value != QueryConstants.NULL_BYTE && value <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for byte data type");
        }
    }

    public static CharCountFunction getCharCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
            case FINITE:
            case NON_NEGATIVE: // char is unsigned
                return value -> value != QueryConstants.NULL_CHAR;
            case NULL:
                return value -> value == QueryConstants.NULL_CHAR;
            case POSITIVE:
                return value -> value != QueryConstants.NULL_CHAR && value > 0;
            case ZERO:
                return value -> value == 0;
            case NEGATIVE: // char is unsigned
            case NAN:
            case INFINITE:
                return value -> false;
            case NON_ZERO:
                return value -> value != QueryConstants.NULL_CHAR && value != 0;
            case NON_POSITIVE:
                return value -> value == QueryConstants.NULL_CHAR || value <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for char data type");
        }
    }

    public static ShortCountFunction getShortCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_SHORT;
            case NULL:
                return value -> value == QueryConstants.NULL_SHORT;
            case POSITIVE:
                return value -> value > 0;// NULL_SHORT is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_SHORT && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            case NON_ZERO:
                return value -> value != QueryConstants.NULL_SHORT && value != 0;
            case NON_NEGATIVE:
                return value -> value >= 0; // NULL_SHORT is negative
            case NON_POSITIVE:
                return value -> value != QueryConstants.NULL_SHORT && value <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for short data type");
        }
    }

    public static IntCountFunction getIntCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_INT;
            case NULL:
                return value -> value == QueryConstants.NULL_INT;
            case POSITIVE:
                return value -> value > 0; // NULL_INT is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_INT && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            case NON_ZERO:
                return value -> value != QueryConstants.NULL_INT && value != 0;
            case NON_NEGATIVE:
                return value -> value >= 0; // NULL_INT is negative
            case NON_POSITIVE:
                return value -> value != QueryConstants.NULL_INT && value <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for int data type");
        }
    }

    public static LongCountFunction getLongCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
            case FINITE:
                return value -> value != QueryConstants.NULL_LONG;
            case NULL:
                return value -> value == QueryConstants.NULL_LONG;
            case POSITIVE:
                return value -> value > 0; // NULL_LONG is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_LONG && value < 0;
            case NAN:
            case INFINITE:
                return value -> false;
            case NON_ZERO:
                return value -> value != QueryConstants.NULL_LONG && value != 0;
            case NON_NEGATIVE:
                return value -> value >= 0; // NULL_LONG is negative
            case NON_POSITIVE:
                return value -> value != QueryConstants.NULL_LONG && value <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for long data type");
        }
    }

    public static FloatCountFunction getFloatCountFunction(final AggCountType countType) {
        // NOTE: we are leveraging the fact that comparisons with NaN always return false (<, >, <=, >= etc.) to
        // filter out NaN values
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
                return value -> value != QueryConstants.NULL_FLOAT;
            case NULL:
                return value -> value == QueryConstants.NULL_FLOAT;
            case POSITIVE:
                return value -> value > 0; // NULL_FLOAT is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_FLOAT && value < 0;
            case NAN:
                return Float::isNaN;
            case INFINITE:
                return Float::isInfinite;
            case FINITE:
                return value -> value != QueryConstants.NULL_FLOAT && Float.isFinite(value);
            case NON_ZERO:
                return value -> value != QueryConstants.NULL_FLOAT && value != 0;
            case NON_NEGATIVE:
                return value -> value >= 0; // NULL_FLOAT is negative
            case NON_POSITIVE:
                return value -> value != QueryConstants.NULL_FLOAT && value <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for float data type");
        }
    }

    public static DoubleCountFunction getDoubleCountFunction(final AggCountType countType) {
        // NOTE: we are leveraging the fact that comparisons with NaN always return false (<, >, <=, >= etc.) to
        // filter out NaN values
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
                return value -> value != QueryConstants.NULL_DOUBLE;
            case NULL:
                return value -> value == QueryConstants.NULL_DOUBLE;
            case POSITIVE:
                return value -> value > 0; // NULL_DOUBLE is negative
            case ZERO:
                return value -> value == 0;
            case NEGATIVE:
                return value -> value != QueryConstants.NULL_DOUBLE && value < 0;
            case NAN:
                return Double::isNaN;
            case INFINITE:
                return Double::isInfinite;
            case FINITE:
                return value -> value != QueryConstants.NULL_DOUBLE && Double.isFinite(value);
            case NON_ZERO:
                return value -> value != QueryConstants.NULL_DOUBLE && value != 0;
            case NON_NEGATIVE:
                return value -> value >= 0; // NULL_DOUBLE is negative
            case NON_POSITIVE:
                return value -> value != QueryConstants.NULL_DOUBLE && value <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for double data type");
        }
    }

    public static ObjectCountFunction getObjectCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
                return Objects::nonNull;
            case NULL:
                return Objects::isNull;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for object data type");
        }
    }

    public static BigDecimalCountFunction getBigDecimalCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
            case FINITE:
                return Objects::nonNull;
            case NULL:
                return Objects::isNull;
            case POSITIVE:
                return value -> value != null && value.signum() > 0;
            case ZERO:
                return value -> value != null && value.signum() == 0;
            case NEGATIVE:
                return value -> value != null && value.signum() < 0;
            case NAN:
            case INFINITE:
                return index -> false;
            case NON_ZERO:
                return value -> value != null && value.signum() != 0;
            case NON_NEGATIVE:
                return value -> value != null && value.signum() >= 0;
            case NON_POSITIVE:
                return value -> value != null && value.signum() <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for BigDecimal data type");
        }
    }

    public static BigIntegerCountFunction getBigIntegerCountFunction(final AggCountType countType) {
        switch (countType) {
            case ALL:
                return value -> true;
            case NON_NULL:
            case FINITE:
                return Objects::nonNull;
            case NULL:
                return Objects::isNull;
            case POSITIVE:
                return value -> value != null && value.signum() > 0;
            case ZERO:
                return value -> value != null && value.signum() == 0;
            case NEGATIVE:
                return value -> value != null && value.signum() < 0;
            case NAN:
            case INFINITE:
                return index -> false;
            case NON_ZERO:
                return value -> value != null && value.signum() != 0;
            case NON_NEGATIVE:
                return value -> value != null && value.signum() >= 0;
            case NON_POSITIVE:
                return value -> value != null && value.signum() <= 0;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported count type: " + countType + " for BigInteger data type");
        }
    }
    // endregion count-interfaces-functions
}
