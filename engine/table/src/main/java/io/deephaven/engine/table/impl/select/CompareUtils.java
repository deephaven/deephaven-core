//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.function.Basic;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.compare.ObjectComparisons;
import io.deephaven.util.type.NumericTypeUtils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Objects;

class CompareUtils {
    /**
     * Compares two objects, which may be of different types. This is not an optimized function and is not recommended
     * for performance-critical value comparison. This is flexible, not fast.
     *
     * @param a the first object to compare
     * @param b the second object to compare
     * @return a < b returns negative value, a > b returns positive value, a == b returns 0
     */
    static int compare(Object a, Object b) {
        // Convert deephaven nulls to Java nulls.
        if (Objects.nonNull(a) && Objects.equals(a, Basic.nullValueFor(a.getClass()))) {
            a = null;
        }
        if (Objects.nonNull(b) && Objects.equals(b, Basic.nullValueFor(b.getClass()))) {
            b = null;
        }

        // Enforce NULL < non-null (Deephaven convention)
        if (a == null && b == null) {
            return 0;
        } else if (a == null) {
            return -1;
        } else if (b == null) {
            return 1;
        }

        if (a.getClass() == b.getClass()) {
            return ObjectComparisons.compare(a, b);
        }

        // (Maybe) do some lossless conversions that make comparison easier.
        if (a instanceof Instant) {
            a = DateTimeUtils.epochNanos((Instant) a);
        }
        if (b instanceof Instant) {
            b = DateTimeUtils.epochNanos((Instant) b);
        }
        if (a instanceof BigInteger) {
            a = new BigDecimal((BigInteger) a);
        }
        if (b instanceof BigInteger) {
            b = new BigDecimal((BigInteger) b);
        }

        final Class<?> aClass = a.getClass();
        final Class<?> bClass = b.getClass();

        // Test again after conversions.
        if (aClass == bClass) {
            return ObjectComparisons.compare(a, b);
        }

        // Handle comparisons between basic data types.
        if ((NumericTypeUtils.isIntegralOrChar(aClass) || NumericTypeUtils.isFloat(aClass))
                && (NumericTypeUtils.isIntegralOrChar(bClass) || NumericTypeUtils.isFloat(bClass))) {
            return comparePrimitives(a, b);
        }

        if (aClass == BigDecimal.class) {
            final BigDecimal abd = (BigDecimal) a;

            if (NumericTypeUtils.isChar(bClass)) {
                return abd.compareTo(BigDecimal.valueOf((char) b));
            }
            if (NumericTypeUtils.isIntegral(bClass)) {
                return abd.compareTo(BigDecimal.valueOf(((Number) b).longValue()));
            }
            if (NumericTypeUtils.isFloat(bClass)) {
                return abd.compareTo(BigDecimal.valueOf(((Number) b).doubleValue()));
            }
        }
        if (bClass == BigDecimal.class) {
            final BigDecimal bbd = (BigDecimal) b;
            if (NumericTypeUtils.isChar(aClass)) {
                return BigDecimal.valueOf((char) a).compareTo(bbd);
            }
            if (NumericTypeUtils.isIntegral(aClass)) {
                return BigDecimal.valueOf(((Number) a).longValue()).compareTo(bbd);
            }
            if (NumericTypeUtils.isFloat(aClass)) {
                return BigDecimal.valueOf(((Number) a).doubleValue()).compareTo(bbd);
            }
        }

        throw new IllegalArgumentException("Unable to compare " + aClass + " and " + bClass);
    }

    private static int comparePrimitives(Object a, Object b) {
        // Convert to double for other comparisons.
        double val1, val2;

        if (a instanceof Character) {
            val1 = (double) (char) a;
        } else if (a instanceof Byte) {
            val1 = (double) (byte) a;
        } else if (a instanceof Short) {
            val1 = (double) (short) a;
        } else if (a instanceof Integer) {
            val1 = (double) (int) a;
        } else if (a instanceof Long) {
            val1 = (double) (long) a;
        } else if (a instanceof Float) {
            val1 = (double) (float) a;
        } else if (a instanceof Double) {
            val1 = (double) a;
        } else {
            throw new IllegalArgumentException("Unsupported type for first argument: " + a.getClass());
        }

        if (b instanceof Character) {
            val2 = (double) (char) b;
        } else if (b instanceof Byte) {
            val2 = (double) (byte) b;
        } else if (b instanceof Short) {
            val2 = (double) (short) b;
        } else if (b instanceof Integer) {
            val2 = (double) (int) b;
        } else if (b instanceof Long) {
            val2 = (double) (long) b;
        } else if (b instanceof Float) {
            val2 = (double) (float) b;
        } else if (b instanceof Double) {
            val2 = (double) b;
        } else {
            throw new IllegalArgumentException("Unsupported type for second argument: " + b.getClass());
        }

        return Double.compare(val1, val2);
    }
}
