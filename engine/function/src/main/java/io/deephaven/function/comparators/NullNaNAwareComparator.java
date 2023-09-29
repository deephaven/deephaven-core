/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.function.comparators;

import java.lang.Number;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Comparator;

import static io.deephaven.function.Basic.isNull;
import static io.deephaven.function.Numeric.isNaN;

/**
 * Comparator which is aware of NaN and Null values, in the Deephaven convention.
 *
 * @param <T> type to compare.
 */
public class NullNaNAwareComparator<T> implements Comparator<T> {

    /**
     * Creates a comparator.
     */
    public NullNaNAwareComparator() {}

    private static <T> boolean isNanVal(T v) {
        if (v instanceof Float) {
            return Float.isNaN((Float) v);
        } else if (v instanceof Double) {
            return Double.isNaN((Double) v);
        } else {
            return false;
        }
    }

    @Override
    public int compare(final T o1, final T o2) {
        final boolean isNull1 = isNull(o1);
        final boolean isNull2 = isNull(o2);

        if (isNull1 && isNull2) {
            return 0;
        } else if (isNull1) {
            return -1;
        } else if (isNull2) {
            return 1;
        }

        final boolean isNaN1 = isNanVal(o1);
        final boolean isNaN2 = isNanVal(o2);

        // NaN is considered the greatest
        if (isNaN1 && isNaN2) {
            return 0;
        } else if (isNaN1) {
            return 1;
        } else if (isNaN2) {
            return -1;
        }

        if (o1 instanceof Number && o2 instanceof Number) {
            return toBigDecimal((Number)o1).compareTo(toBigDecimal((Number)o2));
        } else if (o1 instanceof Comparable && o2 instanceof Comparable) {
            return ((Comparable)o1).compareTo(o2);
        } else {
            throw new UnsupportedOperationException("Input types are not java.lang.Number or java.lang.Comparable: (" + o1.getClass() + ", " + o2.getClass() + ")");
        }
    }

    /**
     * Convert a number to a BigDecimal.
     * 
     * @param x a number
     * @return number represented as a BigDecimal
     */
    protected static BigDecimal toBigDecimal(final Number x) {
        if (x instanceof Byte || x instanceof Short || x instanceof Integer || x instanceof Long) {
            return new BigDecimal(x.longValue());
        } else if (x instanceof Float || x instanceof Double) {
            return new BigDecimal(x.doubleValue());
        } else if (x instanceof BigInteger) {
            return new BigDecimal((BigInteger) x);
        } else if (x instanceof BigDecimal) {
            return (BigDecimal) x;
        } else {
            throw new UnsupportedOperationException("Unsupported java.lang.Number data type passed in to toBigDecimal (" + x.getClass() + ")");
        }
    }
}
