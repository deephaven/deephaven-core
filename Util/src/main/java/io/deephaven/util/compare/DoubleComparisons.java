//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

public class DoubleComparisons {

    private static final int ZERO_HASHCODE = Double.hashCode(0.0);

    /**
     * Compares two doubles according to the following rules:
     *
     * <ul>
     * <li>{@link QueryConstants#NULL_DOUBLE} is less than all other {@code double} values (including
     * {@link Double#NEGATIVE_INFINITY})</li>
     * <li>{@code 0.0} and {@code -0.0} are equal</li>
     * <li>{@link Double#NaN} (and all other {@code double} {@code NaN} representations) is equal to {@link Double#NaN}
     * and greater than all other {@code double} values (including {@link Double#POSITIVE_INFINITY})</li>
     * <li>Otherwise, normal {@code double} comparison logic is used</li>
     * </ul>
     *
     * <p>
     * Note: this differs from the Java language numerical comparison operators {@code <, <=, ==, >=, >} and
     * {@link Double#compare(double, double)}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return the value {@code 0} if {@code lhs} is equal to {@code rhs}; a value less than {@code 0} if {@code lhs} is
     *         less than {@code rhs}; and a value greater than {@code 0} if {@code lhs} is greater than {@code rhs}
     */
    public static int compare(double lhs, double rhs) {
        // Note this intentionally makes -0.0 and 0.0 compare equal
        if (lhs == rhs) {
            return 0;
        }
        // One could be NULL, but not both
        if (lhs == QueryConstants.NULL_DOUBLE) {
            return -1;
        }
        if (rhs == QueryConstants.NULL_DOUBLE) {
            return 1;
        }
        // One or both could be NaN
        if (Double.isNaN(lhs)) {
            if (Double.isNaN(rhs)) {
                return 0; // Both NaN
            }
            return 1; // lhs is NaN, rhs is not
        }
        if (Double.isNaN(rhs)) {
            return -1; // rhs is NaN, lhs is not
        }
        // Neither is NULL or NaN, and they are not equal; fall back to regular comparisons
        return lhs < rhs ? -1 : 1;
    }

    /**
     * Compare two doubles for equality consistent with {@link #compare(double, double)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * <p>
     * Logically equivalent to {@code compare(lhs, rhs) == 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(double lhs, double rhs) {
        return lhs == rhs || (Double.isNaN(lhs) && Double.isNaN(rhs));
    }

    /**
     * Returns a hash code for a {@code double} value consistent with {@link #eq(double, double)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code double} value
     */
    public static int hashCode(double x) {
        // Note this intentionally makes -0.0 and 0.0 hashcode equal
        return x == 0.0
                ? ZERO_HASHCODE
                : Double.hashCode(x);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) > 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than {@code rhs}
     */
    public static boolean gt(double lhs, double rhs) {
        // return compare(lhs, rhs) > 0;
        return !leq(lhs, rhs);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) < 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than {@code rhs}
     */
    public static boolean lt(double lhs, double rhs) {
        // return compare(lhs, rhs) < 0;
        return !geq(lhs, rhs);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) >= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than or equal to {@code rhs}
     */
    public static boolean geq(double lhs, double rhs) {
        // return compare(lhs, rhs) >= 0;
        return (lhs >= rhs && lhs != QueryConstants.NULL_DOUBLE)
                || Double.isNaN(lhs)
                || rhs == QueryConstants.NULL_DOUBLE;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) <= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than or equal to {@code rhs}
     */
    public static boolean leq(double lhs, double rhs) {
        // return compare(lhs, rhs) <= 0;
        return (lhs <= rhs && rhs != QueryConstants.NULL_DOUBLE)
                || lhs == QueryConstants.NULL_DOUBLE
                || Double.isNaN(rhs);
    }
}
