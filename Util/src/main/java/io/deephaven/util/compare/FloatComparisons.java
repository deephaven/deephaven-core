//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

public class FloatComparisons {

    private static final int ZERO_HASHCODE = Float.hashCode(0.0f);

    /**
     * Compares two floats according to the following rules:
     *
     * <ul>
     * <li>{@link QueryConstants#NULL_FLOAT} is less than all other {@code float} values (including
     * {@link Float#NEGATIVE_INFINITY})</li>
     * <li>{@code 0.0} and {@code -0.0} are equal</li>
     * <li>{@link Float#NaN} (and all other float {@code NaN} representations) is equal to {@link Float#NaN} and greater
     * than all other {@code float} values (including {@link Float#POSITIVE_INFINITY})</li>
     * <li>Otherwise, normal {@code float} comparison logic is used</li>
     * </ul>
     *
     * <p>
     * Note: this differs from the Java language numerical comparison operators {@code <, <=, ==, >=, >} and
     * {@link Float#compare(float, float)}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return the value {@code 0} if {@code lhs} is equal to {@code rhs}; a value less than {@code 0} if {@code lhs} is
     *         less than {@code rhs}; and a value greater than {@code 0} if {@code lhs} is greater than {@code rhs}
     */
    public static int compare(float lhs, float rhs) {
        // Note this intentionally makes -0.0 and 0.0 compare equal
        if (lhs == rhs) {
            return 0;
        }
        // One could be NULL, but not both
        if (lhs == QueryConstants.NULL_FLOAT) {
            return -1;
        }
        if (rhs == QueryConstants.NULL_FLOAT) {
            return 1;
        }
        // One or both could be NaN
        if (Float.isNaN(lhs)) {
            if (Float.isNaN(rhs)) {
                return 0; // Both NaN
            }
            return 1; // lhs is NaN, rhs is not
        }
        if (Float.isNaN(rhs)) {
            return -1; // rhs is NaN, lhs is not
        }
        // Neither is NULL or NaN, and they are not equal; fall back to regular comparisons
        return lhs < rhs ? -1 : 1;
    }

    /**
     * Compare two floats for equality consistent with {@link #compare(float, float)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * <p>
     * Logically equivalent to {@code compare(lhs, rhs) == 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(float lhs, float rhs) {
        return lhs == rhs || (Float.isNaN(lhs) && Float.isNaN(rhs));
    }

    /**
     * Returns a hash code for a {@code float} value consistent with {@link #eq(float, float)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code float} value
     */
    public static int hashCode(float x) {
        // Note this intentionally makes -0.0f and 0.0f hashcode equal
        return x == 0.0f
                ? ZERO_HASHCODE
                : Float.hashCode(x);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) > 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than {@code rhs}
     */
    public static boolean gt(float lhs, float rhs) {
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
    public static boolean lt(float lhs, float rhs) {
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
    public static boolean geq(float lhs, float rhs) {
        // return compare(lhs, rhs) >= 0;
        return (lhs >= rhs && lhs != QueryConstants.NULL_FLOAT)
                || Float.isNaN(lhs)
                || rhs == QueryConstants.NULL_FLOAT;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) <= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than or equal to {@code rhs}
     */
    public static boolean leq(float lhs, float rhs) {
        // return compare(lhs, rhs) <= 0;
        return (lhs <= rhs && rhs != QueryConstants.NULL_FLOAT)
                || lhs == QueryConstants.NULL_FLOAT
                || Float.isNaN(rhs);
    }
}
