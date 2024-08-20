//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

public class ShortComparisons {

    /**
     * Compares two shorts according to the following rules:
     *
     * <ul>
     * <li>{@link QueryConstants#NULL_SHORT} is less than all other {@code short} values</li>
     * <li>Otherwise, normal {@code short} comparison logic is used</li>
     * </ul>
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return the value {@code 0} if {@code lhs} is equal to {@code rhs}; a value less than {@code 0} if {@code lhs} is
     *         less than {@code rhs}; and a value greater than {@code 0} if {@code lhs} is greater than {@code rhs}
     */
    public static int compare(short lhs, short rhs) {
        return Short.compare(lhs, rhs);
    }

    /**
     * Compare two shorts for equality consistent with {@link #compare(short, short)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * <p>
     * Logically equivalent to {@code compare(lhs, rhs) == 0}.
     * 
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(short lhs, short rhs) {
        return lhs == rhs;
    }

    /**
     * Returns a hash code for a {@code short} value consistent with {@link #eq(short, short)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code short} value
     */
    public static int hashCode(short x) {
        return Short.hashCode(x);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) > 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than {@code rhs}
     */
    public static boolean gt(short lhs, short rhs) {
        return lhs > rhs;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) < 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than {@code rhs}
     */
    public static boolean lt(short lhs, short rhs) {
        return lhs < rhs;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) >= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than or equal to {@code rhs}
     */
    public static boolean geq(short lhs, short rhs) {
        return lhs >= rhs;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) <= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than or equal to {@code rhs}
     */
    public static boolean leq(short lhs, short rhs) {
        return lhs <= rhs;
    }
}
