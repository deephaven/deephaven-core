//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import java.util.Objects;

public class ObjectComparisons {

    /**
     * Compares two Objects according to the following rules:
     *
     * <ul>
     * <li>{@code null} is less than all other values</li>
     * <li>Otherwise, {@link Comparable#compareTo(Object)} is used</li>
     * </ul>
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return the value {@code 0} if {@code lhs} is equal to {@code rhs}; a value less than {@code 0} if {@code lhs} is
     *         less than {@code rhs}; and a value greater than {@code 0} if {@code lhs} is greater than {@code rhs}
     */
    public static int compare(Object lhs, Object rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return -1;
        }
        if (rhs == null) {
            return 1;
        }
        // noinspection unchecked,rawtypes
        return ((Comparable) lhs).compareTo(rhs);
    }

    /**
     * Compare two Objects for equality consistent with {@link #compare(Object, Object)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * <p>
     * Logically equivalent to {@code compare(lhs, rhs) == 0}.
     * 
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(Object lhs, Object rhs) {
        return Objects.equals(lhs, rhs);
    }

    /**
     * Returns a hash code for an {@code Object} value consistent with {@link #eq(Object, Object)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * @param x the value to hash
     * @return a hash code value for an {@code Object} value
     */
    public static int hashCode(Object x) {
        return Objects.hashCode(x);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) > 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than {@code rhs}
     */
    public static boolean gt(Object lhs, Object rhs) {
        return compare(lhs, rhs) > 0;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) < 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than {@code rhs}
     */
    public static boolean lt(Object lhs, Object rhs) {
        return compare(lhs, rhs) < 0;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) >= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than or equal to {@code rhs}
     */
    public static boolean geq(Object lhs, Object rhs) {
        return compare(lhs, rhs) >= 0;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) <= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than or equal to {@code rhs}
     */
    public static boolean leq(Object lhs, Object rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
