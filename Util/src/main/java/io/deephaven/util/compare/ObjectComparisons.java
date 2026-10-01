//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
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
     * Compare two Objects for equality using {@link Objects#equals(Object, Object)}; this is consistent with
     * {@link #hashCode(Object)}, that is {@code eq(x, y) => hashCode(x) == hashCode(y)}.
     *
     * <p>
     * For types whose {@link Comparable#compareTo(Object) compareTo} is not consistent with
     * {@link Object#equals(Object) equals} (e.g., {@link java.math.BigDecimal} 1.0 and 1.00), {@code eq} may be false
     * for values where {@code compare(lhs, rhs) == 0}. Use {@link #compareEquals(Object, Object)} for equality that is
     * consistent with {@link #compare(Object, Object)}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(Object lhs, Object rhs) {
        return Objects.equals(lhs, rhs);
    }

    /**
     * Compare two Objects for equality consistent with {@link #compare(Object, Object)}; logically equivalent to
     * {@code compare(lhs, rhs) == 0}.
     *
     * <p>
     * This equality is suitable for any ordering in which distinct objects that are not {@link Object#equals(Object)
     * equals} may share an equivalence class (e.g., {@link java.math.BigDecimal} 1.0 and 1.00). Unlike
     * {@link #eq(Object, Object)}, it is not consistent with {@link #hashCode(Object)}, so it must not be used for
     * hashing.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if {@code compare(lhs, rhs) == 0}, {@code false} otherwise
     */
    public static boolean compareEquals(Object lhs, Object rhs) {
        return compare(lhs, rhs) == 0;
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
