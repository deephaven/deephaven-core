//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

public class BooleanComparisons {

    /**
     * Compares two booleans with {@code false} before {@code true}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return the value {@code 0} if {@code lhs} is equal to {@code rhs}; a value less than {@code 0} if {@code lhs} is
     *         less than {@code rhs}; and a value greater than {@code 0} if {@code lhs} is greater than {@code rhs}
     */
    public static int compare(boolean lhs, boolean rhs) {
        return Boolean.compare(lhs, rhs);
    }

    /**
     * Compare two booleans for equality consistent with {@link #compare(boolean, boolean)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * <p>
     * Logically equivalent to {@code compare(lhs, rhs) == 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(boolean lhs, boolean rhs) {
        return lhs == rhs;
    }

    /**
     * Returns a hash code for a {@code boolean} value consistent with {@link #eq(boolean, boolean)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code boolean} value
     */
    public static int hashCode(boolean x) {
        return Boolean.hashCode(x);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) > 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than {@code rhs}
     */
    public static boolean gt(boolean lhs, boolean rhs) {
        return compare(lhs, rhs) > 0;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) < 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than {@code rhs}
     */
    public static boolean lt(boolean lhs, boolean rhs) {
        return compare(lhs, rhs) < 0;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) >= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than or equal to {@code rhs}
     */
    public static boolean geq(boolean lhs, boolean rhs) {
        return compare(lhs, rhs) >= 0;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) <= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than or equal to {@code rhs}
     */
    public static boolean leq(boolean lhs, boolean rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
