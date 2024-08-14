//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

public class CharComparisons {

    /**
     * Compares two chars according to the following rules:
     *
     * <ul>
     * <li>{@link QueryConstants#NULL_CHAR} is less than all other {@code char} values</li>
     * <li>Otherwise, normal {@code char} comparison logic is used</li>
     * </ul>
     *
     * <p>
     * Note: this differs from the Java language numerical comparison operators {@code <, <=, >=, >} and
     * {@link Character#compare(char, char)}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return the value {@code 0} if {@code lhs} is equal to {@code rhs}; a value less than {@code 0} if {@code lhs} is
     *         less than {@code rhs}; and a value greater than {@code 0} if {@code lhs} is greater than {@code rhs}
     */
    public static int compare(char lhs, char rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == QueryConstants.NULL_CHAR) {
            return -1;
        }
        if (rhs == QueryConstants.NULL_CHAR) {
            return 1;
        }
        return Character.compare(lhs, rhs);
    }

    /**
     * Compare two chars for equality consistent with {@link #compare(char, char)}; that is
     * {@code compare(lhs, rhs) == 0 ⇒ eq(lhs, rhs)} and {@code compare(lhs, rhs) != 0 ⇒ !eq(lhs, rhs)}.
     *
     * <p>
     * Logically equivalent to {@code compare(lhs, rhs) == 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} if the values are equal, {@code false} otherwise
     */
    public static boolean eq(char lhs, char rhs) {
        return lhs == rhs;
    }

    /**
     * Returns a hash code for a {@code char} value consistent with {@link #eq(char, char)}; that is,
     * {@code eq(x, y) ⇒ hashCode(x) == hashCode(y)}.
     *
     * @param x the value to hash
     * @return a hash code value for a {@code char} value
     */
    public static int hashCode(char x) {
        return Character.hashCode(x);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) > 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than {@code rhs}
     */
    public static boolean gt(char lhs, char rhs) {
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
    public static boolean lt(char lhs, char rhs) {
        // return compare(lhs, rhs) < 0;
        return (lhs < rhs || lhs == QueryConstants.NULL_CHAR) && rhs != QueryConstants.NULL_CHAR;
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) >= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is greater than or equal to {@code rhs}
     */
    public static boolean geq(char lhs, char rhs) {
        // return compare(lhs, rhs) >= 0;
        return !lt(lhs, rhs);
    }

    /**
     * Logically equivalent to {@code compare(lhs, rhs) <= 0}.
     *
     * @param lhs the first value
     * @param rhs the second value
     * @return {@code true} iff {@code lhs} is less than or equal to {@code rhs}
     */
    public static boolean leq(char lhs, char rhs) {
        // return compare(lhs, rhs) <= 0;
        return (lhs <= rhs && rhs != QueryConstants.NULL_CHAR) || lhs == QueryConstants.NULL_CHAR;
    }
}
