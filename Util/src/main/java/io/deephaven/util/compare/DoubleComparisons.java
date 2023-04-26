/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

public class DoubleComparisons {

    public static int compare(double lhs, double rhs) {
        // Note this intentionally makes -0.0 and 0.0 compare equal
        if (eq(lhs, rhs)) {
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
            return 1; // lhs is NaN, rhs is not
        }
        if (Double.isNaN(rhs)) {
            return -1; // rhs is NaN, lhs is not
        }
        // Neither is NULL or NaN, and they are not equal; fall back to regular comparisons
        return lhs < rhs ? -1 : 1;
    }

    public static boolean eq(double lhs, double rhs) {
        return Double.doubleToLongBits(lhs) == Double.doubleToLongBits(rhs);
    }

    public static boolean gt(double lhs, double rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(double lhs, double rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(double lhs, double rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(double lhs, double rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
