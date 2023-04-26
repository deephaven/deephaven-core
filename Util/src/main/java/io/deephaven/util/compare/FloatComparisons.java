/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

public class FloatComparisons {

    public static int compare(float lhs, float rhs) {
        // Note this intentionally makes -0.0 and 0.0 compare equal
        if (eq(lhs, rhs)) {
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
            return 1; // lhs is NaN, rhs is not
        }
        if (Float.isNaN(rhs)) {
            return -1; // rhs is NaN, lhs is not
        }
        // Neither is NULL or NaN, and they are not equal; fall back to regular comparisons
        return lhs < rhs ? -1 : 1;
    }

    public static boolean eq(float lhs, float rhs) {
        return Float.floatToIntBits(lhs) == Float.floatToIntBits(rhs);
    }

    public static boolean gt(float lhs, float rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(float lhs, float rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(float lhs, float rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(float lhs, float rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
