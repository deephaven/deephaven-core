//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.compare;

public class IntComparisons {

    public static int compare(int lhs, int rhs) {
        return Integer.compare(lhs, rhs);
    }

    public static boolean eq(int lhs, int rhs) {
        return lhs == rhs;
    }

    public static boolean gt(int lhs, int rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(int lhs, int rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(int lhs, int rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(int lhs, int rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
