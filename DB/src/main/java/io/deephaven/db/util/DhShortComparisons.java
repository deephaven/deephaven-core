package io.deephaven.db.util;

public class DhShortComparisons {
    public static int compare(short lhs, short rhs) {
        return Short.compare(lhs, rhs);
    }

    public static boolean eq(short lhs, short rhs) {
        return lhs == rhs;
    }

    public static boolean gt(short lhs, short rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(short lhs, short rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(short lhs, short rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(short lhs, short rhs) {
        return compare(lhs, rhs) <= 0;
    }
}