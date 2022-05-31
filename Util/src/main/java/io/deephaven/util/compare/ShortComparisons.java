package io.deephaven.util.compare;

public class ShortComparisons {

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
