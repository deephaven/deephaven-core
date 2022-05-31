package io.deephaven.util.compare;

public class LongComparisons {

    public static int compare(long lhs, long rhs) {
        return Long.compare(lhs, rhs);
    }

    public static boolean eq(long lhs, long rhs) {
        return lhs == rhs;
    }

    public static boolean gt(long lhs, long rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(long lhs, long rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(long lhs, long rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(long lhs, long rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
