package io.deephaven.util.compare;

public class ByteComparisons {

    public static int compare(byte lhs, byte rhs) {
        return Byte.compare(lhs, rhs);
    }

    public static boolean eq(byte lhs, byte rhs) {
        return lhs == rhs;
    }

    public static boolean gt(byte lhs, byte rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(byte lhs, byte rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(byte lhs, byte rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(byte lhs, byte rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
