package io.deephaven.db.util;

import io.deephaven.util.QueryConstants;

public class DhCharComparisons {
    public static int compare(char lhs, char rhs) {
        if (lhs == rhs) {
            return 0;
        }
        final boolean lhsNull = lhs == QueryConstants.NULL_CHAR;
        final boolean rhsNull = rhs == QueryConstants.NULL_CHAR;
        if (lhsNull) {
            return -1;
        }
        if (rhsNull) {
            return 1;
        }
        return Character.compare(lhs, rhs);
    }

    public static boolean eq(char lhs, char rhs) {
        return lhs == rhs;
    }

    public static boolean gt(char lhs, char rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(char lhs, char rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(char lhs, char rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(char lhs, char rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
