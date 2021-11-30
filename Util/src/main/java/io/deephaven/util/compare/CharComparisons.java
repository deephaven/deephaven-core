package io.deephaven.util.compare;

import io.deephaven.util.QueryConstants;

public class CharComparisons {

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
