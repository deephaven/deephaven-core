/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.compare;

import java.util.Objects;

public class ObjectComparisons {

    public static int compare(Object lhs, Object rhs) {
        if (lhs == rhs) {
            return 0;
        }
        if (lhs == null) {
            return -1;
        }
        if (rhs == null) {
            return 1;
        }
        // noinspection unchecked,rawtypes
        return ((Comparable) lhs).compareTo(rhs);
    }

    public static boolean eq(Object lhs, Object rhs) {
        return Objects.equals(lhs, rhs);
    }

    public static boolean gt(Object lhs, Object rhs) {
        return compare(lhs, rhs) > 0;
    }

    public static boolean lt(Object lhs, Object rhs) {
        return compare(lhs, rhs) < 0;
    }

    public static boolean geq(Object lhs, Object rhs) {
        return compare(lhs, rhs) >= 0;
    }

    public static boolean leq(Object lhs, Object rhs) {
        return compare(lhs, rhs) <= 0;
    }
}
