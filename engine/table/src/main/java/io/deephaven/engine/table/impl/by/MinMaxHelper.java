//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.util.QueryConstants;

final class MinMaxHelper {

    public static boolean isNullOrNan(byte x) {
        return x == QueryConstants.NULL_BYTE;
    }

    public static boolean isNullOrNan(char x) {
        return x == QueryConstants.NULL_CHAR;
    }

    public static boolean isNullOrNan(short x) {
        return x == QueryConstants.NULL_SHORT;
    }

    public static boolean isNullOrNan(int x) {
        return x == QueryConstants.NULL_INT;
    }

    public static boolean isNullOrNan(long x) {
        return x == QueryConstants.NULL_LONG;
    }

    public static boolean isNullOrNan(float x) {
        return x == QueryConstants.NULL_FLOAT || Float.isNaN(x);
    }

    public static boolean isNullOrNan(double x) {
        return x == QueryConstants.NULL_DOUBLE || Double.isNaN(x);
    }

    public static boolean isNullOrNan(Object o) {
        return o == null;
    }
}
