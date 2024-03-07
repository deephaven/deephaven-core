//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.util;

import io.deephaven.util.QueryConstants;

public class NullSafeAddition {
    public static long plusLong(long a, long b) {
        if (a == QueryConstants.NULL_LONG) {
            return b;
        } else if (b == QueryConstants.NULL_LONG) {
            return a;
        } else {
            return a + b;
        }
    }

    public static long minusLong(long a, long b) {
        if (a == QueryConstants.NULL_LONG) {
            return -b;
        } else if (b == QueryConstants.NULL_LONG) {
            return a;
        } else {
            return a - b;
        }
    }

    public static double plusDouble(double a, double b) {
        if (a == QueryConstants.NULL_DOUBLE) {
            return b;
        } else if (b == QueryConstants.NULL_DOUBLE) {
            return a;
        } else {
            return a + b;
        }
    }
}
