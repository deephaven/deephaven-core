/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

public class CacheLogUtils {

    public static Boolean enumToBoolean(final int value, final int YES_CONSTANT, final int NO_CONSTANT) {
        if (value == YES_CONSTANT) {
            return Boolean.TRUE;
        }
        if (value == NO_CONSTANT) {
            return Boolean.FALSE;
        }
        if (value == Integer.MIN_VALUE) {
            return null;
        }
        throw new IllegalArgumentException(
                "Unexpected value=" + value + ", YES_CONSTANT=" + YES_CONSTANT + ", NO_CONSTANT=" + NO_CONSTANT);
    }
}
