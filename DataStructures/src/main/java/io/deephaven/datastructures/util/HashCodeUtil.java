/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.datastructures.util;

import java.util.Date;

public class HashCodeUtil {

    public static int toHashCode(double val) {
        // many bit representations for NaN
        if (Double.isNaN(val))
            return 0;

        long t = Double.doubleToLongBits(val);
        return (int) (t ^ (t >>> 32));
    }

    public static int toHashCode(long val) {
        return (int) (val ^ (val >>> 32));
    }

    public static int toHashCode(Object obj) {
        return (obj == null) ? 0 : obj.hashCode();
    }

    public static int combineHashCodes(Object... values) {
        return createHashCode(values);
    }

    public static int createHashCode(Object[] values) {
        int result = 37, hash;

        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) {
                hash = 0;
            } else {
                Class<?> type = values[i].getClass();

                if (type == String.class) {
                    String str = (String) values[i];
                    if (str != null)
                        hash = str.hashCode();
                    else
                        hash = 0;
                } else if (type == Integer.class) {
                    hash = ((Integer) values[i]);
                } else if (type == Double.class) {
                    double val = ((Double) values[i]);
                    if (Double.isNaN(val))
                        hash = 0;
                    else {
                        long t = Double.doubleToLongBits(val);
                        hash = (int) (t ^ (t >>> 32));
                    }
                } else if (type == Long.class) {
                    long t = ((Long) values[i]);
                    hash = (int) (t ^ (t >>> 32));
                } else if (type == Date.class) {
                    long t = ((Date) values[i]).getTime();
                    hash = (int) (t ^ (t >>> 32));
                } else if (type == Byte.class) {
                    hash = ((Byte) values[i]);
                } else if (type == Boolean.class) {
                    hash = values[i].hashCode(); // the hashcode should be fine in this case
                } else if (type.isEnum()) {
                    hash = ((Enum<?>) values[i]).ordinal();
                } else {
                    hash = values[i].hashCode();
                }
            }
            result = result * 17 + hash;
        }

        return result;
    }
}
