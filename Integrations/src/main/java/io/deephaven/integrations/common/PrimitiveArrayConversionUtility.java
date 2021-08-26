/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.common;

import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.util.QueryConstants;

/**
 * General purpose helper methods for array conversion methods from specific object types to/from
 * primitive types. This is specifically intended to improve performance in integration with Python,
 * where conversion of primitive type arrays involves direct copy of a memory buffer, and is much
 * more performant than element-by-element inspection.
 */
public class PrimitiveArrayConversionUtility {
    /**
     * Translates a java.lang.Boolean array to a byte array. The mapping will be performed as null
     * -> -1, false -> 0, and true -> 1. This is the (psuedo)inverse of
     * `translateArrayByteToBoolean`.
     *
     * @param array - the Boolean array
     * @return the byte array
     */
    public static byte[] translateArrayBooleanToByte(final Boolean[] array) {
        byte[] out = new byte[array.length];
        for (int i = 0; i < array.length; i++) {
            Boolean element = array[i];
            out[i] = (byte) (element == null ? -1 : (element ? 1 : 0));
        }
        return out;
    }

    /**
     * Translates a byte array to a Boolean array. The mapping will be performed as <0 -> null, 0 ->
     * false, >0 -> true. This is the (psuedo)inverse of `translateArrayBooleanToByte`.
     *
     * @param array - the byte array
     * @return the Boolean array
     */
    public static Boolean[] translateArrayByteToBoolean(final byte[] array) {
        Boolean[] out = new Boolean[array.length];
        for (int i = 0; i < array.length; i++) {
            byte element = array[i];
            out[i] = (element < 0 ? null : (element > 0));
        }
        return out;
    }

    /**
     * Translates a DBDateTime array to a long array. This is the (psuedo)inverse of
     * `translateArrayLongToDBDateTime`.
     *
     * @param array - the DBDateTime array
     * @return the corresponding long array
     */
    public static long[] translateArrayDBDateTimeToLong(final DBDateTime[] array) {
        long[] out = new long[array.length];
        for (int i = 0; i < array.length; i++) {
            DBDateTime element = array[i];
            out[i] = (element == null ? QueryConstants.NULL_LONG : element.getNanos());
        }
        return out;
    }

    /**
     * Translates a long array to a DBDateTime array. This is the (psuedo)inverse of
     * `translateArrayLongToDBDateTime`.
     *
     * @param array - the long array
     * @return the corresponding DBDateTime array
     */
    public static DBDateTime[] translateArrayLongToDBDateTime(final long[] array) {
        DBDateTime[] out = new DBDateTime[array.length];
        for (int i = 0; i < array.length; i++) {
            out[i] = new DBDateTime(array[i]);
        }
        return out;
    }
}
