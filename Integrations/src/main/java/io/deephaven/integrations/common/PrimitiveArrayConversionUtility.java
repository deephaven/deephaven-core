/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.common;

import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;

/**
 * General purpose helper methods for array conversion methods from specific object types to/from primitive types. This
 * is specifically intended to improve performance in integration with Python, where conversion of primitive type arrays
 * involves direct copy of a memory buffer, and is much more performant than element-by-element inspection.
 */
public class PrimitiveArrayConversionUtility {
    /**
     * Translates a java.lang.Boolean array to a byte array. The mapping will be performed according to
     * {@link BooleanUtils#booleanAsByte(Boolean)}. This is the (psuedo)inverse of `translateArrayByteToBoolean`.
     *
     * @param array - the Boolean array
     * @return the byte array
     */
    public static byte[] translateArrayBooleanToByte(final Boolean[] array) {
        final byte[] out = new byte[array.length];
        for (int ai = 0; ai < array.length; ai++) {
            out[ai] = BooleanUtils.booleanAsByte(array[ai]);
        }
        return out;
    }

    /**
     * Translates a byte array to a Boolean array. The mapping will be performed according to
     * {@link BooleanUtils#byteAsBoolean(byte)}. This is the (psuedo)inverse of `translateArrayBooleanToByte`.
     *
     * @param array - the byte array
     * @return the Boolean array
     */
    public static Boolean[] translateArrayByteToBoolean(final byte[] array) {
        final Boolean[] out = new Boolean[array.length];
        for (int ai = 0; ai < array.length; ai++) {
            out[ai] = BooleanUtils.byteAsBoolean(array[ai]);
        }
        return out;
    }

    /**
     * Translates a DateTime array to a long array. The mapping will be performed according to
     * {@link DateTimeUtils#nanos(DateTime)}. This is the (psuedo)inverse of `translateArrayLongToDateTime`.
     *
     * @param array - the DateTime array
     * @return the corresponding long array
     */
    public static long[] translateArrayDateTimeToLong(final DateTime[] array) {
        final long[] out = new long[array.length];
        for (int ai = 0; ai < array.length; ai++) {
            out[ai] = DateTimeUtils.nanos(array[ai]);
        }
        return out;
    }

    /**
     * Translates a long array to a DateTime array. The mapping will be performed according to
     * {@link DateTimeUtils#nanosToTime(long)}. This is the (psuedo)inverse of `translateArrayLongToDateTime`.
     *
     * @param array - the long array
     * @return the corresponding DateTime array
     */
    public static DateTime[] translateArrayLongToDateTime(final long[] array) {
        final DateTime[] out = new DateTime[array.length];
        for (int ai = 0; ai < array.length; ai++) {
            out[ai] = DateTimeUtils.nanosToTime(array[ai]);
        }
        return out;
    }
}
