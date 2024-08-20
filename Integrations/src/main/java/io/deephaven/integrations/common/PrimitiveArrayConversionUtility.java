//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.common;

import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.BooleanUtils;

import java.time.Instant;

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
     * Translates an Instant array to a long array. The mapping will be performed according to
     * {@link DateTimeUtils#epochNanos(Instant)}. This is the (psuedo)inverse of `translateArrayLongToInstant`.
     *
     * @param array - the Instant array
     * @return the corresponding long array
     */
    public static long[] translateArrayInstantToLong(final Instant[] array) {
        final long[] out = new long[array.length];
        for (int ai = 0; ai < array.length; ai++) {
            out[ai] = DateTimeUtils.epochNanos(array[ai]);
        }
        return out;
    }

    /**
     * Translates a long array to an Instant array. The mapping will be performed according to
     * {@link DateTimeUtils#epochNanosToInstant(long)}. This is the (psuedo)inverse of `translateArrayLongToInstant`.
     *
     * @param array - the long array
     * @return the corresponding Instant array
     */
    public static Instant[] translateArrayLongToInstant(final long[] array) {
        final Instant[] out = new Instant[array.length];
        for (int ai = 0; ai < array.length; ai++) {
            out[ai] = DateTimeUtils.epochNanosToInstant(array[ai]);
        }
        return out;
    }

    /**
     * Translates a String array to an Instant array. The mapping will be performed according to
     * {@link DateTimeUtils#parseInstant(String)}.
     *
     * @param array - the string array
     * @return the corresponding Instant array
     */
    public static Instant[] translateArrayStringToInstant(final String[] array) {
        final Instant[] out = new Instant[array.length];
        for (int ai = 0; ai < array.length; ai++) {
            out[ai] = array[ai] == null ? null : DateTimeUtils.parseInstant(array[ai]);
        }
        return out;
    }
}
