/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

/**
 * Utilities for translating boxed {@link Boolean}s to/from primitive bytes.
 */
public class BooleanUtils {

    /**
     * The byte encoding of null booleans.
     */
    public static final byte NULL_BOOLEAN_AS_BYTE = (byte) -1;

    /**
     * The byte encoding of true booleans.
     */
    public static final byte TRUE_BOOLEAN_AS_BYTE = (byte) 1;

    /**
     * The byte encoding of false booleans.
     */
    public static final byte FALSE_BOOLEAN_AS_BYTE = (byte) 0;

    /**
     * The byte encoding of the null boolean, as a boxed Byte.
     */
    public static final Byte NULL_BOOLEAN_AS_BYTE_BOXED = NULL_BOOLEAN_AS_BYTE;

    /**
     * Convert {@code byteValue} to a Boolean.
     *
     * @param byteValue the byte to convert to a boxed boolean
     *
     * @return the boxed boolean represented by byteValue
     */
    public static Boolean byteAsBoolean(final byte byteValue) {
        // noinspection AutoBoxing
        return byteValue == NULL_BOOLEAN_AS_BYTE ? null : (byteValue != FALSE_BOOLEAN_AS_BYTE);
    }

    /**
     * Convert {@code booleanValue} to a byte.
     *
     * @param booleanValue the boxed boolean value to convert to a byte
     *
     * @return booleanValue represented as a byte
     */
    public static byte booleanAsByte(final Boolean booleanValue) {
        return booleanValue == null ? NULL_BOOLEAN_AS_BYTE
                : booleanValue ? TRUE_BOOLEAN_AS_BYTE : FALSE_BOOLEAN_AS_BYTE;
    }
}
