/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util;

/**
 * Utilities for translating boxed {@link Boolean}s to/from primitive bytes.
 */
public class BooleanUtils extends io.deephaven.util.BooleanUtils {
    /**
     * Convert {@code byteValue} to a Boolean.
     *
     * @param byteValue the byte to convert to a boxed boolean
     *
     * @return the boxed boolean represented by byteValue
     */
    public static Boolean byteAsBoolean(final byte byteValue) {
        return io.deephaven.util.BooleanUtils.byteAsBoolean(byteValue);
    }

    /**
     * Convert {@code booleanValue} to a byte.
     *
     * @param booleanValue the boxed boolean value to convert to a byte
     *
     * @return booleanValue represented as a byte
     */
    public static byte booleanAsByte(final Boolean booleanValue) {
        return io.deephaven.util.BooleanUtils.booleanAsByte(booleanValue);
    }
}
