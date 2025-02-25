//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.QueryConstants;
import org.apache.arrow.vector.types.TimeUnit;

abstract class FactoryHelper {

    /**
     * Converts a {@link TimeUnit} to a factor for converting to nanoseconds.
     *
     * @param unit The {@link TimeUnit} to convert.
     * @return The factor for converting to nanoseconds.
     */
    static long factorForTimeUnit(final TimeUnit unit) {
        switch (unit) {
            case NANOSECOND:
                return 1;
            case MICROSECOND:
                return 1000;
            case MILLISECOND:
                return 1000 * 1000L;
            case SECOND:
                return 1000 * 1000 * 1000L;
            default:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Unexpected time unit value: " + unit);
        }
    }

    /**
     * Applies a mask to handle overflow for unsigned values by constraining the value to the range that can be
     * represented with the specified number of bytes.
     * <p>
     * This method ensures that negative values (in the case of unsigned inputs) are masked to fit within the valid
     * range for the given number of bytes, effectively wrapping them around to their equivalent unsigned
     * representation.
     * <p>
     * Special handling is included to preserve the value of null-equivalent constants and to skip masking for signed
     * values.
     * <p>
     * Note that short can only be sign extended from byte so we don't need to consider other numByte configurations.
     *
     * @param unsigned Whether the value should be treated as unsigned.
     * @param value The input value to potentially mask.
     * @return The masked value if unsigned and overflow occurs; otherwise, the original value.
     */
    @SuppressWarnings("SameParameterValue")
    static short maskIfOverflow(final boolean unsigned, short value) {
        if (unsigned && value != QueryConstants.NULL_SHORT) {
            value &= (short) ((1L << 8) - 1);
        }
        return value;
    }

    /**
     * Applies a mask to handle overflow for unsigned values by constraining the value to the range that can be
     * represented with the specified number of bytes.
     * <p>
     * This method ensures that negative values (in the case of unsigned inputs) are masked to fit within the valid
     * range for the given number of bytes, effectively wrapping them around to their equivalent unsigned
     * representation.
     * <p>
     * Special handling is included to preserve the value of null-equivalent constants and to skip masking for signed
     * values.
     *
     * @param unsigned Whether the value should be treated as unsigned.
     * @param numBytes The number of bytes to constrain the value to (e.g., 1 for byte, 2 for short).
     * @param value The input value to potentially mask.
     * @return The masked value if unsigned and overflow occurs; otherwise, the original value.
     */
    static int maskIfOverflow(final boolean unsigned, final int numBytes, int value) {
        if (unsigned && value != QueryConstants.NULL_INT) {
            value &= (int) ((1L << (numBytes * 8)) - 1);
        }
        return value;
    }

    /**
     * Applies a mask to handle overflow for unsigned values by constraining the value to the range that can be
     * represented with the specified number of bytes.
     * <p>
     * This method ensures that negative values (in the case of unsigned inputs) are masked to fit within the valid
     * range for the given number of bytes, effectively wrapping them around to their equivalent unsigned
     * representation.
     * <p>
     * Special handling is included to preserve the value of null-equivalent constants and to skip masking for signed
     * values.
     *
     * @param unsigned Whether the value should be treated as unsigned.
     * @param numBytes The number of bytes to constrain the value to (e.g., 1 for byte, 2 for short).
     * @param value The input value to potentially mask.
     * @return The masked value if unsigned and overflow occurs; otherwise, the original value.
     */
    static long maskIfOverflow(final boolean unsigned, final int numBytes, long value) {
        if (unsigned && value != QueryConstants.NULL_LONG) {
            value &= ((1L << (numBytes * 8)) - 1);
        }
        return value;
    }
}
