//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.PageValueReader;

import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MAX_CONVERTIBLE_MICROS;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MAX_CONVERTIBLE_MILLIS;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MICRO;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.MILLI;
import static io.deephaven.parquet.base.materializers.ParquetMaterializerUtils.NANO;

/**
 * Utility methods for converting the raw primitive values read via {@link PageValueReader} (or extracted directly from
 * Parquet page statistics) into the corresponding Deephaven column values. Shared by {@link PageMaterializer}
 * implementations and by statistics-based min/max extraction.
 */
public final class PageValueConversions {

    private PageValueConversions() {}

    public static byte byteFromInt(int value) {
        return (byte) value;
    }

    public static char charFromInt(int value) {
        return (char) value;
    }

    public static short shortFromInt(int value) {
        return (short) value;
    }

    public static byte byteFromBoolean(boolean value) {
        return (byte) (value ? 1 : 0);
    }

    public static short shortFromBoolean(boolean value) {
        return (short) (value ? 1 : 0);
    }

    public static int intFromBoolean(boolean value) {
        return value ? 1 : 0;
    }

    public static long longFromBoolean(boolean value) {
        return value ? 1 : 0;
    }

    public static int intFromUnsignedByte(int value) {
        return Byte.toUnsignedInt((byte) value);
    }

    public static int intFromUnsignedShort(int value) {
        return Short.toUnsignedInt((short) value);
    }

    public static short shortFromUnsignedByte(int value) {
        return (short) Byte.toUnsignedInt((byte) value);
    }

    public static long longFromUnsignedByte(int value) {
        return Byte.toUnsignedLong((byte) value);
    }

    public static long longFromUnsignedShort(int value) {
        return Short.toUnsignedLong((short) value);
    }

    public static long longFromUnsignedInt(int value) {
        return Integer.toUnsignedLong(value);
    }

    /**
     * An unsigned value exceeds {@link Long#MAX_VALUE} exactly when its signed bit pattern is negative.
     */
    public static long longFromUnsignedLong(long value) {
        if (value < 0) {
            throw new UncheckedDeephavenException("Unsigned long value " + Long.toUnsignedString(value)
                    + " is too large to be represented as a long");
        }
        return value;
    }

    /**
     * Mirrors {@code com.google.common.primitives.UnsignedLong}.
     */
    public static BigInteger bigIntegerFromUnsignedLong(long value) {
        final BigInteger magnitude = BigInteger.valueOf(value & Long.MAX_VALUE);
        return value < 0 ? magnitude.setBit(Long.SIZE - 1) : magnitude;
    }

    public static LocalDate localDateFromEpochDay(int value) {
        return LocalDate.ofEpochDay(value);
    }

    public static LocalTime localTimeFromMillisOfDay(int value) {
        return LocalTime.ofNanoOfDay(value * MILLI);
    }

    public static LocalTime localTimeFromMicrosOfDay(long value) {
        return LocalTime.ofNanoOfDay(value * MICRO);
    }

    public static LocalTime localTimeFromNanosOfDay(long value) {
        return LocalTime.ofNanoOfDay(value * NANO);
    }

    /**
     * Converts milliseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     */
    public static LocalDateTime localDateTimeFromEpochMillis(long value) {
        return LocalDateTime.ofEpochSecond(value / 1_000L, (int) ((value % 1_000L) * MILLI), ZoneOffset.UTC);
    }

    /**
     * Converts microseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     */
    public static LocalDateTime localDateTimeFromEpochMicros(long value) {
        return LocalDateTime.ofEpochSecond(value / 1_000_000L, (int) ((value % 1_000_000L) * MICRO), ZoneOffset.UTC);
    }

    /**
     * Converts nanoseconds from the Epoch to a {@link LocalDateTime} in UTC timezone.
     */
    public static LocalDateTime localDateTimeFromEpochNanos(long value) {
        return LocalDateTime.ofEpochSecond(value / 1_000_000_000L, (int) ((value % 1_000_000_000L) * NANO),
                ZoneOffset.UTC);
    }

    public static long instantNanosFromEpochMillis(long value) {
        if (value > MAX_CONVERTIBLE_MILLIS || value < -MAX_CONVERTIBLE_MILLIS) {
            throw new UncheckedDeephavenException("Converting " + value + " millis to nanos would overflow");
        }
        return value * MILLI;
    }

    public static long instantNanosFromEpochMicros(long value) {
        if (value > MAX_CONVERTIBLE_MICROS || value < -MAX_CONVERTIBLE_MICROS) {
            throw new UncheckedDeephavenException("Converting " + value + " micros to nanos would overflow");
        }
        return value * MICRO;
    }
}
