//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ZonedDateTimeCodec implements ObjectCodec<ZonedDateTime> {

    public ZonedDateTimeCodec(String args) {}

    @Override
    public byte @NotNull [] encode(@Nullable ZonedDateTime input) {
        if (input == null) {
            return CodecUtil.ZERO_LENGTH_BYTE_ARRAY;
        }

        final int bufSize = computeSize(input);
        final byte[] buf = new byte[bufSize];
        ByteBuffer bb = ByteBuffer.wrap(buf);
        bb.putLong(toEpochNano(input));

        final String zone = input.getZone().getId();
        bb.putInt(zone.length());
        bb.put(zone.getBytes(StandardCharsets.UTF_8));

        return buf;
    }

    @Nullable
    @Override
    public ZonedDateTime decode(byte @NotNull [] input, int offset, int length) {
        if (length == 0) {
            return null;
        }

        final ByteBuffer buf = ByteBuffer.wrap(input, offset, length);
        final long nanos = buf.getLong();
        final int zidLen = buf.getInt();

        final byte[] zidBytes = new byte[zidLen];
        buf.get(zidBytes, 0, zidLen);
        final String zid = new String(zidBytes, StandardCharsets.UTF_8);

        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(0, nanos), ZoneId.of(zid));
    }

    @Override
    public boolean isNullable() {
        return true;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int expectedObjectWidth() {
        return VARIABLE_WIDTH_SENTINEL;
    }

    private static int computeSize(@NotNull ZonedDateTime val) {
        return Long.BYTES + Integer.BYTES + val.getZone().getId().length();
    }

    // Sadly, this is copied from DateTimeUtils, since we cannot depend on the engine-time package.
    private static long toEpochNano(@Nullable final ZonedDateTime value) {
        if (value == null) {
            return QueryConstants.NULL_LONG;
        }

        return safeComputeNanos(value.toEpochSecond(), value.getNano());
    }

    /**
     * Representability cannot be decided from {@code epochSecond} alone. Two's-complement arithmetic is modular, so
     * {@code epochSecond * 1_000_000_000L + nanoOfSecond} is exact whenever the mathematical result fits in a
     * {@code long}, including when the multiplication alone overflows -- as it does for the bottom second of the range
     * ({@code epochSecond == -9223372037}), which the old bound plus {@code Math.addExact} rejected with an
     * {@link ArithmeticException}. Conversely a large negative {@code epochSecond} passed the bound and wrapped
     * silently to an unrelated value. Checking the combined result by dividing it back out is exact in both directions.
     */
    private static long safeComputeNanos(final long epochSecond, final long nanoOfSecond) {
        final long nanos = epochSecond * 1_000_000_000L + nanoOfSecond;
        if (Math.floorDiv(nanos, 1_000_000_000L) != epochSecond
                || Math.floorMod(nanos, 1_000_000_000L) != nanoOfSecond) {
            throw new IllegalArgumentException("Numeric overflow detected during conversion of " + epochSecond
                    + " seconds and " + nanoOfSecond + " nanoseconds to nanoseconds");
        }
        return nanos;
    }
}
