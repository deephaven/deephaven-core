//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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
    static final long MAX_CONVERTIBLE_SECONDS = (Long.MAX_VALUE / 1_000_000_000L) - 1;

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

    private static long safeComputeNanos(long epochSecond, long nanoOfSecond) {
        if (epochSecond > MAX_CONVERTIBLE_SECONDS) {
            throw new IllegalArgumentException(
                    "Numeric overflow detected during conversion of " + epochSecond + " to nanoseconds");
        }

        return Math.addExact(epochSecond * 1_000_000_000L, nanoOfSecond);
    }
}
