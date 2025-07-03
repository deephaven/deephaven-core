//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.time.DateTimeUtils;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InstantNanosFromInt96MaterializerTest {

    private static final long OFFSET = InstantNanosFromInt96Materializer.getReferenceTimeZone("UTC");
    private static final long NANOS_PER_DAY = DateTimeUtils.DAY;
    private static final int JULIAN_BASE = InstantNanosFromInt96Materializer.JULIAN_OFFSET_TO_UNIX_EPOCH_DAYS;

    @Test
    final void roundTripInt96() {
        final long[] testValues = {
                0L,
                1_234_567_890_123_456L,
                NANOS_PER_DAY + 789};
        for (final long epochNanos : testValues) {
            final byte[] int96 = encodeAsInt96(epochNanos);
            final long decoded = InstantNanosFromInt96Materializer.convertValue(int96);
            assertEquals(epochNanos, decoded);
        }
    }

    /** Build the little-endian 12-byte Impala/Parquet INT96 payload. */
    private static byte[] encodeAsInt96(final long epochNanos) {
        final long adjusted = epochNanos - OFFSET; // remove constant shift
        final long days = Math.floorDiv(adjusted, NANOS_PER_DAY); // whole days since epoch
        final long nanosOfDay = adjusted - days * NANOS_PER_DAY; // remainder nanos
        final int julianDate = (int) (days + JULIAN_BASE); // Impala stores Julian day
        return ByteBuffer.allocate(12)
                .order(ByteOrder.LITTLE_ENDIAN)
                .putLong(nanosOfDay)
                .putInt(julianDate)
                .array();
    }
}
