//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class ZonedDateTimeCodecTest {
    private void roundTripWithOffset(final ZonedDateTime value, final int offset) {
        final ZonedDateTimeCodec codec = new ZonedDateTimeCodec("");
        byte[] enc = codec.encode(value);
        // if we expect to be decoding from an offset, construct the input accordingly
        if (offset > 0) {
            final byte[] buffer = new byte[enc.length + offset];
            System.arraycopy(enc, 0, buffer, offset, enc.length);
            enc = buffer;
        }

        // when fixed width we expect every encoded value to be the same size
        if (codec.expectedObjectWidth() > 0) {
            assertEquals(codec.expectedObjectWidth(), enc.length - offset);
        }
        final ZonedDateTime v1 = codec.decode(enc, offset, enc.length - offset);
        assertEquals(value, v1);
    }

    @Test
    public void testNull() {
        roundTripWithOffset(null, 0);
    }

    private static final ZoneId NY = ZoneId.of("America/New_York");

    /**
     * DH-23557 finding 2. The codec encodes epoch nanos, so its representable range is the whole {@code long} nanos
     * domain -- {@code Instant.ofEpochSecond(9223372036, 854775807)} is exactly {@link Long#MAX_VALUE} nanos, and
     * {@code Instant.ofEpochSecond(-9223372037, 145224192)} exactly {@link Long#MIN_VALUE}. Both ends must round trip,
     * and anything beyond either end must be rejected rather than wrapped.
     *
     * <p>
     * This test previously asserted that {@code epochSecond == 9223372036} <em>throws</em>, which encoded the defect:
     * {@code safeComputeNanos} decided representability from the seconds alone.
     */
    @Test
    public void testMax() {
        roundTripWithOffset(ZonedDateTime.ofInstant(Instant.ofEpochSecond(9223372036L, 854775807L), NY), 0);
        roundTripWithOffset(ZonedDateTime.ofInstant(Instant.ofEpochSecond(9223372036L, 0L), NY), 0);
        roundTripWithOffset(ZonedDateTime.ofInstant(Instant.ofEpochSecond(9223372035L, 999_999_999L), NY), 0);

        assertThrows(IllegalArgumentException.class, () -> roundTripWithOffset(
                ZonedDateTime.ofInstant(Instant.ofEpochSecond(9223372036L, 854775808L), NY), 0));
        assertThrows(IllegalArgumentException.class, () -> roundTripWithOffset(
                ZonedDateTime.ofInstant(Instant.ofEpochSecond(9223372037L, 0L), NY), 0));
        assertThrows(IllegalArgumentException.class, () -> roundTripWithOffset(
                ZonedDateTime.ofInstant(Instant.parse("3000-01-01T00:00:00Z"), NY), 0));
    }

    /**
     * DH-23557 finding 2, the other end. The bottom second of the range used to fail with an undocumented
     * {@link ArithmeticException} from {@code Math.addExact}, and anything well below the range wrapped silently to an
     * unrelated -- often positive -- value.
     */
    @Test
    public void testMin() {
        roundTripWithOffset(ZonedDateTime.ofInstant(Instant.ofEpochSecond(-9223372037L, 145224192L), NY), 0);
        roundTripWithOffset(ZonedDateTime.ofInstant(Instant.ofEpochSecond(-9223372037L, 145224193L), NY), 0);
        roundTripWithOffset(ZonedDateTime.ofInstant(Instant.ofEpochSecond(-9223372036L, 0L), NY), 0);

        assertThrows(IllegalArgumentException.class, () -> roundTripWithOffset(
                ZonedDateTime.ofInstant(Instant.ofEpochSecond(-9223372037L, 145224191L), NY), 0));
        assertThrows(IllegalArgumentException.class, () -> roundTripWithOffset(
                ZonedDateTime.ofInstant(Instant.parse("1000-01-01T00:00:00Z"), NY), 0));
        assertThrows(IllegalArgumentException.class, () -> roundTripWithOffset(
                ZonedDateTime.ofInstant(Instant.parse("0001-01-01T00:00:00Z"), NY), 0));
    }

    @Test
    public void TestNormal() {
        roundTripWithOffset(ZonedDateTime.of(1969, 10, 20, 10, 11, 12, 13, ZoneId.of("America/Chicago")), 0);
        roundTripWithOffset(ZonedDateTime.of(2020, 10, 20, 10, 11, 12, 13, ZoneId.of("America/New_York")), 3);
        roundTripWithOffset(ZonedDateTime.of(2200, 10, 20, 10, 11, 12, 13, ZoneId.of("America/Los_Angeles")), 6);
    }
}
