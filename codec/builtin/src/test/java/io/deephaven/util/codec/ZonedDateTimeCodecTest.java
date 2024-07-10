//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.codec;

import org.junit.Assert;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.junit.Assert.assertEquals;

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

    @Test
    public void testMax() {
        try {
            roundTripWithOffset(ZonedDateTime.ofInstant(
                    Instant.ofEpochSecond(ZonedDateTimeCodec.MAX_CONVERTIBLE_SECONDS + 1),
                    ZoneId.of("America/New_York")), 0);
            Assert.fail();
        } catch (IllegalArgumentException ignored) {
        }

        roundTripWithOffset(ZonedDateTime.ofInstant(
                Instant.ofEpochSecond(ZonedDateTimeCodec.MAX_CONVERTIBLE_SECONDS, 999_999_999L),
                ZoneId.of("America/New_York")), 0);

    }

    @Test
    public void TestNormal() {
        roundTripWithOffset(ZonedDateTime.of(1969, 10, 20, 10, 11, 12, 13, ZoneId.of("America/Chicago")), 0);
        roundTripWithOffset(ZonedDateTime.of(2020, 10, 20, 10, 11, 12, 13, ZoneId.of("America/New_York")), 3);
        roundTripWithOffset(ZonedDateTime.of(2200, 10, 20, 10, 11, 12, 13, ZoneId.of("America/Los_Angeles")), 6);
    }
}
