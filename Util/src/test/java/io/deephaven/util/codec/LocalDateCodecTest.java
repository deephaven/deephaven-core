package io.deephaven.util.codec;

import junit.framework.TestCase;
import org.junit.Assert;

import java.time.LocalDate;
import java.time.Year;

public class LocalDateCodecTest extends TestCase {
    private void roundTrip(final String args, final String value) {
        final LocalDate v = LocalDate.parse(value);
        roundTripWithOffset(args, v, v, 0);
    }

    private void roundTripWithOffset(final String args, final String value, final int offset) {
        final LocalDate v = LocalDate.parse(value);
        roundTripWithOffset(args, v, v, offset);
    }

    private void roundTrip(final String args, final String value, final String expected) {
        final LocalDate v = LocalDate.parse(value);
        final LocalDate exp = LocalDate.parse(expected);
        roundTripWithOffset(args, v, exp, 0);
    }

    // test a simple encode->decode round trip through the codec
    private void roundTrip(final String args, final LocalDate value) {
        roundTripWithOffset(args, value, value, 0);
    }

    private void roundTripWithOffset(final String args, final LocalDate value, final int offset) {
        roundTripWithOffset(args, value, value, offset);
    }

    private void roundTripWithOffset(final String args, final LocalDate value, final LocalDate expected,
            final int offset) {
        final LocalDateCodec codec = new LocalDateCodec(args);
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
        final LocalDate v1 = codec.decode(enc, offset, enc.length - offset);
        assertEquals(expected, v1);
    }

    private void expectIllegalArgumentException(final String args, final LocalDate value) {
        try {
            final LocalDateCodec codec = new LocalDateCodec(args);
            codec.encode(value);
            Assert.fail("Encoding of " + value + " unexpectedly succeeded");
        } catch (IllegalArgumentException ex) {
        }
    }

    private void expectNull(final String args) {
        final LocalDateCodec codec = new LocalDateCodec(args);
        final byte[] enc = codec.encode(null);
        // when fixed width we expect every encoded value to be the same size
        if (codec.expectedObjectWidth() > 0) {
            assertEquals(codec.expectedObjectWidth(), enc.length);
        }
        final LocalDate v1 = codec.decode(enc, 0, enc.length);
        assertEquals(v1, null);
    }

    public void testFullEncoding() {
        roundTrip("", LocalDate.of(Year.MIN_VALUE, 1, 1));
        roundTrip("", LocalDate.of(Year.MAX_VALUE, 12, 31));

        roundTrip("", LocalDate.of(-9999, 1, 1));
        roundTrip("", LocalDate.of(9999, 12, 31));

        roundTrip("", "2018-01-01");
        roundTrip("", "0001-01-01");

        roundTrip("", (LocalDate) null);
    }

    public void testFullYearRange() {
        for (int year = Year.MIN_VALUE; year <= Year.MAX_VALUE; year += 1_000_000) {
            roundTrip("", LocalDate.of(year, 1, 1));
            roundTrip("", LocalDate.of(year, 12, 31));
        }
    }

    public void testCompactYearRange() {
        for (int year = -9999; year <= 9999; year++) {
            roundTrip("", LocalDate.of(year, 1, 1));
            roundTrip("", LocalDate.of(year, 12, 31));
        }
    }

    public void testCompactEncoding() {
        roundTrip(LocalDateCodec.Domain.Compact.name(), "0000-01-01"); // minimum compact value
        roundTrip(LocalDateCodec.Domain.Compact.name(), "9999-12-31");

        roundTrip(LocalDateCodec.Domain.Compact.name(), "2018-01-01");
        roundTrip(LocalDateCodec.Domain.Compact.name(), "0001-01-01");

        roundTrip("Compact", (LocalDate) null);
    }

    public void testFullNotNullEncoding() {
        roundTrip("Full,notnull", LocalDate.of(-999_999_999, 1, 1));
        roundTrip("Full,notnull", LocalDate.of(999_999_999, 12, 31));

        roundTrip("Full,notnull", LocalDate.of(-9999, 1, 1));
        roundTrip("Full,notnull", LocalDate.of(9999, 12, 31));

        roundTrip("Full,notnull", "2018-01-01");
        roundTrip("Full,notnull", "0001-01-01");

        try {
            roundTrip("Full,notnull", (LocalDate) null);
            Assert.fail("Codec should not accept null!");
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals("Codec cannot encode null LocalDate values", ex.getMessage());
        }

    }

    public void testCompactNotNullEncoding() {
        roundTrip("Compact,notnull", "0000-01-01"); // minimum compact value
        roundTrip("Compact,notnull", "9999-12-31");

        roundTrip("Compact,notnull", "2018-01-01");
        roundTrip("Compact,notnull", "0001-01-01");

        try {
            roundTrip("Compact,notnull", (LocalDate) null);
            Assert.fail("Codec should not accept null!");
        } catch (IllegalArgumentException ex) {
            Assert.assertEquals("Codec cannot encode null LocalDate values", ex.getMessage());
        }
    }

    public void testVariableNull() {
        expectNull("");
    }

    public void testFixedEncodeWhitspaceArg() {
        roundTrip(" " + LocalDateCodec.Domain.Compact.name() + " ", "2018-01-01");
    }

    public void testIllegalEncodingType() {
        expectIllegalArgumentException("blah", LocalDate.of(2018, 1, 1));
    }

    public void testOutOfRangeCompactDate() {
        expectIllegalArgumentException("compact", LocalDate.of(-1, 1, 1));
        expectIllegalArgumentException("compact", LocalDate.of(10000, 1, 1));
    }

    public void testCompactEncodingWithOffset() {
        roundTripWithOffset(LocalDateCodec.Domain.Compact.name(), "0000-01-01", 3); // minimum compact value
        roundTripWithOffset(LocalDateCodec.Domain.Compact.name(), "9999-12-31", 3);

        roundTripWithOffset(LocalDateCodec.Domain.Compact.name(), "2018-01-01", 3);
        roundTripWithOffset(LocalDateCodec.Domain.Compact.name(), "0001-01-01", 3);

        roundTripWithOffset("Compact", (LocalDate) null, 0);
    }

    public void testFullEncodingWithOffset() {
        roundTripWithOffset("", LocalDate.of(Year.MIN_VALUE, 1, 1), 3);
        roundTripWithOffset("", LocalDate.of(Year.MAX_VALUE, 12, 31), 3);

        roundTripWithOffset("", LocalDate.of(-9999, 1, 1), 3);
        roundTripWithOffset("", LocalDate.of(9999, 12, 31), 3);

        roundTripWithOffset("", "2018-01-01", 3);
        roundTripWithOffset("", "0001-01-01", 3);

        roundTripWithOffset("", (LocalDate) null, 3);
    }
}
