package io.deephaven.util.codec;

import junit.framework.TestCase;
import org.junit.Assert;

import java.time.LocalTime;

public class LocalTimeCodecTest extends TestCase {
    private void roundTrip(final String args, final String value) {
        final LocalTime v = LocalTime.parse(value);
        roundTripWithOffset(args, v, v, 0);
    }

    private void roundTripWithOffset(final String args, final String value, final int offset) {
        final LocalTime v = LocalTime.parse(value);
        roundTripWithOffset(args, v, v, offset);
    }

    private void roundTrip(final String args, final String value, final String expected) {
        final LocalTime v = LocalTime.parse(value);
        final LocalTime exp = LocalTime.parse(expected);
        roundTripWithOffset(args, v, exp, 0);
    }

    // test a simple encode->decode round trip through the codec
    private void roundTrip(final String args, final LocalTime value) {
        roundTripWithOffset(args, value, value, 0);
    }

    private void roundTripWithOffset(final String args, final LocalTime value, final int offset) {
        roundTripWithOffset(args, value, value, offset);
    }

    private void roundTripWithOffset(final String args, final LocalTime value,
        final LocalTime expected, final int offset) {
        final LocalTimeCodec codec = new LocalTimeCodec(args);
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
        final LocalTime v1 = codec.decode(enc, offset, enc.length - offset);
        assertEquals(expected, v1);
    }

    private void expectIllegalArgumentException(final String args, final LocalTime value) {
        try {
            final LocalTimeCodec codec = new LocalTimeCodec(args);
            codec.encode(value);
            Assert.fail("Encoding of " + value + " unexpectedly succeeded");
        } catch (IllegalArgumentException ex) {
        }
    }

    private void expectNull(final String args) {
        final LocalTimeCodec codec = new LocalTimeCodec(args);
        final byte[] enc = codec.encode(null);
        // when fixed width we expect every encoded value to be the same size
        if (codec.expectedObjectWidth() > 0) {
            assertEquals(codec.expectedObjectWidth(), enc.length);
        }
        final LocalTime v1 = codec.decode(enc, 0, enc.length);
        assertEquals(v1, null);
    }

    public void testNanosEncoding() {
        roundTrip("", LocalTime.of(0, 0, 0, 0));
        roundTrip("", LocalTime.of(23, 0, 0, 0));
        roundTrip("", LocalTime.of(23, 59, 59, 999_999_999));

        roundTrip("", "18:36:29.123456789");
        roundTrip("", "03:00:00");
        roundTrip("", (LocalTime) null);
    }

    // test encoding with 7 fractional digits (nteresting because this matches SQL Server TIME
    // precision)
    public void test100NanosEncoding() {
        roundTrip("7", LocalTime.of(0, 0, 0, 0));
        roundTrip("7", LocalTime.of(23, 0, 0, 0));
        roundTrip("7", LocalTime.of(23, 59, 59, 999_999_900));

        roundTrip("7", "18:36:29.1234567");
        roundTrip("7", "03:00:00");
        roundTrip("7", (LocalTime) null);
    }

    public void testMillisEncoding() {
        roundTrip("3", LocalTime.of(0, 0, 0, 0));
        roundTrip("3", LocalTime.of(23, 59, 59, 999_000_000)); // 999 millis is max fractional value
                                                               // with milli precision

        roundTrip("3", "18:36:29.123");
        roundTrip("3", "03:00:00");
        roundTrip("3", (LocalTime) null);
    }

    public void testMicrosEncoding() {
        roundTrip("6", LocalTime.of(0, 0, 0, 0));
        roundTrip("6", LocalTime.of(23, 59, 59, 999_999_000)); // 999 millis is max fractional value
                                                               // with milli precision

        roundTrip("6", "18:36:29.123456");
        roundTrip("6", "03:00:00");
        roundTrip("6", (LocalTime) null);
    }

    public void testSecondEncoding() {
        roundTrip("0", LocalTime.of(0, 0, 0, 0));
        roundTrip("0", LocalTime.of(23, 59, 59, 0)); // 999 millis is max fractional value with
                                                     // milli precision

        roundTrip("0", "18:36:29");
        roundTrip("0", "03:00:00");
        roundTrip("0", (LocalTime) null);
    }

    public void testNanosNotNullEncoding() {
        roundTrip("9,notnull", LocalTime.of(0, 0, 0, 0));
        roundTrip("9,notnull", LocalTime.of(23, 0, 0, 0));
        roundTrip("9,notnull", LocalTime.of(23, 59, 59, 999_999_999));

        roundTrip("9,notnull", "18:36:29.123456789");
        roundTrip("9,notnull", "03:00:00");
    }

    // test encoding with 7 fractional digits (nteresting because this matches SQL Server TIME
    // precision)
    public void test100NanosNotNullEncoding() {
        roundTrip("7,notnull", LocalTime.of(0, 0, 0, 0));
        roundTrip("7,notnull", LocalTime.of(23, 0, 0, 0));
        roundTrip("7,notnull", LocalTime.of(23, 59, 59, 999_999_900));

        roundTrip("7,notnull", "18:36:29.1234567");
        roundTrip("7,notnull", "03:00:00");
    }

    public void testMillisNotNullEncoding() {
        roundTrip("3,notnull", LocalTime.of(0, 0, 0, 0));
        roundTrip("3,notnull", LocalTime.of(23, 59, 59, 999_000_000)); // 999 millis is max
                                                                       // fractional value with
                                                                       // milli precision

        roundTrip("3,notnull", "18:36:29.123");
        roundTrip("3,notnull", "03:00:00");
    }

    public void testMicrosNotNullEncoding() {
        roundTrip("6,notnull", LocalTime.of(0, 0, 0, 0));
        roundTrip("6,notnull", LocalTime.of(23, 59, 59, 999_999_000)); // 999 millis is max
                                                                       // fractional value with
                                                                       // milli precision

        roundTrip("6,notnull", "18:36:29.123456");
        roundTrip("6,notnull", "03:00:00");
    }

    public void testSecondNotNullEncoding() {
        roundTrip("0,notnull", LocalTime.of(0, 0, 0, 0));
        roundTrip("0,notnull", LocalTime.of(23, 59, 59, 0)); // 999 millis is max fractional value
                                                             // with milli precision

        roundTrip("0,notnull", "18:36:29");
        roundTrip("0,notnull", "03:00:00");
    }

    public void testNanosNullableEncoding() {
        roundTrip("9,nullable", LocalTime.of(0, 0, 0, 0));
        roundTrip("9,nullable", LocalTime.of(23, 0, 0, 0));
        roundTrip("9,nullable", LocalTime.of(23, 59, 59, 999_999_999));

        roundTrip("9,nullable", "18:36:29.123456789");
        roundTrip("9,nullable", "03:00:00");
        roundTrip("9,nullable", (LocalTime) null);
    }

    public void testVariableNull() {
        expectNull("");
    }

    public void testFixedEncodeWhitspaceArg() {
        roundTrip(" 3 ", "18:36:29.123");
    }

    public void testIllegalEncodingType() {
        expectIllegalArgumentException("blah", LocalTime.of(0, 0, 0, 0));
    }

    public void testNanosEncodingWithOffset() {
        roundTripWithOffset("", LocalTime.of(0, 0, 0, 0), 3);
        roundTripWithOffset("", LocalTime.of(23, 0, 0, 0), 3);
        roundTripWithOffset("", LocalTime.of(23, 59, 59, 999_999_999), 3);

        roundTripWithOffset("", "18:36:29.123456789", 3);
        roundTripWithOffset("", "03:00:00", 3);
        roundTripWithOffset("", (LocalTime) null, 3);
    }

    public void testNanosNotNullEncodingWithOffset() {
        roundTripWithOffset("9,notnull", LocalTime.of(0, 0, 0, 0), 3);
        roundTripWithOffset("9,notnull", LocalTime.of(23, 0, 0, 0), 3);
        roundTripWithOffset("9,notnull", LocalTime.of(23, 59, 59, 999_999_999), 3);

        roundTripWithOffset("9,notnull", "18:36:29.123456789", 3);
        roundTripWithOffset("9,notnull", "03:00:00", 3);
    }

    public void testMillisEncodingWithOffset() {
        roundTripWithOffset("3", LocalTime.of(0, 0, 0, 0), 3);
        roundTripWithOffset("3", LocalTime.of(23, 59, 59, 999_000_000), 3); // 999 millis is max
                                                                            // fractional value with
                                                                            // milli precision

        roundTripWithOffset("3", "18:36:29.123", 3);
        roundTripWithOffset("3", "03:00:00", 3);
        roundTripWithOffset("3", (LocalTime) null, 3);
    }

    public void testMillisNotNullEncodingWithOffset() {
        roundTripWithOffset("3,notnull", LocalTime.of(0, 0, 0, 0), 3);
        roundTripWithOffset("3,notnull", LocalTime.of(23, 59, 59, 999_000_000), 3); // 999 millis is
                                                                                    // max
                                                                                    // fractional
                                                                                    // value with
                                                                                    // milli
                                                                                    // precision

        roundTripWithOffset("3,notnull", "18:36:29.123", 3);
        roundTripWithOffset("3,notnull", "03:00:00", 3);
    }
}
