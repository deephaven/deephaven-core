package io.deephaven.util.codec;

import junit.framework.TestCase;
import org.junit.Assert;

import java.math.BigDecimal;

public class BigDecimalCodecTest extends TestCase {

    private void roundTrip(final String args, final double value) {
        final BigDecimal v = BigDecimal.valueOf(value);
        roundTrip(args, v, v);
    }

    private void roundTripWithOffset(final String args, final double value, final int offset) {
        final BigDecimal v = BigDecimal.valueOf(value);
        roundTripWithOffset(args, v, v, offset);
    }

    private void roundTrip(final String args, final double value, final double expected) {
        final BigDecimal v = BigDecimal.valueOf(value);
        final BigDecimal exp = BigDecimal.valueOf(expected);
        roundTrip(args, v, exp);
    }

    private void roundTrip(final String args, final BigDecimal value) {
        roundTripWithOffset(args, value, value, 0);
    }

    // test a simple encode->decode round trip through the codec
    private void roundTripWithOffset(final String args, final BigDecimal value, final int offset) {
        roundTripWithOffset(args, value, value, offset);
    }

    private void roundTrip(final String args, final BigDecimal value, final BigDecimal expected) {
        roundTripWithOffset(args, value, expected, 0);
    }

    private void roundTripWithOffset(final String args, final BigDecimal value,
        final BigDecimal expected, final int offset) {
        final BigDecimalCodec codec = new BigDecimalCodec(args);
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
        final BigDecimal v1 = codec.decode(enc, offset, enc.length - offset);
        assertEquals(expected.stripTrailingZeros(), v1);
    }

    private void expectIllegalArgumentException(final String args, final double value) {
        try {
            final BigDecimalCodec codec = new BigDecimalCodec(args);
            codec.encode(new BigDecimal(value));
            Assert.fail("Encoding of " + value + " unexpectedly succeeded");
        } catch (IllegalArgumentException ex) {
        }
    }

    private void expectNull(final String args) {
        expectNullWithOffset(args, 0);
    }

    private void expectNullWithOffset(final String args, final int offset) {
        final BigDecimalCodec codec = new BigDecimalCodec(args);
        byte[] enc = codec.encode(null);
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
        final BigDecimal v1 = codec.decode(enc, offset, enc.length - offset);
        assertEquals(v1, null);
    }

    public void testVariableEncoding() {
        roundTrip("", 9.99999);
        roundTrip("", 1.1);
        roundTrip("", 0.00001);
        roundTrip("", 10000.00001);

        roundTrip("", 9);
        roundTrip("", 1);
        roundTrip("", 0);
        roundTrip("", 10000);
        roundTrip("", 1000000000);

        roundTrip("", 9.9);
        roundTrip("", 1.1);
        roundTrip("", 0.1);
        roundTrip("", 1.0);
        roundTrip("", 0.0);
    }

    public void testVariableEncodingNeg() {
        roundTrip("", -9.99999);
        roundTrip("", -1.1);
        roundTrip("", -0.00001);
        roundTrip("", -10000.00001);

        roundTrip("", -9);
        roundTrip("", -1);
        roundTrip("", -10000);
        roundTrip("", -1000000000);

        roundTrip("", -9.9);
        roundTrip("", -1.1);
        roundTrip("", -0.1);
        roundTrip("", -1.0);
        roundTrip("", -0.0);
    }

    public void testVariableNull() {
        expectNull("");
    }

    public void testFixedEncodeExact() {
        roundTrip("10,3", 9999999.999);

        roundTrip("10,5", 0.00001);
        roundTrip("10,5", 1.1);
        roundTrip("10,5", 9.99999);
        roundTrip("10,5", 10000.00001);
        roundTrip("10,5", 99999.99999);

        roundTrip("10,0", 0);
        roundTrip("10,0", 1);
        roundTrip("10,0", 9);
        roundTrip("10,0", 10000.0);
        roundTrip("10,0", 1000000000.0);
        roundTrip("10,0", 9999999999.0);

        roundTrip("2,1", 0.0);
        roundTrip("2,1", 0.1);
        roundTrip("2,1", 1.0);
        roundTrip("2,1", 1.1);
        roundTrip("2,1", 9.9);

        // for fun, some base 2 prices...
        roundTrip("15,8", 1.0 / 256.0);
        roundTrip("15,8", 2.0 / 256.0);
        roundTrip("15,8", 3.0 / 256.0);
        roundTrip("15,8", 4.0 / 256.0);
        roundTrip("15,8", 5.0 / 256.0);
    }

    public void testFixedEncodeWhitspaceArg() {
        roundTrip(" 10 , 5 ", 0.00001);
    }

    public void testFixedEncodeExactNeg() {
        roundTrip("10,5", -0.00001);
        roundTrip("10,5", -1.1);
        roundTrip("10,5", -9.99999);
        roundTrip("10,5", -10000.00001);
        roundTrip("10,5", -99999.99999);

        roundTrip("10,0", -1);
        roundTrip("10,0", -9);
        roundTrip("10,0", -10000.0);
        roundTrip("10,0", -1000000000.0);
        roundTrip("10,0", -9999999999.0);

        roundTrip("2,1", -0.1);
        roundTrip("2,1", -1.0);
        roundTrip("2,1", -1.1);
        roundTrip("2,1", -9.9);

        // for fun, some base 2 prices...
        roundTrip("15,8", -1.0 / 256.0);
        roundTrip("15,8", -2.0 / 256.0);
        roundTrip("15,8", -3.0 / 256.0);
        roundTrip("15,8", -4.0 / 256.0);
        roundTrip("15,8", -5.0 / 256.0);
    }


    public void testFixedNull() {
        expectNull("10,5");
        expectNull("10,0");
        expectNull("5,5");
        expectNull("1,0");
    }

    public void testFixedEncodeRounding() {
        roundTrip("10,5,allowRounding", 1.111111, 1.11111);
        roundTrip("10,5,allowRounding", 9.999999, 10);
        roundTrip("10,5,allowRounding", 0.000001, 0);
        roundTrip("10,5,allowRounding", 1.000001, 1);
    }

    public void testFixedEncodeNoRounding() {
        roundTrip("10,5,noRounding", 1.11111);
        roundTrip("10,5,noRounding", 9.99999);
        roundTrip("10,5,noRounding", 0.00001);
        roundTrip("10,5,noRounding", 1.00001);
    }

    public void testFixedOverflow() {
        // we should get overflow exceptions if the value is too large, regardless if we allow
        // rounding
        expectIllegalArgumentException("5,5,allowRounding", 1111111);
        expectIllegalArgumentException("5,5,noRounding", 1111111);
        expectIllegalArgumentException("10,3,noRounding", 9999999999L); // just one over
    }

    public void testIllegalRoundingMode() {
        expectIllegalArgumentException("5,5,joIsCrazy", 1);
    }

    public void testFixedPrecisionLimit() {

        final int maxPrec = BigDecimalCodec.MAX_FIXED_PRECISION;
        final BigDecimal hugeInt =
            BigDecimal.valueOf(10).pow(BigDecimalCodec.MAX_FIXED_PRECISION - 1);
        final BigDecimal hugeFrac = BigDecimal.valueOf(10)
            .pow(BigDecimalCodec.MAX_FIXED_PRECISION - 4).add(BigDecimal.valueOf(0.111));
        final BigDecimal teenyNumber =
            BigDecimal.valueOf(0.1).pow(BigDecimalCodec.MAX_FIXED_PRECISION - 1);

        // 614 should be ok, that's how many decimal digits we can store in 255 bytes
        roundTrip(maxPrec + ",5", 12345);
        // but not 615
        expectIllegalArgumentException((maxPrec + 1) + ",5", 1111111);

        // prove that we can encode and decode a huge integer
        roundTrip(maxPrec + ",0", hugeInt);
        roundTrip(maxPrec + ",0", hugeInt.negate());

        // prove that we can encode and decode a huge fraction
        roundTrip(maxPrec + ",3", hugeFrac);
        roundTrip(maxPrec + ",3", hugeFrac.negate());

        // prove that we can encode and decode a teeny tiny number
        roundTrip(maxPrec + "," + maxPrec, teenyNumber);
        roundTrip(maxPrec + "," + maxPrec, teenyNumber.negate());
    }

    public void testFixedStrict() {
        expectIllegalArgumentException("5,5,noRounding", 9.999999);
        expectIllegalArgumentException("5,5,noRounding", 0.000001);
        expectIllegalArgumentException("5,5,noRounding", 1.000001);

        // test that we get the same w/o the arg (default should be true)
        expectIllegalArgumentException("5,5", 9.999999);
        expectIllegalArgumentException("5,5", 0.000001);
        expectIllegalArgumentException("5,5", 1.000001);
    }

    public void testVariableEncodingWithOffset() {
        expectNullWithOffset("", 3);

        roundTripWithOffset("", 9.99999, 3);
        roundTripWithOffset("", 1.1, 3);
        roundTripWithOffset("", 0.00001, 3);
        roundTripWithOffset("", 10000.00001, 3);

        roundTripWithOffset("", 9, 3);
        roundTripWithOffset("", 1, 3);
        roundTripWithOffset("", 0, 3);
        roundTripWithOffset("", 10000, 3);
        roundTripWithOffset("", 1000000000, 3);

        roundTripWithOffset("", 9.9, 3);
        roundTripWithOffset("", 1.1, 3);
        roundTripWithOffset("", 0.1, 3);
        roundTripWithOffset("", 1.0, 3);
        roundTripWithOffset("", 0.0, 3);
    }

    public void testFixedEncodingWithOffset() {

        expectNullWithOffset("10,3", 3);

        roundTripWithOffset("10,3", 9999999.999, 3);
        roundTripWithOffset("10,3", 9999999.999, 3);

        roundTripWithOffset("10,5", 0.00001, 3);
        roundTripWithOffset("10,5", 1.1, 3);
        roundTripWithOffset("10,5", 9.99999, 3);
        roundTripWithOffset("10,5", 10000.00001, 3);
        roundTripWithOffset("10,5", 99999.99999, 3);

        roundTripWithOffset("10,0", 0, 3);
        roundTripWithOffset("10,0", 1, 3);
        roundTripWithOffset("10,0", 9, 3);
        roundTripWithOffset("10,0", 10000.0, 3);
        roundTripWithOffset("10,0", 1000000000.0, 3);
        roundTripWithOffset("10,0", 9999999999.0, 3);

        roundTripWithOffset("2,1", 0.0, 3);
        roundTripWithOffset("2,1", 0.1, 3);
        roundTripWithOffset("2,1", 1.0, 3);
        roundTripWithOffset("2,1", 1.1, 3);
        roundTripWithOffset("2,1", 9.9, 3);

        // for fun, some base 2 prices...
        roundTripWithOffset("15,8", 1.0 / 256.0, 3);
        roundTripWithOffset("15,8", 2.0 / 256.0, 3);
        roundTripWithOffset("15,8", 3.0 / 256.0, 3);
        roundTripWithOffset("15,8", 4.0 / 256.0, 3);
        roundTripWithOffset("15,8", 5.0 / 256.0, 3);
    }

    // these commented out tests are fun to run manually

    /*
     * public void testSpeed() { final Random random = new Random(); final long start =
     * System.currentTimeMillis(); final int n = 10_000_000;
     * 
     * for(int i = 0; i < n; i++) { BigDecimal bd =
     * BigDecimal.valueOf(Math.round(random.nextDouble() * 10_000_000)); bd =
     * bd.setScale(9).round(new MathContext(20)); if(random.nextDouble() >= 0.5) { bd = bd.negate();
     * } roundTrip("20,9,noRounding", bd); } final long end = System.currentTimeMillis();
     * System.out.println("Encoded & decoded " + n + " in " + (end-start) + "ms"); }
     * 
     * private void printEncoded(byte[] enc) { for(int i = 0; i < enc.length; i++) {
     * System.out.print(" "); int d = enc[i]; System.out.print(String.format("0x%02X",(int)(d &
     * 0xff))); } System.out.println(); }
     * 
     * public void testOrdering() { BigDecimalCodec codec = new BigDecimalCodec(10, 3, true);
     * printEncoded(codec.encode(BigDecimal.valueOf(-9999999.999)));
     * printEncoded(codec.encode(BigDecimal.valueOf(-10000.200)));
     * printEncoded(codec.encode(BigDecimal.valueOf(-0.1)));
     * printEncoded(codec.encode(BigDecimal.valueOf(-0.01)));
     * printEncoded(codec.encode(BigDecimal.valueOf(-0.001)));
     * printEncoded(codec.encode(BigDecimal.valueOf(0)));
     * printEncoded(codec.encode(BigDecimal.valueOf(0.001)));
     * printEncoded(codec.encode(BigDecimal.valueOf(0.01)));
     * printEncoded(codec.encode(BigDecimal.valueOf(0.1)));
     * printEncoded(codec.encode(BigDecimal.valueOf(100))); }
     */
}

