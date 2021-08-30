package io.deephaven.util.codec;

import junit.framework.TestCase;
import org.junit.Assert;

import java.math.BigInteger;

public class BigIntegerCodecTest extends TestCase {

    private void roundTrip(final String args, final long value) {
        final BigInteger v = BigInteger.valueOf(value);
        roundTripWithOffset(args, v, v, 0);
    }

    private void roundTripWithOffset(final String args, final long value, final int offset) {
        final BigInteger v = BigInteger.valueOf(value);
        roundTripWithOffset(args, v, v, offset);
    }

    private void roundTrip(final String args, final long value, final long expected) {
        final BigInteger v = BigInteger.valueOf(value);
        final BigInteger exp = BigInteger.valueOf(expected);
        roundTripWithOffset(args, v, exp, 0);
    }

    // test a simple encode->decode round trip through the codec
    private void roundTrip(final String args, final BigInteger value) {
        roundTripWithOffset(args, value, value, 0);
    }

    private void roundTripWithOffset(final String args, final BigInteger value,
        final BigInteger expected, final int offset) {
        final BigIntegerCodec codec = new BigIntegerCodec(args);
        byte[] enc = codec.encode(value);
        // if we expect to be decoding from an offset, construct the input accordingly
        if (offset > 0) {
            final byte[] buffer = new byte[enc.length + offset];
            System.arraycopy(enc, 0, buffer, offset, enc.length);
            enc = buffer;
        }
        // when fixed width we expect every encoded value to be the same size
        if (args.contains("precision=")) {
            assertEquals(codec.expectedObjectWidth(), enc.length - offset);
        }
        final BigInteger v1 = codec.decode(enc, offset, enc.length - offset);
        assertEquals(expected, v1);
    }

    private void expectIllegalArgumentException(final String args, final long value) {
        try {
            final BigIntegerCodec codec = new BigIntegerCodec(args);
            codec.encode(BigInteger.valueOf(value));
            Assert.fail("Encoding of " + value + " unexpectedly succeeded");
        } catch (IllegalArgumentException ex) {
        }
    }

    private void expectNull(final String args) {
        expectNullWithOffset(args, 0);
    }

    private void expectNullWithOffset(final String args, final int offset) {
        final BigIntegerCodec codec = new BigIntegerCodec(args);
        byte[] enc = codec.encode(null);
        // if we expect to be decoding from an offset, construct the input accordingly
        if (offset > 0) {
            final byte[] buffer = new byte[enc.length + offset];
            System.arraycopy(enc, 0, buffer, offset, enc.length);
            enc = buffer;
        }
        // when fixed width we expect every encoded value to be the same size
        if (args.contains("precision=")) {
            assertEquals(codec.expectedObjectWidth(), enc.length - offset);
        }
        final BigInteger v1 = codec.decode(enc, offset, enc.length - offset);
        assertEquals(v1, null);
    }

    public void testVariableEncoding() {
        roundTrip("", 9);
        roundTrip("", 1);
        roundTrip("", 0);
        roundTrip("", 10000);
        roundTrip("", 1000000000);
    }

    public void testVariableEncodingNeg() {
        roundTrip("", -9);
        roundTrip("", -1);
        roundTrip("", -10000);
        roundTrip("", -1000000000);
    }

    public void testVariableNull() {
        expectNull("");
    }

    public void testFixedEncodeExact() {
        roundTrip("10", 0);
        roundTrip("10", 1);
        roundTrip("10", 9);
        roundTrip("10", 10000);
        roundTrip("10", 1000000000);
        roundTrip("10", 9999999999L);
    }

    public void testFixedEncodeWhiteSpaceArg() {
        roundTrip(" 10 ", 0);
    }

    public void testFixedEncodeExactNeg() {
        roundTrip("10", -1);
        roundTrip("10", -9);
        roundTrip("10", -10000);
        roundTrip("10", -1000000000);
        roundTrip("10", -9999999999L);
    }


    public void testFixedNull() {
        expectNull("10");
        expectNull("1");
    }

    public void testFixedOverflow() {
        // we should get overflow exceptions if the value is too large
        expectIllegalArgumentException("5", 1111111);
    }

    public void testBadArg() {
        // scale not permissable for BigInteger codec
        expectIllegalArgumentException("5,5", 1);
    }

    public void testPrecisionLimits() {
        final int maxPrec = BigIntegerCodec.MAX_FIXED_PRECISION;

        // should be ok, that's how many decimal digits we can store in 255 bytes
        roundTrip(Integer.toString(maxPrec), 12345);

        // make sure we can't create with with one too many
        expectIllegalArgumentException(Integer.toString(maxPrec + 1), 1111111);

        // make sure we can't create with negative or zero
        expectIllegalArgumentException("0", 1111111);
        expectIllegalArgumentException("-1", 1111111);
    }

    public void testLargeValues() {

        final int maxPrec = BigIntegerCodec.MAX_FIXED_PRECISION;
        final BigInteger hugeInt =
            BigInteger.valueOf(10).pow(BigIntegerCodec.MAX_FIXED_PRECISION - 1);
        final BigInteger hugeNegativeInt =
            BigInteger.valueOf(10).pow(BigIntegerCodec.MAX_FIXED_PRECISION - 1).negate();

        // prove that we can encode and decode a huge integer
        roundTrip(Integer.toString(maxPrec), hugeInt);
        roundTrip(Integer.toString(maxPrec), hugeInt.negate());

        roundTrip(Integer.toString(maxPrec), hugeNegativeInt);
        roundTrip(Integer.toString(maxPrec), hugeNegativeInt.negate());
    }

    public void testVariableEncodingWithOffset() {
        expectNullWithOffset("", 3);
        roundTripWithOffset("", 9, 3);
        roundTripWithOffset("", 1, 3);
        roundTripWithOffset("", 0, 3);
        roundTripWithOffset("", 10000, 3);
        roundTripWithOffset("", 1000000000, 3);
    }

    public void testFixedEncodeWithOffset() {
        expectNullWithOffset("10", 3);
        roundTripWithOffset("10", 0, 3);
        roundTripWithOffset("10", 1, 3);
        roundTripWithOffset("10", 9, 3);
        roundTripWithOffset("10", 10000, 3);
        roundTripWithOffset("10", 1000000000, 3);
        roundTripWithOffset("10", 9999999999L, 3);
    }
}
