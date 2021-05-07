package io.deephaven.util.codec;

import junit.framework.TestCase;
import org.junit.Assert;

import java.util.Arrays;

public class ByteArrayCodecTest extends TestCase {

    private void roundTrip(final String args, final byte[] value, final byte[] expected) {
        roundTripWithOffset(args, value, expected, 0);
    }

    private void roundTripWithOffset(final String args, final byte[] value, final byte[] expected, final int offset) {
        final ByteArrayCodec codec = new ByteArrayCodec(args);
        byte[] enc = codec.encode(value);
        // if we expect to be decoding from an offset, construct the input accordingly
        if(offset > 0) {
            final byte[] buffer = new byte[enc.length+offset];
            System.arraycopy(enc, 0, buffer, offset, enc.length);
            enc = buffer;
        }
        // when fixed width we expect every encoded value to be the same size
        if(codec.expectedObjectWidth() > 0) {
            assertEquals(codec.expectedObjectWidth(), enc.length-offset);
        }
        final byte[] v1 = codec.decode(enc, offset, enc.length-offset);
        assert(Arrays.equals(expected, v1));
    }

    public void testRoundTripFixed() {
        final byte[] value = new byte[] {
                1, 2, 3, -1, 34, -128, 10
        };

        roundTrip("7", value, value);
    }

    public void testRoundTripFixedNull() {
        roundTrip("7", null, null);
    }

    public void testRoundTripFixedEmpty() {
        try {
            final ByteArrayCodec codec = new ByteArrayCodec("0");
            Assert.fail("Creating empty column unexpectedly succeeded");
        } catch(IllegalArgumentException ex) {}
    }



    public void testRoundTripFixedNotNull() {
        final byte[] value = new byte[] {
                1, 2, 3, -1, 34, -128, 10
        };

        roundTrip("7,notnull", value, value);
    }

    public void testRoundTripFixedNonNull() {
        try {
            final ByteArrayCodec codec = new ByteArrayCodec("0,notNull");
            codec.encode(null);
            Assert.fail("Encoding null with not-null codec unexpecedly succeeded");
        } catch(IllegalArgumentException ex) {}
    }

    public void testRoundTripFixedEmptyNotNull() {
        try {
            final ByteArrayCodec codec = new ByteArrayCodec("0,notNull");
            Assert.fail("Creating empty column unexpectedly succeeded");
        } catch(IllegalArgumentException ex) {}
    }

    public void testRoundTripVariable() {
        final byte[] value = new byte[] {
                1, 2, 3, -1, 34, -128, 10
        };

        roundTrip("", value, value);
    }

    public void testRoundTripVariableNull() {
        roundTrip("", null, null);
    }

    public void testRoundTripVariableEmpty() {
        final byte[] value = new byte[0];
        try {
            // we expect this to fail
            roundTrip("", value, value);
            Assert.fail("Encoding of empty array unexpectedly succeeded");
        } catch(UnsupportedOperationException ex) {}
    }

    public void testRoundTripFixedWithOffset() {
        final byte[] value = new byte[] {
                1, 2, 3, -1, 34, -128, 10
        };

        roundTripWithOffset("7", value, value, 3);
        roundTripWithOffset("7", null, null, 3);
    }

    public void testRoundTripVariableWithOffset() {
        final byte[] value = new byte[] {
                1, 2, 3, -1, 34, -128, 10
        };

        roundTripWithOffset("", value, value, 3);
        roundTripWithOffset("", null, null, 3);
    }
}
