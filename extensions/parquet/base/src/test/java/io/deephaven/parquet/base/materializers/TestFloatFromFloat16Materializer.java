//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestFloatFromFloat16Materializer {

    /**
     * The value of {@code halfBits} computed arithmetically, independent of the bit manipulation under test.
     */
    private static float reference(final int halfBits) {
        final int exponent = (halfBits >>> 10) & 0x1f;
        final int significand = halfBits & 0x3ff;
        final float magnitude;
        if (exponent == 0x1f) {
            magnitude = significand == 0 ? Float.POSITIVE_INFINITY : Float.NaN;
        } else if (exponent == 0) {
            magnitude = Math.scalb((float) significand, -24);
        } else {
            magnitude = Math.scalb((float) (significand | 0x400), exponent - 25);
        }
        return (halfBits & 0x8000) == 0 ? magnitude : -magnitude;
    }

    @Test
    void everyBitPattern() {
        for (int halfBits = 0; halfBits <= 0xffff; ++halfBits) {
            final float expected = reference(halfBits);
            final float actual = FloatFromFloat16Materializer.float16ToFloat((short) halfBits);
            final int actualBits = Float.floatToRawIntBits(actual);
            if (Float.isNaN(expected)) {
                assertThat(actual).as("0x%04x", halfBits).isNaN();
                // Sign and payload survive.
                assertThat(actualBits >>> 31).as("0x%04x sign", halfBits).isEqualTo(halfBits >>> 15);
                assertThat((actualBits >>> 13) & 0x3ff).as("0x%04x payload", halfBits).isEqualTo(halfBits & 0x3ff);
            } else {
                // Bitwise, so -0.0 is distinguished from 0.0.
                assertThat(actualBits).as("0x%04x", halfBits).isEqualTo(Float.floatToRawIntBits(expected));
            }
        }
    }

    @Test
    void littleEndianBinary() {
        // 0x3c00 is 1.0; 0xc000 is -2.0.
        assertThat(FloatFromFloat16Materializer.convertValue(Binary.fromConstantByteArray(new byte[] {0x00, 0x3c})))
                .isEqualTo(1.0f);
        assertThat(FloatFromFloat16Materializer.convertValue(
                Binary.fromConstantByteArray(new byte[] {0x7f, 0x00, (byte) 0xc0, 0x7f}, 1, 2)))
                .isEqualTo(-2.0f);
    }
}
