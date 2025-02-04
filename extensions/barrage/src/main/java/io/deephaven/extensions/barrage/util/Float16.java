//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

/**
 * Lifted from Apache Arrow project:
 * https://github.com/apache/arrow/blob/ee62d970338f173fff4c0d11b975fe30b5fda70b/java/memory/memory-core/src/main/java/org/apache/arrow/memory/util/Float16.java
 *
 * <ul>
 * Changes made:
 * <li>Keep Only the Method Used</li>
 * <li>Use floatToIntBits over floatToIntRawBits for GWT compilation</li>
 * </ul>
 *
 * The class is a utility class to manipulate half-precision 16-bit
 * <a href="https://en.wikipedia.org/wiki/Half-precision_floating-point_format">IEEE 754</a> floating point data types
 * (also called fp16 or binary16). A half-precision float can be created from or converted to single-precision floats,
 * and is stored in a short data type. The IEEE 754 standard specifies an float16 as having the following format:
 *
 * <ul>
 * <li>Sign bit: 1 bit
 * <li>Exponent width: 5 bits
 * <li>Significand: 10 bits
 * </ul>
 *
 * <p>
 * The format is laid out as follows:
 *
 * <pre>
 * 1   11111   1111111111
 * ^   --^--   -----^----
 * sign  |          |_______ significand
 *       |
 *      -- exponent
 * </pre>
 *
 * Half-precision floating points can be useful to save memory and/or bandwidth at the expense of range and precision
 * when compared to single-precision floating points (float32). Ref:
 * https://android.googlesource.com/platform/libcore/+/master/luni/src/main/java/libcore/util/FP16.java
 */
public class Float16 {
    // The bitmask to and a number with to obtain the sign bit.
    private static final int SIGN_MASK = 0x8000;
    // The offset to shift by to obtain the exponent bits.
    private static final int EXPONENT_SHIFT = 10;
    // The bitmask to and a number shifted by EXPONENT_SHIFT right, to obtain exponent bits.
    private static final int SHIFTED_EXPONENT_MASK = 0x1f;
    // The bitmask to and a number with to obtain significand bits.
    private static final int SIGNIFICAND_MASK = 0x3ff;
    // The offset of the exponent from the actual value.
    private static final int EXPONENT_BIAS = 15;
    // The offset to shift by to obtain the sign bit.
    private static final int SIGN_SHIFT = 15;

    private static final int FP32_SIGN_SHIFT = 31;
    private static final int FP32_EXPONENT_SHIFT = 23;
    private static final int FP32_SHIFTED_EXPONENT_MASK = 0xff;
    private static final int FP32_SIGNIFICAND_MASK = 0x7fffff;
    private static final int FP32_EXPONENT_BIAS = 127;
    private static final int FP32_QNAN_MASK = 0x400000;
    private static final int FP32_DENORMAL_MAGIC = 126 << 23;
    private static final float FP32_DENORMAL_FLOAT = Float.intBitsToFloat(FP32_DENORMAL_MAGIC);

    /**
     * Converts the specified half-precision float value into a single-precision float value. The following special
     * cases are handled: If the input is NaN, the returned value is Float NaN. If the input is POSITIVE_INFINITY or
     * NEGATIVE_INFINITY, the returned value is respectively Float POSITIVE_INFINITY or Float NEGATIVE_INFINITY. If the
     * input is 0 (positive or negative), the returned value is +/-0.0f. Otherwise, the returned value is a normalized
     * single-precision float value.
     *
     * @param b The half-precision float value to convert to single-precision
     * @return A normalized single-precision float value
     */
    public static float toFloat(short b) {
        int bits = b & 0xffff;
        int s = bits & SIGN_MASK;
        int e = (bits >>> EXPONENT_SHIFT) & SHIFTED_EXPONENT_MASK;
        int m = bits & SIGNIFICAND_MASK;
        int outE = 0;
        int outM = 0;
        if (e == 0) { // Denormal or 0
            if (m != 0) {
                // Convert denorm fp16 into normalized fp32
                float o = Float.intBitsToFloat(FP32_DENORMAL_MAGIC + m);
                o -= FP32_DENORMAL_FLOAT;
                return s == 0 ? o : -o;
            }
        } else {
            outM = m << 13;
            if (e == 0x1f) { // Infinite or NaN
                outE = 0xff;
                if (outM != 0) { // SNaNs are quieted
                    outM |= FP32_QNAN_MASK;
                }
            } else {
                outE = e - EXPONENT_BIAS + FP32_EXPONENT_BIAS;
            }
        }
        int out = (s << 16) | (outE << FP32_EXPONENT_SHIFT) | outM;
        return Float.intBitsToFloat(out);
    }

    /**
     * Converts the specified single-precision float value into a half-precision float value. The following special
     * cases are handled:
     *
     * <p>
     * If the input is NaN, the returned value is NaN. If the input is Float POSITIVE_INFINITY or Float
     * NEGATIVE_INFINITY, the returned value is respectively POSITIVE_INFINITY or NEGATIVE_INFINITY. If the input is 0
     * (positive or negative), the returned value is POSITIVE_ZERO or NEGATIVE_ZERO. If the input is a less than
     * MIN_VALUE, the returned value is flushed to POSITIVE_ZERO or NEGATIVE_ZERO. If the input is a less than
     * MIN_NORMAL, the returned value is a denorm half-precision float. Otherwise, the returned value is rounded to the
     * nearest representable half-precision float value.
     *
     * @param f The single-precision float value to convert to half-precision
     * @return A half-precision float value
     */
    public static short toFloat16(float f) {
        int bits = Float.floatToIntBits(f);
        int s = (bits >>> FP32_SIGN_SHIFT);
        int e = (bits >>> FP32_EXPONENT_SHIFT) & FP32_SHIFTED_EXPONENT_MASK;
        int m = bits & FP32_SIGNIFICAND_MASK;
        int outE = 0;
        int outM = 0;
        if (e == 0xff) { // Infinite or NaN
            outE = 0x1f;
            outM = m != 0 ? 0x200 : 0;
        } else {
            e = e - FP32_EXPONENT_BIAS + EXPONENT_BIAS;
            if (e >= 0x1f) { // Overflow
                outE = 0x1f;
            } else if (e <= 0) { // Underflow
                if (e < -10) {
                    // The absolute fp32 value is less than MIN_VALUE, flush to +/-0
                } else {
                    // The fp32 value is a normalized float less than MIN_NORMAL,
                    // we convert to a denorm fp16
                    m = m | 0x800000;
                    int shift = 14 - e;
                    outM = m >> shift;
                    int lowm = m & ((1 << shift) - 1);
                    int hway = 1 << (shift - 1);
                    // if above halfway or exactly halfway and outM is odd
                    if (lowm + (outM & 1) > hway) {
                        // Round to nearest even
                        // Can overflow into exponent bit, which surprisingly is OK.
                        // This increment relies on the +outM in the return statement below
                        outM++;
                    }
                }
            } else {
                outE = e;
                outM = m >> 13;
                // if above halfway or exactly halfway and outM is odd
                if ((m & 0x1fff) + (outM & 0x1) > 0x1000) {
                    // Round to nearest even
                    // Can overflow into exponent bit, which surprisingly is OK.
                    // This increment relies on the +outM in the return statement below
                    outM++;
                }
            }
        }
        // The outM is added here as the +1 increments for outM above can
        // cause an overflow in the exponent bit which is OK.
        return (short) ((s << SIGN_SHIFT) | (outE << EXPONENT_SHIFT) + outM);
    }
}
