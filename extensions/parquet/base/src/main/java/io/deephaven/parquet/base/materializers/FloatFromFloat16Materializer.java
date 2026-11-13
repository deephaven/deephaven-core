//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import io.deephaven.parquet.base.PageMaterializer;
import io.deephaven.parquet.base.PageMaterializerFactory;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.api.Binary;

import java.util.Arrays;

/**
 * Reads a parquet {@code FLOAT16} column, a two-byte little-endian IEEE 754 half-precision value, into {@code float}s.
 */
public class FloatFromFloat16Materializer implements PageMaterializer {

    public static final PageMaterializerFactory FACTORY = new PageMaterializerFactory() {
        @Override
        public PageMaterializer makeMaterializerWithNulls(ValuesReader dataReader, Object nullValue, int numValues) {
            return new FloatFromFloat16Materializer(dataReader, (float) nullValue, numValues);
        }

        @Override
        public PageMaterializer makeMaterializerNonNull(ValuesReader dataReader, int numValues) {
            return new FloatFromFloat16Materializer(dataReader, numValues);
        }
    };

    /**
     * Converts the little-endian half-precision value in {@code value}.
     */
    public static float convertValue(final Binary value) {
        return float16ToFloat(value.get2BytesLittleEndian());
    }

    /**
     * Converts half-precision bits to the {@code float} of equal value. Exact, since every half is a float; NaN
     * payloads are preserved.
     */
    // Replace with Java 20's Float.float16ToFloat once that is the minimum supported version.
    public static float float16ToFloat(final short halfBits) {
        final int sign = (halfBits & 0x8000) << 16;
        final int exponent = (halfBits >>> 10) & 0x1f;
        final int significand = halfBits & 0x3ff;
        if (exponent == 0x1f) {
            // Infinity or NaN
            return Float.intBitsToFloat(sign | 0x7f800000 | (significand << 13));
        }
        if (exponent == 0) {
            // Zero or subnormal: significand * 2^-24, exact in float
            final float magnitude = significand * 0x1p-24f;
            return sign == 0 ? magnitude : -magnitude;
        }
        // Normal: rebias the exponent from 15 to 127
        return Float.intBitsToFloat(sign | ((exponent + (127 - 15)) << 23) | (significand << 13));
    }

    private final ValuesReader dataReader;

    private final float nullValue;
    private final float[] data;

    private FloatFromFloat16Materializer(ValuesReader dataReader, int numValues) {
        this(dataReader, 0, numValues);
    }

    private FloatFromFloat16Materializer(ValuesReader dataReader, float nullValue, int numValues) {
        this.dataReader = dataReader;
        this.nullValue = nullValue;
        this.data = new float[numValues];
    }

    @Override
    public void fillNulls(int startIndex, int endIndex) {
        Arrays.fill(data, startIndex, endIndex, nullValue);
    }

    @Override
    public void fillValues(int startIndex, int endIndex) {
        for (int ii = startIndex; ii < endIndex; ii++) {
            data[ii] = convertValue(dataReader.readBytes());
        }
    }

    @Override
    public Object fillAll() {
        fillValues(0, data.length);
        return data;
    }

    @Override
    public Object data() {
        return data;
    }
}
