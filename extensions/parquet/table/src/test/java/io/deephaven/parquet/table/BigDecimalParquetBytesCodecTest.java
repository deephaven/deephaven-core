//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.parquet.base.BigDecimalParquetBytesCodec;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class BigDecimalParquetBytesCodecTest {

    @Test
    public void examples() {
        // [.0000, .9999]
        checkNoRounding(codec(4, 4), bd("0.0005"), bi(5));

        // [0.0000, 9.9999]
        checkNoRounding(codec(5, 4), bd("0.0005"), bi(5));

        // [.000, .999]
        check(codec(3, 3), bd("0.0005"), bi(1), bd("0.001"));

        // [0.000, 9.999]
        check(codec(4, 3), bd("0.0005"), bi(1), bd("0.001"));

        // [.0000, .9999]
        check(codec(4, 4), bd("0.00050"), bi(5), bd("0.0005"));

        // [0.0000, 9.9999]
        check(codec(5, 4), bd("0.00050"), bi(5), bd("0.0005"));

        // [.00000, .99999]
        check(codec(5, 5), bd("0.0005"), bi(50), bd("0.00050"));

        // [0.00000, 9.99999]
        check(codec(6, 5), bd("0.0005"), bi(50), bd("0.00050"));

        // [00.000, 99.999]
        checkNoRounding(codec(5, 3), bd("0.123"), bi(123));

        // [00.000, 99.999]
        checkNoRounding(codec(5, 3), bd("99.999"), bi(99999));
    }

    @Test
    public void badEncodings() {
        cantEncode(codec(3, 3), bd("1.0"));
        cantEncode(codec(4, 3), bd("10.0"));
    }

    @Test
    public void badDecodings() {
        cantDecode(codec(3, 3), bi(1000));
        cantDecode(codec(4, 3), bi(10000));
    }

    private static BigDecimal bd(String val) {
        return new BigDecimal(val);
    }

    private static BigInteger bi(int val) {
        return BigInteger.valueOf(val);
    }

    private static BigDecimalParquetBytesCodec codec(int precision, int scale) {
        return new BigDecimalParquetBytesCodec(precision, scale);
    }

    private static void checkNoRounding(BigDecimalParquetBytesCodec codec, BigDecimal input,
            BigInteger expectedEncoding) {
        check(codec, input, expectedEncoding, input);
    }

    private static void check(BigDecimalParquetBytesCodec codec, BigDecimal input, BigInteger expectedEncoding,
            BigDecimal expectedDecoding) {
        checkEncoding(codec, input, expectedEncoding);
        checkDecoding(codec, expectedEncoding, expectedDecoding);
        // Check negative
        checkEncoding(codec, input.negate(), expectedEncoding.negate());
        checkDecoding(codec, expectedEncoding.negate(), expectedDecoding.negate());
    }

    private static void checkEncoding(BigDecimalParquetBytesCodec codec, BigDecimal input,
            BigInteger expectedEncoding) {
        final byte[] inputEncoded = codec.encode(input);
        assertArrayEquals(expectedEncoding.toByteArray(), inputEncoded);
    }

    private static void checkDecoding(BigDecimalParquetBytesCodec codec, BigInteger encoding,
            BigDecimal expectedDecoding) {
        if (expectedDecoding.precision() > codec.getPrecision()) {
            throw new IllegalStateException();
        }
        if (expectedDecoding.scale() != codec.getScale()) {
            throw new IllegalStateException();
        }
        final byte[] bytes = encoding.toByteArray();
        final BigDecimal decoded = codec.decode(bytes, 0, bytes.length);
        assertEquals(expectedDecoding, decoded);
        // Anything value that is decodable should be exactly re-encodable
        checkEncoding(codec, expectedDecoding, encoding);
    }

    private static void cantEncode(BigDecimalParquetBytesCodec codec, BigDecimal input) {
        cantEncodeImpl(codec, input);
        cantEncodeImpl(codec, input.negate());
    }

    private static void cantDecode(BigDecimalParquetBytesCodec codec, BigInteger encoding) {
        cantDecodeImpl(codec, encoding);
        cantDecodeImpl(codec, encoding.negate());
    }

    private static void cantEncodeImpl(BigDecimalParquetBytesCodec codec, BigDecimal input) {
        try {
            codec.encode(input);
            fail("Expected ArithmeticException");
        } catch (ArithmeticException e) {
            // ignore
        }
    }

    private static void cantDecodeImpl(BigDecimalParquetBytesCodec codec, BigInteger encoding) {
        final byte[] bytes = encoding.toByteArray();
        try {
            codec.decode(bytes, 0, bytes.length);
            fail("Expected ArithmeticException");
        } catch (ArithmeticException e) {
            // ignore
        }
    }
}
