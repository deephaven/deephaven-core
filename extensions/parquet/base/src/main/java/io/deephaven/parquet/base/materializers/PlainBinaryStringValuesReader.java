//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Reads PLAIN-encoded BINARY values directly out of the heap array behind the page buffer.
 * <p>
 * The stock {@code BinaryPlainValuesReader} allocates a {@link ByteBuffer} slice and a {@code Binary} wrapper per
 * value, both of which {@code toStringUsingUTF8()} immediately discards. Decoding from the array skips them, leaving
 * only the {@code String} and its {@code byte[]}, and reducing GC.
 *
 * @see PlainBinaryStringMaterializer
 */
public final class PlainBinaryStringValuesReader extends ValuesReader {

    private byte[] array;
    /** Index into {@link #array} of the next value's length prefix. */
    private int position;
    /** Index into {@link #array} one past the last readable byte of this page. */
    private int limit;

    /**
     * @param in A heap-backed buffer positioned at the first value and limited to the end of the page
     */
    public PlainBinaryStringValuesReader(final ByteBuffer in) {
        if (!isSupported(in)) {
            throw new IllegalArgumentException("Page buffer is not heap-backed");
        }
        array = in.array();
        position = in.arrayOffset() + in.position();
        limit = in.arrayOffset() + in.limit();
    }

    /** Whether {@code in} exposes a backing array, which is the whole point of this reader. */
    public static boolean isSupported(final ByteBuffer in) {
        return in.hasArray();
    }

    /**
     * Decode {@code [from, to)} into {@code dest}. The cursor persists across calls, since
     * {@link io.deephaven.parquet.base.PageMaterializer#fillValues} is invoked once per run of non-null values.
     */
    public void readStrings(final String[] dest, final int from, final int to) {
        // Hoisted to locals so the array stores below cannot force the JIT to reload the fields each iteration.
        final byte[] localArray = array;
        final int localLimit = limit;
        int pos = position;
        for (int ii = from; ii < to; ++ii) {
            final int length = readLength(localArray, pos, localLimit);
            pos += Integer.BYTES;
            if (length > localLimit - pos) {
                throw overrun(length, localLimit - pos);
            }
            dest[ii] = new String(localArray, pos, length, StandardCharsets.UTF_8);
            pos += length;
        }
        position = pos;
    }

    /**
     * Unsupported: Deephaven materializes whole pages, and nulls consume no page bytes, so nothing skips. Every other
     * {@link ValuesReader} accessor is left to the base class, which throws for the same reason.
     */
    @Override
    public void skip() {
        throw new UnsupportedOperationException("PlainBinaryStringValuesReader supports only readStrings");
    }

    /**
     * Read a 4-byte little-endian length, bounds-checked. The check is not optional: the page buffer is a reused,
     * over-allocated cache buffer, so reading past {@code limit} would quietly return stale bytes from the previous
     * page instead of failing.
     */
    private static int readLength(final byte[] array, final int pos, final int limit) {
        if (limit - pos < Integer.BYTES) {
            throw new ParquetDecodingException(
                    "Ran out of page data reading a PLAIN BINARY length prefix; " + (limit - pos) + " bytes remain");
        }
        // Duplicates BytesUtils.readIntLittleEndian, which declares an IOException it cannot throw. Copying the math
        // avoids a catch block that can never run.
        final int length = (array[pos] & 0xFF)
                | (array[pos + 1] & 0xFF) << 8
                | (array[pos + 2] & 0xFF) << 16
                | (array[pos + 3] & 0xFF) << 24;
        if (length < 0) {
            throw new ParquetDecodingException("Negative PLAIN BINARY value length " + length);
        }
        return length;
    }

    private static ParquetDecodingException overrun(final int length, final int remaining) {
        return new ParquetDecodingException(
                "PLAIN BINARY value of length " + length + " overruns the page; " + remaining + " bytes remain");
    }
}
