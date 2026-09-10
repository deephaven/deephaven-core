//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base.materializers;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.HeapByteBufferAllocator;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.plain.BinaryPlainValuesReader;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestPlainBinaryStringValuesReader {

    private static final List<String> AWKWARD = List.of(
            "", // empty
            "a",
            "plain ascii",
            "naïve café", // 2-byte sequences
            "日本語テキスト", // 3-byte sequences
            "emoji 😀🎉", // surrogate pairs, 4-byte sequences
            "mixed ascii/日本/😀",
            "trailing space ",
            "\0embedded nul");

    /**
     * Byte sequences that are not valid UTF-8. Parquet decodes with {@code Charset.decode}, this reader with
     * {@code new String(byte[], UTF_8)}; both substitute rather than throw, but they are different decoders and can
     * disagree on how many U+FFFD a bad sequence yields.
     */
    private static final List<byte[]> MALFORMED = List.of(
            new byte[] {(byte) 0x80}, // lone continuation
            new byte[] {(byte) 0xC3}, // truncated 2-byte lead
            new byte[] {(byte) 0xE6, (byte) 0x97}, // truncated 3-byte
            new byte[] {(byte) 0xF0, (byte) 0x9F, (byte) 0x98}, // truncated 4-byte
            new byte[] {(byte) 0xC0, (byte) 0x80}, // overlong NUL
            new byte[] {(byte) 0xE0, (byte) 0x80, (byte) 0x80}, // overlong
            new byte[] {(byte) 0xED, (byte) 0xA0, (byte) 0x80}, // CESU-8 surrogate half
            new byte[] {(byte) 0xF5, (byte) 0x80, (byte) 0x80, (byte) 0x80}, // beyond U+10FFFF
            new byte[] {(byte) 0xF8, (byte) 0x88, (byte) 0x80, (byte) 0x80, (byte) 0x80}, // 5-byte
            new byte[] {(byte) 0xFF, (byte) 0xFE}, // invalid leads
            new byte[] {'o', 'k', (byte) 0xC3, '!', 'o', 'k'}); // bad byte between good ones

    private static byte[] encode(final List<String> values) {
        final List<Binary> binaries = new ArrayList<>(values.size());
        for (final String value : values) {
            binaries.add(Binary.fromString(value));
        }
        return encodeBinary(binaries);
    }

    /** As {@link #encode}, but for values that are not valid UTF-8 and so cannot round-trip through String. */
    private static byte[] encodeRaw(final List<byte[]> values) {
        final List<Binary> binaries = new ArrayList<>(values.size());
        for (final byte[] value : values) {
            binaries.add(Binary.fromConstantByteArray(value));
        }
        return encodeBinary(binaries);
    }

    /** Builds a PLAIN BINARY page with parquet's own writer, so the bytes are what a real page would hold. */
    private static byte[] encodeBinary(final List<Binary> values) {
        try (final PlainValuesWriter writer =
                new PlainValuesWriter(64, 64 * 1024, new HeapByteBufferAllocator())) {
            values.forEach(writer::writeBytes);
            return writer.getBytes().toByteArray();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static ByteBuffer pageBuffer(final byte[] encoded) {
        return ByteBuffer.wrap(encoded);
    }

    /** The behaviour the fast reader has to match, value for value. */
    private static String[] decodeWithStockReader(final byte[] encoded, final int count) throws IOException {
        final ValuesReader reader = new BinaryPlainValuesReader();
        reader.initFromPage(count, ByteBufferInputStream.wrap(pageBuffer(encoded)));
        final String[] out = new String[count];
        for (int ii = 0; ii < count; ++ii) {
            out[ii] = reader.readBytes().toStringUsingUTF8();
        }
        return out;
    }

    /**
     * Differential coverage over empty, ASCII, and 2-, 3- and 4-byte UTF-8 values. Both the stock reader's output and
     * the original list are asserted, so a bug that corrupted both readers identically would still fail.
     */
    @Test
    void matchesStockReaderOnAwkwardValues() throws IOException {
        final byte[] encoded = encode(AWKWARD);
        final String[] expected = decodeWithStockReader(encoded, AWKWARD.size());

        final String[] actual = new String[AWKWARD.size()];
        new PlainBinaryStringValuesReader(pageBuffer(encoded)).readStrings(actual, 0, actual.length);

        assertThat(actual).containsExactly(expected);
        assertThat(actual).containsExactlyElementsOf(AWKWARD);
    }

    /**
     * Dirty data must decode to exactly what the stock reader produces, U+FFFD for U+FFFD -- otherwise enabling this
     * path by default silently changes column values.
     */
    @Test
    void matchesStockReaderOnMalformedUtf8() throws IOException {
        final byte[] encoded = encodeRaw(MALFORMED);
        final String[] expected = decodeWithStockReader(encoded, MALFORMED.size());

        final String[] actual = new String[MALFORMED.size()];
        new PlainBinaryStringValuesReader(pageBuffer(encoded)).readStrings(actual, 0, actual.length);

        assertThat(actual).containsExactly(expected);
    }

    /**
     * Broad differential coverage over 5,000 values spanning 1-, 2- and 3-byte sequences. The seed is fixed so any
     * failure is reproducible.
     */
    @Test
    void matchesStockReaderOnRandomValues() throws IOException {
        final Random random = new Random(0xDEADBEEF);
        final List<String> values = new ArrayList<>();
        for (int ii = 0; ii < 5_000; ++ii) {
            final int length = random.nextInt(24);
            final StringBuilder builder = new StringBuilder(length);
            for (int cc = 0; cc < length; ++cc) {
                // Span 1-, 2- and 3-byte UTF-8 sequences.
                builder.append((char) (random.nextInt(3) == 0 ? 0x20 + random.nextInt(95) : random.nextInt(0x2000)));
            }
            values.add(builder.toString());
        }
        final byte[] encoded = encode(values);
        final String[] expected = decodeWithStockReader(encoded, values.size());

        final String[] actual = new String[values.size()];
        new PlainBinaryStringValuesReader(pageBuffer(encoded)).readStrings(actual, 0, actual.length);

        assertThat(actual).containsExactly(expected);
    }

    /**
     * {@code fillValues} is called once per run of non-null values, so the cursor must survive across calls and across
     * gaps left for nulls.
     */
    @Test
    void cursorPersistsAcrossSubRanges() {
        final byte[] encoded = encode(AWKWARD);
        final PlainBinaryStringValuesReader reader = new PlainBinaryStringValuesReader(pageBuffer(encoded));

        final String[] actual = new String[AWKWARD.size()];
        reader.readStrings(actual, 0, 1);
        reader.readStrings(actual, 1, 4);
        reader.readStrings(actual, 4, 5);
        reader.readStrings(actual, 5, AWKWARD.size());

        assertThat(actual).containsExactlyElementsOf(AWKWARD);
    }

    /**
     * {@code readStrings} is the bulk path {@code StringMaterializer} uses; {@code readBytes} is the
     * {@link ValuesReader} contract any other consumer would reach for. The two must agree.
     */
    @Test
    void readStringsAndReadBytesAgree() {
        final byte[] encoded = encode(AWKWARD);
        final PlainBinaryStringValuesReader reader = new PlainBinaryStringValuesReader(pageBuffer(encoded));
        for (final String expected : AWKWARD) {
            assertThat(reader.readBytes().toStringUsingUTF8()).isEqualTo(expected);
        }
    }

    /**
     * {@code skip} must walk the length prefixes exactly as a read would, so that a subsequent read lands on the
     * correct value.
     */
    @Test
    void skipAdvancesLikeRead() {
        final byte[] encoded = encode(AWKWARD);
        final PlainBinaryStringValuesReader reader = new PlainBinaryStringValuesReader(pageBuffer(encoded));
        reader.skip(3);
        final String[] actual = new String[AWKWARD.size()];
        reader.readStrings(actual, 3, AWKWARD.size());
        assertThat(actual[3]).isEqualTo(AWKWARD.get(3));
        assertThat(actual[AWKWARD.size() - 1]).isEqualTo(AWKWARD.get(AWKWARD.size() - 1));
    }

    /**
     * The real page buffer is oversized and reused, with the values somewhere in the middle. Bytes outside
     * {@code [position, limit)} belong to another page and must never be read.
     */
    @Test
    void respectsBufferPositionAndLimit() {
        final byte[] encoded = encode(AWKWARD);
        final byte[] oversized = new byte[encoded.length + 64];
        java.util.Arrays.fill(oversized, (byte) 0x7F);
        System.arraycopy(encoded, 0, oversized, 16, encoded.length);

        final ByteBuffer in = ByteBuffer.wrap(oversized);
        in.position(16).limit(16 + encoded.length);

        final String[] actual = new String[AWKWARD.size()];
        new PlainBinaryStringValuesReader(in).readStrings(actual, 0, actual.length);
        assertThat(actual).containsExactlyElementsOf(AWKWARD);
    }

    /**
     * Because the page buffer is reused and over-allocated, an unchecked overrun would return bytes from the previous
     * page rather than failing. This is the test that makes the bounds check non-optional.
     */
    @Test
    void truncatedPageThrowsRatherThanReadingStaleBytes() {
        final byte[] encoded = encode(AWKWARD);
        // Trim the last value's payload; the length prefix now claims more than the page holds.
        final ByteBuffer in = pageBuffer(encoded);
        in.limit(encoded.length - 3);

        final String[] actual = new String[AWKWARD.size()];
        final PlainBinaryStringValuesReader reader = new PlainBinaryStringValuesReader(in);
        assertThatThrownBy(() -> reader.readStrings(actual, 0, actual.length))
                .isInstanceOf(ParquetDecodingException.class);
    }

    /** The other overrun case: too few bytes remain for even the 4-byte length prefix. */
    @Test
    void truncatedLengthPrefixThrows() {
        final ByteBuffer in = pageBuffer(encode(List.of("abc")));
        in.limit(2);

        final String[] actual = new String[1];
        final PlainBinaryStringValuesReader reader = new PlainBinaryStringValuesReader(in);
        assertThatThrownBy(() -> reader.readStrings(actual, 0, 1))
                .isInstanceOf(ParquetDecodingException.class);
    }

    /**
     * A direct buffer has no backing array, so the reader must both report it unsupported and refuse to construct.
     */
    @Test
    void rejectsNonHeapBuffer() {
        final ByteBuffer direct = ByteBuffer.allocateDirect(16);
        assertThat(PlainBinaryStringValuesReader.isSupported(direct)).isFalse();
        assertThatThrownBy(() -> new PlainBinaryStringValuesReader(direct))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /**
     * {@code initFromPage} is the {@link ValuesReader} lifecycle entry point; a reader initialized that way must decode
     * identically to one built from a buffer directly.
     */
    @Test
    void initFromPageMatchesConstructor() throws IOException {
        final byte[] encoded = encode(AWKWARD);
        final PlainBinaryStringValuesReader reader = new PlainBinaryStringValuesReader();
        reader.initFromPage(AWKWARD.size(), ByteBufferInputStream.wrap(pageBuffer(encoded)));

        final String[] actual = new String[AWKWARD.size()];
        reader.readStrings(actual, 0, actual.length);
        assertThat(actual).containsExactlyElementsOf(AWKWARD);
    }
}
