//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.zip.CRC32C;

import static io.deephaven.grpc.compression.SnappyCodec.MAX_DATA_CHUNK_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class SnappyCodecTest {

    /**
     * The stream identifier chunk that begins every stream.
     */
    private static final byte[] STREAM_IDENTIFIER = {(byte) 0xff, 6, 0, 0, 's', 'N', 'a', 'P', 'p', 'Y'};

    private static final int COMPRESSED_DATA = 0x00;
    private static final int UNCOMPRESSED_DATA = 0x01;
    private static final int PADDING = 0xfe;

    @Test
    public void roundTripsCompressedAndUncompressedChunks() throws IOException {
        // random bytes do not compress, so snappy-java stores them in uncompressed chunks; the text compresses
        final byte[] random = new byte[300_000];
        new Random(0).nextBytes(random);
        final ByteArrayOutputStream data = new ByteArrayOutputStream();
        data.write(random);
        for (int i = 0; i < 20_000; ++i) {
            data.write(("row " + i + ", ").getBytes(StandardCharsets.US_ASCII));
        }
        final byte[] expected = data.toByteArray();
        final byte[] compressed = compress(expected);
        assertThat(chunkTypes(compressed)).contains(COMPRESSED_DATA, UNCOMPRESSED_DATA);
        assertThat(decompress(compressed)).isEqualTo(expected);
    }

    @Test
    public void decodesTheLongestValidDataChunk() throws IOException {
        // the most data that a chunk holds, encoded as wastefully as the Snappy format allows
        final byte[] expected = new byte[65_536];
        new Random(0).nextBytes(expected);
        final byte[] body = concat(checksum(expected), longestEncoding(expected));
        assertThat(body).hasSize(MAX_DATA_CHUNK_LENGTH);
        assertThat(decompress(concat(STREAM_IDENTIFIER, chunk(COMPRESSED_DATA, body)))).isEqualTo(expected);
    }

    @Test
    public void rejectsLongerDataChunksAtTheHeader() {
        // only the header: the stream fails on the declared length, before snappy-java reads the chunk
        for (final int type : new int[] {COMPRESSED_DATA, UNCOMPRESSED_DATA}) {
            assertThatThrownBy(() -> decompress(concat(STREAM_IDENTIFIER, header(type, MAX_DATA_CHUNK_LENGTH + 1))))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("a data chunk of " + (MAX_DATA_CHUNK_LENGTH + 1) + " bytes");
        }
    }

    @Test
    public void skippableChunksPassAtAnyLength() throws IOException {
        final byte[] expected = "after the padding".getBytes(StandardCharsets.US_ASCII);
        final byte[] compressed = compress(expected);
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        stream.write(compressed, 0, STREAM_IDENTIFIER.length);
        stream.write(chunk(PADDING, new byte[MAX_DATA_CHUNK_LENGTH + 1]));
        stream.write(compressed, STREAM_IDENTIFIER.length, compressed.length - STREAM_IDENTIFIER.length);
        assertThat(decompress(stream.toByteArray())).isEqualTo(expected);
    }

    private static byte[] compress(final byte[] data) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (final OutputStream os = SnappyCodec.INSTANCE.compress(out)) {
            os.write(data);
        }
        return out.toByteArray();
    }

    private static byte[] decompress(final byte[] stream) throws IOException {
        try (final InputStream is = SnappyCodec.INSTANCE.decompress(new ByteArrayInputStream(stream))) {
            return is.readAllBytes();
        }
    }

    /**
     * @return the type of each chunk in {@code stream}, in order
     */
    private static List<Integer> chunkTypes(final byte[] stream) {
        final List<Integer> types = new ArrayList<>();
        int pos = 0;
        while (pos < stream.length) {
            types.add(stream[pos] & 0xFF);
            pos += 4 + ((stream[pos + 1] & 0xFF) | (stream[pos + 2] & 0xFF) << 8 | (stream[pos + 3] & 0xFF) << 16);
        }
        return types;
    }

    /**
     * @return the masked CRC-32C checksum of {@code data}, with which a data chunk begins
     */
    private static byte[] checksum(final byte[] data) {
        final CRC32C crc = new CRC32C();
        crc.update(data);
        final int value = (int) crc.getValue();
        final int masked = ((value >>> 15) | (value << 17)) + 0xa282ead8;
        return new byte[] {(byte) masked, (byte) (masked >>> 8), (byte) (masked >>> 16), (byte) (masked >>> 24)};
    }

    /**
     * @return the longest valid Snappy encoding of {@code data}: the length as a 5-byte varint, then each byte as a
     *         literal of 1 byte with a 4-byte length, 6 bytes for each byte of {@code data}
     */
    private static byte[] longestEncoding(final byte[] data) {
        final byte[] encoding = new byte[5 + 6 * data.length];
        for (int i = 0; i < 4; ++i) {
            encoding[i] = (byte) (data.length >>> (7 * i) & 0x7f | 0x80);
        }
        encoding[4] = (byte) (data.length >>> 28);
        for (int i = 0; i < data.length; ++i) {
            final int pos = 5 + 6 * i;
            // a literal tag whose length follows in 4 bytes, holding the length minus 1, which is 0
            encoding[pos] = (byte) (63 << 2);
            encoding[pos + 5] = data[i];
        }
        return encoding;
    }

    /**
     * @return a chunk header of {@code type}, declaring {@code length} bytes after the header
     */
    private static byte[] header(final int type, final int length) {
        return new byte[] {(byte) type, (byte) length, (byte) (length >>> 8), (byte) (length >>> 16)};
    }

    /**
     * @return a chunk of {@code type} holding {@code body}
     */
    private static byte[] chunk(final int type, final byte[] body) {
        return concat(header(type, body.length), body);
    }

    private static byte[] concat(final byte[] first, final byte[] second) {
        final byte[] result = new byte[first.length + second.length];
        System.arraycopy(first, 0, result, 0, first.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }
}
