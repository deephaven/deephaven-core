//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static io.deephaven.grpc.compression.ZstdCodec.WINDOW_LOG_MAX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ZstdCodecTest {

    @Test
    public void decodesFramesWithTheLargestWindow() throws IOException {
        assertThat(decompress(frame(WINDOW_LOG_MAX, 0))).containsExactly('x');
    }

    @Test
    public void rejectsFramesWithALargerWindow() {
        // the smallest window larger than 2^WINDOW_LOG_MAX: an eighth more
        assertThatThrownBy(() -> decompress(frame(WINDOW_LOG_MAX, 1)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Frame requires too much memory for decoding");
    }

    private static byte[] decompress(final byte[] frame) throws IOException {
        try (final InputStream is = ZstdCodec.INSTANCE.decompress(new ByteArrayInputStream(frame))) {
            return is.readAllBytes();
        }
    }

    /**
     * @param windowLog the base-2 logarithm of the window, from 10 to 41
     * @param eighths the eighths of 2^windowLog to add to the window, from 0 to 7
     * @return a zstd frame that declares the window, with no content size, holding the single byte {@code 'x'}
     */
    private static byte[] frame(final int windowLog, final int eighths) {
        // a block header: the last block, of type RLE, repeating the next byte once
        final int blockHeader = 1 | 1 << 1 | 1 << 3;
        return new byte[] {
                // the magic number
                0x28, (byte) 0xb5, 0x2f, (byte) 0xfd,
                // the frame header descriptor: no content size, checksum or dictionary
                0,
                // the window descriptor
                (byte) ((windowLog - 10) << 3 | eighths),
                (byte) blockHeader, (byte) (blockHeader >>> 8), (byte) (blockHeader >>> 16),
                'x'};
    }
}
