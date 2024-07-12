//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import io.deephaven.io.streams.ByteBufferInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This is the default adapter for LZ4 files. It attempts to decompress with LZ4 and falls back to LZ4_RAW on failure.
 * After that, it always uses LZ4_RAW for decompression. This fallback mechanism is particularly useful for
 * decompressing parquet files that are compressed with LZ4_RAW but tagged as LZ4 in the metadata. This adapter is
 * internally stateful in some cases, and therefore a single instance should not be re-used across files.
 */
class LZ4WithLZ4RawBackupCompressorAdapter extends DeephavenCompressorAdapterFactory.CodecWrappingCompressorAdapter {
    private enum DecompressionMode {
        INIT, LZ4, LZ4_RAW
    }

    private DecompressionMode mode = DecompressionMode.INIT;

    /**
     * Only initialized if we hit an exception while decompressing with LZ4.
     */
    private CompressorAdapter lz4RawAdapter;

    LZ4WithLZ4RawBackupCompressorAdapter(CompressionCodec compressionCodec,
            CompressionCodecName compressionCodecName) {
        super(compressionCodec, compressionCodecName);
    }

    @Override
    public InputStream decompress(
            final InputStream inputStream,
            final int compressedSize,
            final int uncompressedSize,
            final ResourceCache decompressorCache)
            throws IOException {
        if (mode == DecompressionMode.LZ4) {
            return super.decompress(inputStream, compressedSize, uncompressedSize, decompressorCache);
        }
        if (mode == DecompressionMode.LZ4_RAW) {
            // LZ4_RAW adapter should have been initialized if we hit this case.
            return lz4RawAdapter.decompress(inputStream, compressedSize, uncompressedSize, decompressorCache);
        }
        // Buffer input data in case we need to retry with LZ4_RAW.
        final BufferedInputStream bufferedInputStream = IOUtils.buffer(inputStream, compressedSize);
        bufferedInputStream.mark(compressedSize);
        try {
            // Try to decompress the bytes with LZ4 first.
            final InputStream decompressedInput =
                    super.decompress(bufferedInputStream, compressedSize, uncompressedSize, decompressorCache);
            final byte[] decompressedBytes = CompressorAdapter.readNBytes(decompressedInput, uncompressedSize);
            // If we got here, we successfully decompressed with LZ4.
            mode = DecompressionMode.LZ4;
            return new ByteArrayInputStream(decompressedBytes);
        } catch (final IOException ignored) {
        }

        // Retry with LZ4_RAW.
        lz4RawAdapter = DeephavenCompressorAdapterFactory.getInstance().getByName("LZ4_RAW");
        mode = DecompressionMode.LZ4_RAW;
        bufferedInputStream.reset();
        return lz4RawAdapter.decompress(bufferedInputStream, compressedSize, uncompressedSize, decompressorCache);
    }

    @Override
    public void reset() {
        super.reset();
        if (lz4RawAdapter != null) {
            lz4RawAdapter.reset();
        }
    }

    @Override
    public void close() {
        super.close();
        if (lz4RawAdapter != null) {
            lz4RawAdapter.close();
        }
    }
}
