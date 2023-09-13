/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.compress;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This is the default adapter for LZ4 data. It attempts to decompress with LZ4, and falls back to LZ4_RAW on failure.
 * The fallback mechanism is particularly useful for decompressing parquet files that are compressed with LZ4_RAW but
 * tagged as LZ4 in the metadata.
 */
class LZ4WithLZ4RawBackupCompressorAdapter extends DeephavenCompressorAdapterFactory.CodecWrappingCompressorAdapter {
    private CompressorAdapter lz4RawAdapter = null; // Lazily initialized

    LZ4WithLZ4RawBackupCompressorAdapter(CompressionCodec compressionCodec,
            CompressionCodecName compressionCodecName) {
        super(compressionCodec, compressionCodecName);
    }

    @Override
    public BytesInput decompress(final InputStream inputStream, final int compressedSize,
            final int uncompressedSize) throws IOException {
        // Buffer input data in case we need to retry with LZ4_RAW.
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream, compressedSize);
        bufferedInputStream.mark(compressedSize);
        try {
            return super.decompress(bufferedInputStream, compressedSize, uncompressedSize);
        } catch (IOException e) {
            super.reset();
            bufferedInputStream.reset();
            if (lz4RawAdapter == null) {
                lz4RawAdapter = DeephavenCompressorAdapterFactory.getInstance().getByName("LZ4_RAW");
            }
            return lz4RawAdapter.decompress(bufferedInputStream, compressedSize, uncompressedSize);
        }
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
