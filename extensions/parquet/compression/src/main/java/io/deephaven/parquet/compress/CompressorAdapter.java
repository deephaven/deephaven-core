/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.parquet.compress;

import io.deephaven.util.SafeCloseable;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * An Intermediate adapter interface between Deephaven column writing and parquet compression.
 */
public interface CompressorAdapter extends SafeCloseable {
    /**
     * An {@link CompressorAdapter} instance that reads and writes uncompressed data directly.
     */
    CompressorAdapter PASSTHRU = new CompressorAdapter() {
        @Override
        public OutputStream compress(OutputStream os) {
            return os;
        }

        @Override
        public CompressionCodecName getCodecName() {
            return CompressionCodecName.UNCOMPRESSED;
        }

        @Override
        public BytesInput decompress(InputStream inputStream, int compressedSize, int uncompressedSize) {
            return BytesInput.from(inputStream, compressedSize);
        }

        @Override
        public void reset() {}

        @Override
        public void close() {

        }
    };

    /**
     * Creates a new output stream that will take uncompressed writes, and flush data to the provided stream as
     * compressed data.
     * 
     * @param os the output stream to write compressed contents to
     * @return an output stream that can accept writes
     * @throws IOException thrown if an error occurs writing data
     */
    OutputStream compress(OutputStream os) throws IOException;

    /**
     * Returns an in-memory instance of BytesInput containing the fully decompressed results of the input stream.
     * 
     * @param inputStream an input stream containing compressed data
     * @param compressedSize the number of bytes in the compressed data
     * @param uncompressedSize the number of bytes that should be present when decompressed
     * @return the decompressed bytes, copied into memory
     * @throws IOException thrown if an error occurs reading data.
     */
    BytesInput decompress(InputStream inputStream, int compressedSize, int uncompressedSize) throws IOException;

    /**
     * @return the CompressionCodecName enum value that represents this compressor.
     */
    CompressionCodecName getCodecName();

    /**
     * Reset the internal state of this {@link CompressorAdapter} so more rows can be read or written.
     */
    void reset();
}
