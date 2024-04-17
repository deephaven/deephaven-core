//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import io.deephaven.util.SafeCloseable;
import io.deephaven.util.channel.SeekableChannelContext;
import org.apache.hadoop.io.compress.Decompressor;
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
        public BytesInput decompress(final InputStream inputStream, final int compressedSize,
                final int uncompressedSize, final SeekableChannelContext channelContext) {
            return BytesInput.from(inputStream, compressedSize);
        }

        @Override
        public void close() {

        }
    };

    /**
     * Creates a new output stream that will take uncompressed writes, and flush data to the provided stream as
     * compressed data.
     * <p>
     * Note that this method is not thread safe.
     * 
     * @param os the output stream to write compressed contents to
     * @return an output stream that can accept writes
     * @throws IOException thrown if an error occurs writing data
     */
    OutputStream compress(OutputStream os) throws IOException;

    /**
     * Returns an in-memory instance of BytesInput containing the fully decompressed results of the input stream. The
     * provided {@link DecompressorHolder} is used for decompressing if compatible with the compression codec.
     * Otherwise, a new decompressor is created and set in the DecompressorHolder. Callers should process the results
     * before {@code inputStream} is closed; if the {@link BytesInput} interface needs to persist longer than
     * {@code inputStream}, callers should use {@link BytesInput#copy(BytesInput)} on the results.
     * <p>
     * Note that this method is thread safe, assuming the {@link SeekableChannelContext} instances are not shared across
     * threads.
     * 
     * @param inputStream an input stream containing compressed data
     * @param compressedSize the number of bytes in the compressed data
     * @param uncompressedSize the number of bytes that should be present when decompressed
     * @param channelContext the context which can store any additional resources link a {@link Decompressor} to use for
     *        reading from the input stream
     * @return the decompressed bytes, copied into memory
     * @throws IOException thrown if an error occurs reading data.
     */
    BytesInput decompress(InputStream inputStream, int compressedSize, int uncompressedSize,
            SeekableChannelContext channelContext) throws IOException;

    /**
     * @return the CompressionCodecName enum value that represents this compressor.
     */
    CompressionCodecName getCodecName();
}
