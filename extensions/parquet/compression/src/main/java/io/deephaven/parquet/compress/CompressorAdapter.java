//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import io.deephaven.util.SafeCloseable;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.function.Supplier;

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
        public InputStream decompress(
                final InputStream inputStream,
                final int compressedSize,
                final int uncompressedSize,
                final ResourceCache decompressorCache) {
            return inputStream;
        }

        @Override
        public void reset() {}

        @Override
        public void close() {}
    };

    /**
     * Reads exactly {@code length} number of bytes from the input stream into the provided byte array.
     *
     * @param in The input stream to read from
     * @param bytes The byte array to read into
     * @param offset The offset in the byte array to start reading at
     * @param length The number of bytes to read
     * @throws IOException If an error occurs while reading from the input stream
     * @throws IllegalArgumentException If the byte array is too small to read {@code length} number of bytes
     * @throws IOException If the expected number of bytes could not be read
     */
    static void readNBytes(final InputStream in, final byte[] bytes, final int offset, final int length)
            throws IOException {
        if (length > bytes.length - offset) {
            throw new IllegalArgumentException("Bytes array of length " + bytes.length + " with offset " + offset +
                    " is too small to read " + length + " bytes");
        }
        readNBytesHelper(in, bytes, offset, length);
    }

    /**
     * Reads exactly {@code length} number of bytes from the input stream into a new byte array and returns it.
     *
     * @see #readNBytesHelper(InputStream, byte[], int, int)
     */
    static byte[] readNBytes(final InputStream in, final int length) throws IOException {
        final byte[] bytes = new byte[length];
        readNBytesHelper(in, bytes, 0, length);
        return bytes;
    }

    private static void readNBytesHelper(final InputStream in, final byte[] bytes, final int offset, final int length)
            throws IOException {
        int numRead = 0;
        while (numRead < length) {
            final int count = in.read(bytes, offset + numRead, length - numRead);
            if (count == -1) {
                throw new IOException("Expected to read " + length + " bytes, but read " + numRead + " bytes");
            }
            numRead += count;
        }
    }

    /**
     * An {@link InputStream} that will not close the underlying stream when {@link InputStream#close()} is called.
     */
    final class InputStreamNoClose extends FilterInputStream {
        InputStreamNoClose(final InputStream in) {
            super(in);
        }

        @Override
        public void close() {
            // Do not close the underlying stream.
        }
    }

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

    interface ResourceCache {
        DecompressorHolder get(Supplier<DecompressorHolder> factory);
    }

    /**
     * Creates a new input stream that will read compressed data from the provided stream, and return uncompressed data.
     * Caller should not close the returned {@link InputStream} because this might return an internally cached
     * decompressor to the pool. Also, callers should not read more data from the returned {@link InputStream} after
     * closing the argument {@code inputStream}.
     * <p>
     * The provided {@link DecompressorHolder} is used for decompressing if compatible with the compression codec.
     * Otherwise, a new decompressor is created and set in the DecompressorHolder.
     * <p>
     * Note that this method is thread safe, assuming the cached decompressor instances are not shared across threads.
     * 
     * @param inputStream an input stream containing compressed data
     * @param compressedSize the number of bytes in the compressed data
     * @param uncompressedSize the number of bytes that should be present when decompressed
     * @param decompressorCache Used to cache {@link Decompressor} instances for reuse
     * @return an input stream that will return uncompressed data
     * @throws IOException thrown if an error occurs reading data.
     */
    InputStream decompress(
            InputStream inputStream,
            int compressedSize,
            int uncompressedSize,
            ResourceCache decompressorCache) throws IOException;

    /**
     * @return the CompressionCodecName enum value that represents this compressor.
     */
    CompressionCodecName getCodecName();

    /**
     * Reset the internal state of this {@link CompressorAdapter} so more rows can be read or written.
     * <p>
     * This method can be called after {@link #compress} to reset the internal state of the compressor, and is not
     * required before {@link #compress}, or before and after {@link #decompress} because those methods internally
     * manage their own state.
     */
    void reset();
}
