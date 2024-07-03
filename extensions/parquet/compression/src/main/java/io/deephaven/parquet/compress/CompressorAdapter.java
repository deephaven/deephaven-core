//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.compress;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.util.SafeCloseable;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.function.BiFunction;
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
        public InputStream decompress(final InputStream inputStream, final int compressedSize,
                final int uncompressedSize,
                final BiFunction<String, Supplier<SafeCloseable>, SafeCloseable> decompressorCache) {
            return inputStream;
        }

        @Override
        public void reset() {}

        @Override
        public void close() {}
    };

    // TODO What would be a good place to keep this method?
    /**
     * Reads exactly nBytes from the input stream into the provided byte array.
     *
     * @param in The input stream to read from
     * @param nBytes The number of bytes to read
     * @param bytes The byte array to read into
     * @return A ByteBuffer wrapping the byte array with the limit set to nBytes
     * @throws IOException If an error occurs while reading from the input stream
     * @throws IllegalArgumentException If the byte array is too small to read nBytes
     * @throws UncheckedDeephavenException If the expected number of bytes could not be read
     */
    static ByteBuffer readNBytes(final InputStream in, final int nBytes, final byte[] bytes) throws IOException {
        if (nBytes > bytes.length) {
            throw new IllegalArgumentException("bytes array of length " + bytes.length + " is too small to read "
                    + nBytes + " bytes");
        }
        return readNBytesHelper(in, nBytes, bytes);
    }

    /**
     * Reads exactly nBytes from the input stream into a new byte buffer and returns it.
     *
     * @see #readNBytes(InputStream, int, byte[])
     */
    static ByteBuffer readNBytes(final InputStream in, final int nBytes) throws IOException {
        return readNBytesHelper(in, nBytes, new byte[nBytes]);
    }

    private static ByteBuffer readNBytesHelper(final InputStream in, final int nBytes, final byte[] bytes)
            throws IOException {
        int numRead = 0;
        while (numRead < nBytes) {
            final int count = in.read(bytes, numRead, nBytes - numRead);
            if (count == -1) {
                throw new UncheckedDeephavenException("Expected to read " + nBytes + " bytes, but read " +
                        numRead + " bytes");
            }
            numRead += count;
        }
        return ByteBuffer.wrap(bytes, 0, nBytes);
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
    InputStream decompress(InputStream inputStream, int compressedSize, int uncompressedSize,
            BiFunction<String, Supplier<SafeCloseable>, SafeCloseable> decompressorCache) throws IOException;

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
