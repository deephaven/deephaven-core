//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import com.google.auto.service.AutoService;
import io.grpc.Codec;
import org.xerial.snappy.Snappy;
import org.xerial.snappy.SnappyFramedInputStream;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A gRPC {@link Codec} for the {@code snappy} message encoding, using the standard Snappy framing format (with CRC32C
 * checksums) from snappy-java.
 */
public final class SnappyCodec implements Codec {

    /**
     * The gRPC message encoding name, as it appears in {@code grpc-encoding} and {@code grpc-accept-encoding}.
     */
    public static final String ENCODING = "snappy";

    public static final SnappyCodec INSTANCE = new SnappyCodec();

    /**
     * The longest data chunk that a valid stream holds, after the chunk header: the framing format limits a chunk to
     * 65,536 bytes of uncompressed data, and the Snappy format spends at most 6 bytes on each (a 1-byte literal with a
     * 4-byte length), after a varint length of at most 5 bytes and the chunk's 4-byte checksum. Snappy encoders write
     * far shorter chunks, at most 32 + n + n / 6 bytes of compressed data, but every valid encoding passes.
     */
    static final int MAX_DATA_CHUNK_LENGTH = 4 + 5 + 6 * 65_536;

    private SnappyCodec() {}

    @Override
    public String getMessageEncoding() {
        return ENCODING;
    }

    @Override
    public OutputStream compress(final OutputStream os) throws IOException {
        return new SnappyFramedOutputStream(os);
    }

    @Override
    public InputStream decompress(final InputStream is) throws IOException {
        return new AvailableUntilEof(new SnappyFramedInputStream(new ChunkLengthLimit(is)));
    }

    /**
     * Rejects a data chunk longer than {@link #MAX_DATA_CHUNK_LENGTH} when the chunk's header passes through, before
     * {@link SnappyFramedInputStream} reads the header. snappy-java sizes the decoder's buffers from the length in each
     * chunk header, so the check keeps those buffers within what the framing format allows. A skippable chunk passes at
     * any length, as the format permits, since snappy-java skips the chunk without allocating. Extends
     * {@link InputStream} rather than {@link FilterInputStream} so that every read goes through
     * {@link #read(byte[], int, int)}, which follows the chunk headers.
     */
    private static final class ChunkLengthLimit extends InputStream {
        private static final int COMPRESSED_DATA = 0x00;
        private static final int UNCOMPRESSED_DATA = 0x01;

        private final InputStream in;
        private final byte[] single = new byte[1];
        /**
         * The current chunk's header, as read so far.
         */
        private final byte[] header = new byte[4];
        /**
         * The number of bytes of {@link #header} read so far.
         */
        private int headerRead;
        /**
         * The number of bytes of the current chunk, after the header, not read yet.
         */
        private int chunkRemaining;

        private ChunkLengthLimit(final InputStream in) {
            this.in = in;
        }

        @Override
        public int read() throws IOException {
            return read(single, 0, 1) < 0 ? -1 : single[0] & 0xFF;
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            final int count = in.read(b, off, len);
            final int end = off + Math.max(count, 0);
            int pos = off;
            while (pos < end) {
                if (chunkRemaining > 0) {
                    final int passed = Math.min(chunkRemaining, end - pos);
                    chunkRemaining -= passed;
                    pos += passed;
                } else {
                    header[headerRead++] = b[pos++];
                    if (headerRead == header.length) {
                        headerRead = 0;
                        chunkRemaining = checkLength(header);
                    }
                }
            }
            return count;
        }

        /**
         * @param header a chunk header: the chunk type, then the length of the rest of the chunk in 3 little-endian
         *        bytes
         * @return the length of the rest of the chunk
         * @throws IOException if the chunk is a data chunk longer than {@link #MAX_DATA_CHUNK_LENGTH}
         */
        private static int checkLength(final byte[] header) throws IOException {
            final int type = header[0] & 0xFF;
            final int length = (header[1] & 0xFF) | (header[2] & 0xFF) << 8 | (header[3] & 0xFF) << 16;
            if ((type == COMPRESSED_DATA || type == UNCOMPRESSED_DATA) && length > MAX_DATA_CHUNK_LENGTH) {
                throw new IOException("Invalid snappy stream: a data chunk of " + length + " bytes, longer than the "
                        + MAX_DATA_CHUNK_LENGTH + " bytes that the framing format allows");
            }
            return length;
        }

        @Override
        public int available() throws IOException {
            return in.available();
        }

        @Override
        public void close() throws IOException {
            in.close();
        }
    }

    /**
     * {@link SnappyFramedInputStream#available()} is 0 until a frame has been decompressed, but some readers (notably
     * Arrow Flight's {@code ArrowMessage}) parse a message only while {@code available() > 0}, and would see an empty
     * message. Like {@link java.util.zip.InflaterInputStream}, this reports at least 1 until the end of the stream has
     * been read.
     */
    private static final class AvailableUntilEof extends FilterInputStream {
        private boolean eof;

        private AvailableUntilEof(final InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            final int value = super.read();
            if (value < 0) {
                eof = true;
            }
            return value;
        }

        @Override
        public int read(final byte[] b, final int off, final int len) throws IOException {
            final int count = super.read(b, off, len);
            if (count < 0) {
                eof = true;
            }
            return count;
        }

        @Override
        public int available() throws IOException {
            return eof ? 0 : Math.max(1, super.available());
        }
    }

    /**
     * Supplies {@link SnappyCodec#INSTANCE} as a supported compression type, so that {@link CompressionCodecs} finds it
     * with {@link java.util.ServiceLoader} like every other type.
     */
    @AutoService(CompressionCodecProvider.class)
    public static final class Provider implements DeephavenCompressionCodecProvider {
        @Override
        public Codec codec() {
            // loads snappy-java's native library now, so that a library that cannot load fails here, where
            // CompressionCodecs leaves snappy out, rather than at the first compressed message
            Snappy.getNativeLibraryVersion();
            return INSTANCE;
        }
    }
}
