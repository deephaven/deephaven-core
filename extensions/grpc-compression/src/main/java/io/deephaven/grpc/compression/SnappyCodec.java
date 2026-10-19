//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import io.grpc.Codec;
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
        return new AvailableUntilEof(new SnappyFramedInputStream(is));
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
}
