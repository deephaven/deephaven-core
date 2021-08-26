/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api_client.util;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.protobuf.CodedInputStream;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.db.v2.utils.ExternalizableIndexUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.io.streams.ByteBufferInputStream;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BarrageProtoUtil {

    public static ByteBuffer toByteBuffer(final Index index) {
        // noinspection UnstableApiUsage
        try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos)) {
            ExternalizableIndexUtils.writeExternalCompressedDeltas(oos, index);
            oos.flush();
            return ByteBuffer.wrap(baos.peekBuffer(), 0, baos.size());
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Unexpected exception during serialization: ", e);
        }
    }

    public static Index toIndex(final ByteBuffer string) {
        // noinspection UnstableApiUsage
        try (final InputStream bais = new ByteBufferInputStream(string);
                final LittleEndianDataInputStream ois = new LittleEndianDataInputStream(bais)) {
            return ExternalizableIndexUtils.readExternalCompressedDelta(ois);
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Unexpected exception during deserialization: ", e);
        }
    }

    public static class ExposedByteArrayOutputStream extends ByteArrayOutputStream {
        public byte[] peekBuffer() {
            return buf;
        }
    }

    public static class ObjectInputStreamAdapter extends InputStream {

        private int sizeRemaining;
        private final CodedInputStream stream;

        public ObjectInputStreamAdapter(final CodedInputStream stream, final int size) {
            if (size < 0) {
                throw new IllegalArgumentException("size cannot be negative");
            }
            this.sizeRemaining = size;
            this.stream = stream;
        }

        @Override
        public int read() throws IOException {
            if (sizeRemaining <= 0) {
                return -1;
            }
            --sizeRemaining;
            final byte r = stream.readRawByte();
            return (r < 0) ? 256 + r : r;
        }

        @Override
        public int read(@NotNull final byte[] b, final int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            if (len < 0) {
                throw new IllegalArgumentException("len should not be less than zero");
            }
            len = Math.min(sizeRemaining, len);
            if (len <= 0) {
                return -1;
            }
            final byte[] arr = stream.readRawBytes(len);
            System.arraycopy(arr, 0, b, off, len);
            sizeRemaining -= len;
            return len;
        }

        @Override
        public long skip(long n) throws IOException {
            n = Math.min(sizeRemaining, n);
            if (n <= 0) {
                return 0;
            }
            final int skipped = (int) Math.min(Integer.MAX_VALUE, n);
            stream.skipRawBytes(skipped);
            sizeRemaining -= skipped;
            return skipped;
        }

        @Override
        public int available() {
            return Math.max(0, sizeRemaining);
        }

        @Override
        public void close() throws IOException {
            stream.skipRawBytes(sizeRemaining);
            sizeRemaining = 0;
        }
    }
}
