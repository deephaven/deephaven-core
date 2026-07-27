//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.ExternalizableRowSetUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.streams.ByteBufferInputStream;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BarrageProtoUtil {
    private static final int TAG_TYPE_BITS = 3;

    public static final int BODY_TAG =
            (Flight.FlightData.DATA_BODY_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    public static final int DATA_HEADER_TAG =
            (Flight.FlightData.DATA_HEADER_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    public static final int APP_METADATA_TAG =
            (Flight.FlightData.APP_METADATA_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;
    public static final int FLIGHT_DESCRIPTOR_TAG =
            (Flight.FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER << TAG_TYPE_BITS) | WireFormat.WIRETYPE_LENGTH_DELIMITED;

    private static final Logger log = LoggerFactory.getLogger(BarrageProtoUtil.class);

    public static ByteBuffer toByteBuffer(final RowSet rowSet) {
        try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos)) {
            ExternalizableRowSetUtils.writeExternalCompressedDeltas(oos, rowSet);
            oos.flush();
            return ByteBuffer.wrap(baos.peekBuffer(), 0, baos.size());
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Unexpected exception during serialization: ", e);
        }
    }

    public static byte[] toByteArray(final RowSet rowSet) {
        final ByteBuffer bb = toByteBuffer(rowSet);
        final byte[] array = new byte[bb.remaining()];
        bb.get(array);
        return array;
    }

    public static RowSet toRowSet(final ByteBuffer string) {
        try (final InputStream bais = new ByteBufferInputStream(string);
                final LittleEndianDataInputStream ois = new LittleEndianDataInputStream(bais)) {
            return ExternalizableRowSetUtils.readExternalCompressedDelta(ois);
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Unexpected exception during deserialization: ", e);
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

        /**
         * Reads {@code len} bytes from the underlying {@link CodedInputStream} and returns them directly, avoiding the
         * intermediate copy that {@link #read(byte[], int, int)} performs. The consumed bytes are counted against the
         * remaining body size so that {@link #close()} still skips the correct number of trailing bytes. The returned
         * array is owned by the caller.
         */
        public byte[] readRawBytes(final int len) throws IOException {
            if (len < 0) {
                throw new IllegalArgumentException("len should not be less than zero");
            }
            if (sizeRemaining < len) {
                throw new EOFException();
            }
            final byte[] result = stream.readRawBytes(len);
            sizeRemaining -= len;
            return result;
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

    /**
     * A little-endian {@link DataInput} over an Arrow record-batch body. Every {@link DataInput} operation is delegated
     * to a Guava {@link LittleEndianDataInputStream}, preserving existing behavior for all chunk readers. In addition,
     * {@link #readRawBytes(int)} returns a run of payload bytes directly from the underlying {@link CodedInputStream},
     * letting readers of fixed-width types bulk-read a window of values and decode them in place rather than reading
     * one value at a time.
     */
    public static final class BarrageDataInputStream implements DataInput, Closeable {
        private final ObjectInputStreamAdapter adapter;
        private final LittleEndianDataInputStream delegate;

        public BarrageDataInputStream(final ObjectInputStreamAdapter adapter) {
            this.adapter = adapter;
            this.delegate = new LittleEndianDataInputStream(adapter);
        }

        /**
         * @return the next {@code len} bytes, returned directly without an intermediate copy
         */
        public byte[] readRawBytes(final int len) throws IOException {
            return adapter.readRawBytes(len);
        }

        @Override
        public void readFully(final byte[] b) throws IOException {
            delegate.readFully(b);
        }

        @Override
        public void readFully(final byte[] b, final int off, final int len) throws IOException {
            delegate.readFully(b, off, len);
        }

        @Override
        public int skipBytes(final int n) throws IOException {
            return delegate.skipBytes(n);
        }

        @Override
        public boolean readBoolean() throws IOException {
            return delegate.readBoolean();
        }

        @Override
        public byte readByte() throws IOException {
            return delegate.readByte();
        }

        @Override
        public int readUnsignedByte() throws IOException {
            return delegate.readUnsignedByte();
        }

        @Override
        public short readShort() throws IOException {
            return delegate.readShort();
        }

        @Override
        public int readUnsignedShort() throws IOException {
            return delegate.readUnsignedShort();
        }

        @Override
        public char readChar() throws IOException {
            return delegate.readChar();
        }

        @Override
        public int readInt() throws IOException {
            return delegate.readInt();
        }

        @Override
        public long readLong() throws IOException {
            return delegate.readLong();
        }

        @Override
        public float readFloat() throws IOException {
            return delegate.readFloat();
        }

        @Override
        public double readDouble() throws IOException {
            return delegate.readDouble();
        }

        @Override
        public String readLine() throws IOException {
            return delegate.readLine();
        }

        @Override
        public String readUTF() throws IOException {
            return delegate.readUTF();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    public static final class MessageInfo {
        /** outer-most Arrow Flight Message that indicates the msg type (i.e. schema, record batch, etc) */
        public Message header = null;
        /** the embedded flatbuffer metadata indicating information about this batch */
        public BarrageMessageWrapper app_metadata = null;
        /** the parsed protobuf from the flight descriptor embedded in app_metadata */
        public Flight.FlightDescriptor descriptor = null;
        /** the payload beyond the header metadata */
        public LittleEndianDataInputStream inputStream = null;
    }

    public static MessageInfo parseProtoMessage(final InputStream stream) throws IOException {
        final MessageInfo mi = new MessageInfo();

        final CodedInputStream decoder = CodedInputStream.newInstance(stream);

        // if we find a body tag we stop iterating through the loop as there should be no more tags after the body
        // and we lazily drain the payload from the decoder (so the next bytes are payload and not a tag)
        decodeLoop: for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
            final int size;
            switch (tag) {
                case DATA_HEADER_TAG:
                    size = decoder.readRawVarint32();
                    mi.header = Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    break;
                case APP_METADATA_TAG:
                    size = decoder.readRawVarint32();
                    mi.app_metadata = BarrageMessageWrapper
                            .getRootAsBarrageMessageWrapper(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    if (mi.app_metadata.magic() != BarrageUtil.FLATBUFFER_MAGIC) {
                        log.error().append("received invalid magic").endl();
                        mi.app_metadata = null;
                    }
                    break;
                case FLIGHT_DESCRIPTOR_TAG:
                    size = decoder.readRawVarint32();
                    final byte[] bytes = decoder.readRawBytes(size);
                    mi.descriptor = Flight.FlightDescriptor.parseFrom(bytes);
                    break;
                case BODY_TAG:
                    // at this point, we're in the body, we will read it and then break, the rest of the payload should
                    // be the body
                    size = decoder.readRawVarint32();
                    mi.inputStream = new LittleEndianDataInputStream(
                            new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size));
                    // we do not actually remove the content from our stream; prevent reading the next tag via a labeled
                    // break
                    break decodeLoop;

                default:
                    log.info().append("Skipping tag: ").append(tag).endl();
                    decoder.skipField(tag);
            }
        }

        if (mi.header != null && mi.header.headerType() == MessageHeader.RecordBatch && mi.inputStream == null) {
            mi.inputStream =
                    new LittleEndianDataInputStream(new ByteArrayInputStream(ArrayTypeUtils.EMPTY_BYTE_ARRAY));
        }

        return mi;
    }
}
