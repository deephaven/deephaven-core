/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.util;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.common.io.LittleEndianDataOutputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.rowset.impl.ExternalizableRowSetUtils;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.streams.ByteBufferInputStream;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

public class BarrageProtoUtil {
    public static final BarrageSubscriptionOptions DEFAULT_SER_OPTIONS =
            BarrageSubscriptionOptions.builder().build();
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
        // noinspection UnstableApiUsage
        try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos)) {
            ExternalizableRowSetUtils.writeExternalCompressedDeltas(oos, rowSet);
            oos.flush();
            return ByteBuffer.wrap(baos.peekBuffer(), 0, baos.size());
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Unexpected exception during serialization: ", e);
        }
    }

    public static RowSet toRowSet(final ByteBuffer string) {
        // noinspection UnstableApiUsage
        try (final InputStream bais = new ByteBufferInputStream(string);
                final LittleEndianDataInputStream ois = new LittleEndianDataInputStream(bais)) {
            return ExternalizableRowSetUtils.readExternalCompressedDelta(ois);
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

    public static final class MessageInfo {
        /** outer-most Arrow Flight Message that indicates the msg type (i.e. schema, record batch, etc) */
        public Message header = null;
        /** the embedded flatbuffer metadata indicating information about this batch */
        public BarrageMessageWrapper app_metadata = null;
        /** the parsed protobuf from the flight descriptor embedded in app_metadata */
        public Flight.FlightDescriptor descriptor = null;
        /** the payload beyond the header metadata */
        @SuppressWarnings("UnstableApiUsage")
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
                    // noinspection UnstableApiUsage
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
            // noinspection UnstableApiUsage
            mi.inputStream =
                    new LittleEndianDataInputStream(new ByteArrayInputStream(CollectionUtil.ZERO_LENGTH_BYTE_ARRAY));
        }

        return mi;
    }
}
