//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ChunkType;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.PrimitiveIterator;

/**
 * This class is used to append the results of a DoGet directly into destination {@link WritableChunk<Values>}.
 * <p>
 * It will append the results of a DoGet into the destination chunks, and notify the listener of the number of rows
 * appended to the record batch in total. The user will typically want to wait for OnCompletion to be called before
 * assuming they have received all the data.
 */
public class BarrageChunkAppendingMarshaller implements MethodDescriptor.Marshaller<Integer> {

    /**
     * Fetch the client side descriptor for a specific DoGet invocation.
     * <p>
     * Instead of providing BarrageMessage as the response type, this custom marshaller will return the number of rows
     * appended after each RecordBatch. This is informative yet hands-off process reading data into the chunks.
     *
     * @param columnChunkTypes the chunk types per column
     * @param columnTypes the class type per column
     * @param componentTypes the component class type per column
     * @param destChunks the destination chunks
     * @return the client side method descriptor
     */
    public static MethodDescriptor<Flight.Ticket, Integer> getClientDoGetDescriptor(
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final WritableChunk<Values>[] destChunks) {
        final MethodDescriptor.Marshaller<Flight.Ticket> requestMarshaller =
                ProtoUtils.marshaller(Flight.Ticket.getDefaultInstance());
        final MethodDescriptor<?, ?> descriptor = FlightServiceGrpc.getDoGetMethod();

        return MethodDescriptor.<Flight.Ticket, Integer>newBuilder()
                .setType(MethodDescriptor.MethodType.SERVER_STREAMING)
                .setFullMethodName(descriptor.getFullMethodName())
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(new BarrageChunkAppendingMarshaller(
                        BARRAGE_OPTIONS, columnChunkTypes, columnTypes, componentTypes, destChunks))
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build();
    }

    // DoGet does not get to set any options
    private static final BarrageSnapshotOptions BARRAGE_OPTIONS = BarrageSnapshotOptions.builder().build();

    private static final Logger log = LoggerFactory.getLogger(BarrageChunkAppendingMarshaller.class);

    private final BarrageSnapshotOptions options;

    private final ChunkType[] columnChunkTypes;
    private final Class<?>[] columnTypes;
    private final Class<?>[] componentTypes;

    private final WritableChunk<Values>[] destChunks;
    private long numRowsRead = 0;

    public BarrageChunkAppendingMarshaller(
            final BarrageSnapshotOptions options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final WritableChunk<Values>[] destChunks) {
        this.options = options;
        this.columnChunkTypes = columnChunkTypes;
        this.columnTypes = columnTypes;
        this.componentTypes = componentTypes;
        this.destChunks = destChunks;
    }

    @Override
    public InputStream stream(final Integer value) {
        throw new UnsupportedOperationException(
                "BarrageDataMarshaller unexpectedly used to directly convert BarrageMessage to InputStream");
    }

    @Override
    public Integer parse(final InputStream stream) {
        Message header = null;
        try {
            boolean bodyParsed = false;

            final CodedInputStream decoder = CodedInputStream.newInstance(stream);

            for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
                if (tag == BarrageProtoUtil.DATA_HEADER_TAG) {
                    final int size = decoder.readRawVarint32();
                    header = Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    continue;
                } else if (tag != BarrageProtoUtil.BODY_TAG) {
                    decoder.skipField(tag);
                    continue;
                }

                if (bodyParsed) {
                    // although not an error for protobuf, arrow payloads should consider it one
                    throw new IllegalStateException("Unexpected duplicate body tag");
                }

                if (header == null) {
                    throw new IllegalStateException("Missing metadata header; cannot decode body");
                }

                if (header.headerType() != org.apache.arrow.flatbuf.MessageHeader.RecordBatch) {
                    throw new IllegalStateException("Only know how to decode Schema/BarrageRecordBatch messages");
                }

                bodyParsed = true;
                final int size = decoder.readRawVarint32();
                final RecordBatch batch = (RecordBatch) header.header(new RecordBatch());

                // noinspection UnstableApiUsage
                try (final LittleEndianDataInputStream ois =
                        new LittleEndianDataInputStream(new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size))) {
                    final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                            new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                                    i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

                    final long[] bufferInfo = new long[batch.buffersLength()];
                    for (int bi = 0; bi < batch.buffersLength(); ++bi) {
                        int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).offset());
                        int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).length());
                        if (bi < batch.buffersLength() - 1) {
                            final int nextOffset =
                                    LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi + 1).offset());
                            // our parsers handle overhanging buffers
                            length += Math.max(0, nextOffset - offset - length);
                        }
                        bufferInfo[bi] = length;
                    }
                    final PrimitiveIterator.OfLong bufferInfoIter = Arrays.stream(bufferInfo).iterator();

                    for (int ci = 0; ci < destChunks.length; ++ci) {
                        final WritableChunk<Values> dest = destChunks[ci];

                        final long remaining = dest.capacity() - dest.size();
                        if (batch.length() > remaining) {
                            throw new BarrageMarshallingException(String.format("Received RecordBatch length (%d) " +
                                    "exceeds the remaining capacity (%d) of the destination Chunk.", batch.length(),
                                    remaining));
                        }

                        // Barrage should return the provided chunk since there was enough room to append the data
                        final WritableChunk<Values> retChunk = ChunkInputStreamGenerator.extractChunkFromInputStream(
                                options, columnChunkTypes[ci], columnTypes[ci], componentTypes[ci], fieldNodeIter,
                                bufferInfoIter, ois, dest, dest.size(), (int) batch.length());

                        if (retChunk != dest) {
                            throw new BarrageMarshallingException("Unexpected chunk returned from " +
                                    "ChunkInputStreamGenerator.extractChunkFromInputStream");
                        }

                        // barrage does not alter the destination chunk size, so let's set it ourselves
                        dest.setSize(dest.size() + (int) batch.length());
                    }
                    numRowsRead += batch.length();
                }
            }

            if (header != null && header.headerType() == MessageHeader.Schema) {
                // getting started, but no rows yet; schemas do not have body tags
                return 0;
            }

            if (!bodyParsed) {
                throw new IllegalStateException("Missing body tag");
            }

            // we're appending directly to the chunk, but courteously let our user know how many rows were read
            return (int) numRowsRead;
        } catch (final Exception e) {
            log.error().append("Unable to parse a received DoGet: ").append(e).endl();
            if (e instanceof BarrageMarshallingException) {
                throw (BarrageMarshallingException) e;
            }
            throw new GrpcMarshallingException("Unable to parse DoGet", e);
        }
    }
}
