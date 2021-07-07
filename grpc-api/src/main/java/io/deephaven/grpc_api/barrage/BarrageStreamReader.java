/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.barrage;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.deephaven.barrage.flatbuf.BarrageFieldNode;
import io.deephaven.barrage.flatbuf.BarrageRecordBatch;
import io.deephaven.barrage.flatbuf.Message;
import io.deephaven.barrage.flatbuf.MessageHeader;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.ExternalizableIndexUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.grpc_api_client.util.FlatBufferIteratorAdapter;
import io.deephaven.grpc_api_client.util.GrpcMarshallingException;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.BarrageData;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;

public class BarrageStreamReader implements BarrageMessageConsumer.StreamReader<ChunkInputStreamGenerator.Options> {
    private static final int BODY_TAG =
            makeTag(BarrageData.DATA_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    private static final int DATA_HEADER_TAG =
            makeTag(BarrageData.DATA_HEADER_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);

    private static final int TAG_TYPE_BITS = 3;
    public static int makeTag(final int fieldNumber, final int wireType) {
        return (fieldNumber << TAG_TYPE_BITS) | wireType;
    }

    private static final Logger log = LoggerFactory.getLogger(BarrageStreamReader.class);

    @Override
    public BarrageMessage safelyParseFrom(final ChunkInputStreamGenerator.Options options,
                                          final ChunkType[] columnChunkTypes,
                                          final Class<?>[] columnTypes,
                                          final Class<?>[] componentTypes,
                                          final InputStream stream) {
        Message header = null;
        final BarrageMessage msg = new BarrageMessage();

        try {
            boolean bodyParsed = false;
            final CodedInputStream decoder = CodedInputStream.newInstance(stream);

            for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
                if (tag == DATA_HEADER_TAG) {
                    final int size = decoder.readRawVarint32();
                    header = Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    continue;
                } else if (tag != BODY_TAG) {
                    decoder.skipField(tag);
                    continue;
                }

                if (bodyParsed) {
                    // although not an error for protobuf, we consider it one because:
                    // 1) we control all writers
                    // 2) it's plain ol' inefficient!
                    throw new IllegalStateException("Unexpected duplicate body tag");
                }

                if (header == null) {
                    throw new IllegalStateException("Missing metadata header; cannot decode body");
                }

                if (header.headerType() != BarrageStreamGenerator.BARRAGE_RECORD_BATCH_TYPE_ID) {
                    throw new IllegalStateException("Only know how to decode Schema/BarrageRecordBatch messages");
                }

                final BarrageRecordBatch batch = (BarrageRecordBatch) header.header(new BarrageRecordBatch());
                msg.isSnapshot = batch.isSnapshot();

                bodyParsed = true;
                final int size = decoder.readRawVarint32();
                //noinspection UnstableApiUsage
                try (final LittleEndianDataInputStream ois = new LittleEndianDataInputStream(new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size))) {
                    final MutableInt bufferOffset = new MutableInt();
                    final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                            new FlatBufferIteratorAdapter<>(batch.nodesLength(), i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));
                    final Iterator<ChunkInputStreamGenerator.BufferInfo> bufferInfoIter =
                            new FlatBufferIteratorAdapter<>(batch.buffersLength(), i -> {
                                int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(i).offset());
                                final int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(i).length());
                                final int endOfLastBuffer = bufferOffset.getValue();
                                bufferOffset.setValue(offset + length);
                                offset -= endOfLastBuffer;
                                return new ChunkInputStreamGenerator.BufferInfo(offset, length);
                            });

                    if (msg.isSnapshot) {
                        final ByteBuffer effectiveViewport = batch.effectiveViewportAsByteBuffer();
                        if (effectiveViewport != null) {
                            msg.snapshotIndex = extractIndex(effectiveViewport);
                        }
                        msg.snapshotColumns = extractBitSet(batch.effectiveColumnSetAsByteBuffer());
                    }

                    msg.firstSeq = batch.firstSeq();
                    msg.lastSeq = batch.lastSeq();
                    msg.rowsAdded = extractIndex(batch.addedRowsAsByteBuffer());
                    msg.rowsRemoved = extractIndex(batch.removedRowsAsByteBuffer());
                    msg.shifted = extractIndexShiftData(batch.shiftDataAsByteBuffer());

                    final ByteBuffer rowsIncluded = batch.addedRowsIncludedAsByteBuffer();
                    msg.rowsIncluded = rowsIncluded != null ? extractIndex(rowsIncluded) : msg.rowsAdded.clone();

                    int numRowsAdded = msg.rowsIncluded.intSize();
                    if (numRowsAdded == 0 && msg.isSnapshot) {
                        // We only send the full table index in the initial snapshot. After that it is empty.
                        numRowsAdded = -1;
                    }

                    msg.addColumnData = new BarrageMessage.AddColumnData[columnTypes.length];
                    for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                        final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
                        msg.addColumnData[ci] = acd;

                        acd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(options, columnChunkTypes[ci], columnTypes[ci], fieldNodeIter, bufferInfoIter, ois);
                        if (numRowsAdded == -1 && acd.data.size() != 0) {
                            numRowsAdded = acd.data.size();
                        } else if (acd.data.size() != 0 && acd.data.size() != numRowsAdded) {
                            throw new IllegalStateException("Add column data does not have the expected number of rows.");
                        }
                        acd.type = columnTypes[ci];
                        acd.componentType = componentTypes[ci];
                    }

                    msg.modColumnData = new BarrageMessage.ModColumnData[columnTypes.length];
                    for (int i = 0; i < msg.modColumnData.length; ++i) {
                        final BarrageMessage.ModColumnData mcd = new BarrageMessage.ModColumnData();
                        msg.modColumnData[i] = mcd;

                        final BarrageFieldNode node = batch.nodes(i + columnTypes.length);
                        final ByteBuffer bb = node.modifiedRowsAsByteBuffer();
                        mcd.rowsModified = extractIndex(bb);
                        final ByteBuffer modsIncluded = node.includedRowsAsByteBuffer();
                        mcd.rowsIncluded = modsIncluded != null ? extractIndex(modsIncluded) : mcd.rowsModified.clone();

                        final int numModded = mcd.rowsIncluded.intSize();
                        mcd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(options, columnChunkTypes[i], columnTypes[i], fieldNodeIter, bufferInfoIter, ois);
                        if (mcd.data.size() != numModded) {
                            throw new IllegalStateException("Mod column data does not have the expected number of rows.");
                        }
                        mcd.type = columnTypes[i];
                        mcd.componentType = componentTypes[i];
                    }
                }
            }

            if (header != null && header.headerType() == MessageHeader.Schema) {
                // there is no body and our clients do not want to see schema messages
                return null;
            }

            if (!bodyParsed) {
                throw new IllegalStateException("Missing body tag");
            }

            return msg;
        } catch (final Exception e) {
            log.error().append("Unable to parse a received BarrageMessage: ").append(e).endl();
            throw new GrpcMarshallingException("Unable to parse BarrageMessage object", e);
        }
    }

    private static Index extractIndex(final ByteBuffer bb) throws IOException {
        if (bb == null) {
            return Index.FACTORY.getEmptyIndex();
        }
        //noinspection UnstableApiUsage
        try (final LittleEndianDataInputStream is = new LittleEndianDataInputStream(new ByteBufferBackedInputStream(bb))) {
            return ExternalizableIndexUtils.readExternalCompressedDelta(is);
        }
    }

    private static BitSet extractBitSet(final ByteBuffer bb) {
        return BitSet.valueOf(bb);
    }

    private static IndexShiftData extractIndexShiftData(final ByteBuffer bb) throws IOException {
        final IndexShiftData.Builder builder = new IndexShiftData.Builder();

        final Index sIndex, eIndex, dIndex;
        //noinspection UnstableApiUsage
        try (final LittleEndianDataInputStream is = new LittleEndianDataInputStream(new ByteBufferBackedInputStream(bb))) {
            sIndex = ExternalizableIndexUtils.readExternalCompressedDelta(is);
            eIndex = ExternalizableIndexUtils.readExternalCompressedDelta(is);
            dIndex = ExternalizableIndexUtils.readExternalCompressedDelta(is);
        }

        try (final Index.Iterator sit = sIndex.iterator();
             final Index.Iterator eit = eIndex.iterator();
             final Index.Iterator dit = dIndex.iterator()) {
            while (sit.hasNext()) {
                if (!eit.hasNext() || !dit.hasNext()) {
                    throw new IllegalStateException("IndexShiftData is inconsistent");
                }
                final long next = sit.nextLong();
                builder.shiftRange(next, eit.nextLong(), dit.nextLong() - next);
            }
        }

        return builder.build();
    }
}
