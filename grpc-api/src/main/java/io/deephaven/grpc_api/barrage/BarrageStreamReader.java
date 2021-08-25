/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.barrage;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageModColumnMetadata;
import io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.ExternalizableIndexUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.grpc_api.arrow.ArrowFlightUtil;
import io.deephaven.grpc_api.arrow.FlightServiceGrpcImpl;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.grpc_api_client.util.FlatBufferIteratorAdapter;
import io.deephaven.grpc_api_client.util.GrpcMarshallingException;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;

public class BarrageStreamReader
    implements BarrageMessageConsumer.StreamReader<ChunkInputStreamGenerator.Options> {

    private static final Logger log = LoggerFactory.getLogger(BarrageStreamReader.class);

    private int numAddBatchesRemaining = 0;
    private int numModBatchesRemaining = 0;
    private BarrageMessage msg = null;

    @Override
    public BarrageMessage safelyParseFrom(final ChunkInputStreamGenerator.Options options,
        final ChunkType[] columnChunkTypes,
        final Class<?>[] columnTypes,
        final Class<?>[] componentTypes,
        final InputStream stream) {
        Message header = null;
        try {
            boolean bodyParsed = false;
            final CodedInputStream decoder = CodedInputStream.newInstance(stream);

            for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
                if (tag == ArrowFlightUtil.DATA_HEADER_TAG) {
                    final int size = decoder.readRawVarint32();
                    header = Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    continue;
                } else if (tag == ArrowFlightUtil.APP_METADATA_TAG) {
                    final int size = decoder.readRawVarint32();
                    final ByteBuffer msgAsBB = ByteBuffer.wrap(decoder.readRawBytes(size));
                    final BarrageMessageWrapper wrapper =
                        BarrageMessageWrapper.getRootAsBarrageMessageWrapper(msgAsBB);
                    if (wrapper.magic() != BarrageStreamGenerator.FLATBUFFER_MAGIC) {
                        log.warn().append(
                            "BarrageStreamReader: skipping app_metadata that does not look like BarrageMessageWrapper")
                            .endl();
                    } else if (wrapper.msgType() == BarrageMessageType.BarrageUpdateMetadata) {
                        if (msg != null) {
                            throw new IllegalStateException(
                                "Previous message was not complete; pending "
                                    + numAddBatchesRemaining
                                    + " add batches and " + numModBatchesRemaining
                                    + " mod batches");
                        }

                        final BarrageUpdateMetadata metadata =
                            BarrageUpdateMetadata
                                .getRootAsBarrageUpdateMetadata(wrapper.msgPayloadAsByteBuffer());

                        msg = new BarrageMessage();

                        msg.isSnapshot = metadata.isSnapshot();
                        numAddBatchesRemaining = metadata.numAddBatches();
                        numModBatchesRemaining = metadata.numModBatches();
                        if (numAddBatchesRemaining > 1 || numModBatchesRemaining > 1) {
                            throw new UnsupportedOperationException(
                                "Multiple consecutive add or mod RecordBatches are not yet supported");
                        }
                        if (numAddBatchesRemaining < 0 || numModBatchesRemaining < 0) {
                            throw new IllegalStateException(
                                "Found negative number of record batches in barrage metadata: "
                                    + numAddBatchesRemaining + " add batches and "
                                    + numModBatchesRemaining + " mod batches");
                        }

                        if (msg.isSnapshot) {
                            final ByteBuffer effectiveViewport =
                                metadata.effectiveViewportAsByteBuffer();
                            if (effectiveViewport != null) {
                                msg.snapshotIndex = extractIndex(effectiveViewport);
                            }
                            msg.snapshotColumns =
                                extractBitSet(metadata.effectiveColumnSetAsByteBuffer());
                        }

                        msg.firstSeq = metadata.firstSeq();
                        msg.lastSeq = metadata.lastSeq();
                        msg.rowsAdded = extractIndex(metadata.addedRowsAsByteBuffer());
                        msg.rowsRemoved = extractIndex(metadata.removedRowsAsByteBuffer());
                        msg.shifted = extractIndexShiftData(metadata.shiftDataAsByteBuffer());

                        final ByteBuffer rowsIncluded = metadata.addedRowsIncludedAsByteBuffer();
                        msg.rowsIncluded = rowsIncluded != null ? extractIndex(rowsIncluded)
                            : msg.rowsAdded.clone();
                        msg.addColumnData = new BarrageMessage.AddColumnData[columnTypes.length];
                        for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                            msg.addColumnData[ci] = new BarrageMessage.AddColumnData();
                            msg.addColumnData[ci].type = columnTypes[ci];
                            msg.addColumnData[ci].componentType = componentTypes[ci];
                        }
                        msg.modColumnData = new BarrageMessage.ModColumnData[columnTypes.length];
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            msg.modColumnData[ci] = new BarrageMessage.ModColumnData();
                            msg.modColumnData[ci].type = columnTypes[ci];
                            msg.modColumnData[ci].componentType = componentTypes[ci];

                            final BarrageModColumnMetadata mcd = metadata.nodes(ci);
                            msg.modColumnData[ci].rowsModified =
                                extractIndex(mcd.modifiedRowsAsByteBuffer());
                        }
                    }

                    continue;
                } else if (tag != ArrowFlightUtil.BODY_TAG) {
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

                if (header.headerType() != org.apache.arrow.flatbuf.MessageHeader.RecordBatch) {
                    throw new IllegalStateException(
                        "Only know how to decode Schema/BarrageRecordBatch messages");
                }

                if (msg == null) {
                    throw new IllegalStateException("Could not detect barrage metadata for update");
                }

                bodyParsed = true;
                final int size = decoder.readRawVarint32();
                final RecordBatch batch = (RecordBatch) header.header(new RecordBatch());

                // noinspection UnstableApiUsage
                try (final LittleEndianDataInputStream ois = new LittleEndianDataInputStream(
                    new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size))) {
                    final MutableInt bufferOffset = new MutableInt();
                    final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                        new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                            i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

                    final TLongArrayList bufferInfo = new TLongArrayList(batch.buffersLength());
                    for (int bi = 0; bi < batch.buffersLength(); ++bi) {
                        int offset = LongSizedDataStructure.intSize("BufferInfo",
                            batch.buffers(bi).offset());
                        int length = LongSizedDataStructure.intSize("BufferInfo",
                            batch.buffers(bi).length());
                        if (bi < batch.buffersLength() - 1) {
                            final int nextOffset = LongSizedDataStructure.intSize("BufferInfo",
                                batch.buffers(bi + 1).offset());
                            // our parsers handle overhanging buffers
                            length += Math.max(0, nextOffset - offset - length);
                        }
                        bufferOffset.setValue(offset + length);
                        bufferInfo.add(length);
                    }
                    final TLongIterator bufferInfoIter = bufferInfo.iterator();

                    final boolean isAddBatch = numAddBatchesRemaining > 0;
                    if (isAddBatch) {
                        --numAddBatchesRemaining;
                    } else {
                        --numModBatchesRemaining;
                    }

                    if (isAddBatch) {
                        for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                            msg.addColumnData[ci].data = ChunkInputStreamGenerator
                                .extractChunkFromInputStream(options, columnChunkTypes[ci],
                                    columnTypes[ci], fieldNodeIter, bufferInfoIter, ois);
                        }
                    } else {
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            final BarrageMessage.ModColumnData mcd = msg.modColumnData[ci];
                            final int numModded = mcd.rowsModified.intSize();
                            mcd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(
                                options, columnChunkTypes[ci], columnTypes[ci], fieldNodeIter,
                                bufferInfoIter, ois);
                            if (mcd.data.size() != numModded) {
                                throw new IllegalStateException(
                                    "Mod column data does not have the expected number of rows.");
                            }
                        }
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

            if (numAddBatchesRemaining + numModBatchesRemaining == 0) {
                final BarrageMessage retval = msg;
                msg = null;
                return retval;
            }

            // otherwise, must wait for more data
            return null;
        } catch (final Exception e) {
            log.error().append("Unable to parse a received BarrageMessage: ").append(e).endl();
            throw new GrpcMarshallingException("Unable to parse BarrageMessage object", e);
        }
    }

    private static Index extractIndex(final ByteBuffer bb) throws IOException {
        if (bb == null) {
            return Index.FACTORY.getEmptyIndex();
        }
        // noinspection UnstableApiUsage
        try (final LittleEndianDataInputStream is =
            new LittleEndianDataInputStream(new ByteBufferBackedInputStream(bb))) {
            return ExternalizableIndexUtils.readExternalCompressedDelta(is);
        }
    }

    private static BitSet extractBitSet(final ByteBuffer bb) {
        return BitSet.valueOf(bb);
    }

    private static IndexShiftData extractIndexShiftData(final ByteBuffer bb) throws IOException {
        final IndexShiftData.Builder builder = new IndexShiftData.Builder();

        final Index sIndex, eIndex, dIndex;
        // noinspection UnstableApiUsage
        try (final LittleEndianDataInputStream is =
            new LittleEndianDataInputStream(new ByteBufferBackedInputStream(bb))) {
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
