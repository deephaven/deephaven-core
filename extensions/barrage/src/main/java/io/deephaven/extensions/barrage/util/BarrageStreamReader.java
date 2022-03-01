/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.extensions.barrage.util;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageModColumnMetadata;
import io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.ExternalizableRowSetUtils;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ChunkType;
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

public class BarrageStreamReader implements StreamReader {

    private static final Logger log = LoggerFactory.getLogger(BarrageStreamReader.class);

    private int numAddBatchesRemaining = 0;
    private int numModBatchesRemaining = 0;
    private BarrageMessage msg = null;

    @Override
    public BarrageMessage safelyParseFrom(final StreamReaderOptions options,
            final BitSet expectedColumns,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final InputStream stream) {
        Message header = null;
        try {
            boolean bodyParsed = false;

            final CodedInputStream decoder = CodedInputStream.newInstance(stream);

            for (int tag = decoder.readTag(); tag != 0; tag = decoder.readTag()) {
                if (tag == BarrageProtoUtil.DATA_HEADER_TAG) {
                    final int size = decoder.readRawVarint32();
                    header = Message.getRootAsMessage(ByteBuffer.wrap(decoder.readRawBytes(size)));
                    continue;
                } else if (tag == BarrageProtoUtil.APP_METADATA_TAG) {
                    final int size = decoder.readRawVarint32();
                    final ByteBuffer msgAsBB = ByteBuffer.wrap(decoder.readRawBytes(size));
                    final BarrageMessageWrapper wrapper = BarrageMessageWrapper.getRootAsBarrageMessageWrapper(msgAsBB);
                    if (wrapper.magic() != BarrageUtil.FLATBUFFER_MAGIC) {
                        log.warn().append(
                                "BarrageStreamReader: skipping app_metadata that does not look like BarrageMessageWrapper")
                                .endl();
                    } else if (wrapper.msgType() == BarrageMessageType.BarrageUpdateMetadata) {
                        if (msg != null) {
                            throw new IllegalStateException(
                                    "Previous message was not complete; pending " + numAddBatchesRemaining
                                            + " add batches and " + numModBatchesRemaining + " mod batches");
                        }

                        final BarrageUpdateMetadata metadata =
                                BarrageUpdateMetadata.getRootAsBarrageUpdateMetadata(wrapper.msgPayloadAsByteBuffer());

                        msg = new BarrageMessage();

                        msg.isSnapshot = metadata.isSnapshot();
                        msg.snapshotRowSetIsReversed = metadata.effectiveReverseViewport();

                        numAddBatchesRemaining = metadata.numAddBatches();
                        numModBatchesRemaining = metadata.numModBatches();
                        if (numAddBatchesRemaining > 1 || numModBatchesRemaining > 1) {
                            throw new UnsupportedOperationException(
                                    "Multiple consecutive add or mod RecordBatches are not yet supported");
                        }
                        if (numAddBatchesRemaining < 0 || numModBatchesRemaining < 0) {
                            throw new IllegalStateException(
                                    "Found negative number of record batches in barrage metadata: "
                                            + numAddBatchesRemaining + " add batches and " + numModBatchesRemaining
                                            + " mod batches");
                        }

                        if (msg.isSnapshot) {
                            final ByteBuffer effectiveViewport = metadata.effectiveViewportAsByteBuffer();
                            if (effectiveViewport != null) {
                                msg.snapshotRowSet = extractIndex(effectiveViewport);
                            }
                            final ByteBuffer effectiveSnapshotColumns = metadata.effectiveColumnSetAsByteBuffer();
                            if (effectiveSnapshotColumns != null) {
                                msg.snapshotColumns = extractBitSet(effectiveSnapshotColumns);
                            }
                        }

                        msg.firstSeq = metadata.firstSeq();
                        msg.lastSeq = metadata.lastSeq();
                        msg.rowsAdded = extractIndex(metadata.addedRowsAsByteBuffer());
                        msg.rowsRemoved = extractIndex(metadata.removedRowsAsByteBuffer());
                        msg.shifted = extractIndexShiftData(metadata.shiftDataAsByteBuffer());

                        final ByteBuffer rowsIncluded = metadata.addedRowsIncludedAsByteBuffer();
                        msg.rowsIncluded = rowsIncluded != null ? extractIndex(rowsIncluded) : msg.rowsAdded.copy();
                        msg.addColumnData = new BarrageMessage.AddColumnData[columnTypes.length];
                        for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                            msg.addColumnData[ci] = new BarrageMessage.AddColumnData();
                            msg.addColumnData[ci].type = columnTypes[ci];
                            msg.addColumnData[ci].componentType = componentTypes[ci];
                        }

                        // if this message is a snapshot response (vs. subscription) then mod columns may be empty
                        msg.modColumnData = new BarrageMessage.ModColumnData[metadata.modColumnNodesLength()];
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            msg.modColumnData[ci] = new BarrageMessage.ModColumnData();
                            msg.modColumnData[ci].type = columnTypes[ci];
                            msg.modColumnData[ci].componentType = componentTypes[ci];

                            final BarrageModColumnMetadata mcd = metadata.modColumnNodes(ci);
                            msg.modColumnData[ci].rowsModified = extractIndex(mcd.modifiedRowsAsByteBuffer());
                        }
                    }

                    continue;
                } else if (tag != BarrageProtoUtil.BODY_TAG) {
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
                    throw new IllegalStateException("Only know how to decode Schema/BarrageRecordBatch messages");
                }

                // snapshots may not provide metadata, generate it now
                if (msg == null) {
                    msg = new BarrageMessage();

                    // generate a default set of column selectors
                    msg.snapshotColumns = expectedColumns;

                    // create and fill the add column metadata from the schema
                    msg.addColumnData = new BarrageMessage.AddColumnData[columnTypes.length];
                    for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                        msg.addColumnData[ci] = new BarrageMessage.AddColumnData();
                        msg.addColumnData[ci].type = columnTypes[ci];
                        msg.addColumnData[ci].componentType = componentTypes[ci];
                    }

                    // no mod column data
                    msg.modColumnData = new BarrageMessage.ModColumnData[0];

                    // generate empty row sets
                    msg.rowsRemoved = RowSetFactory.empty();
                    msg.shifted = RowSetShiftData.EMPTY;

                    msg.isSnapshot = true;
                    numAddBatchesRemaining = 1;
                }

                bodyParsed = true;
                final int size = decoder.readRawVarint32();
                final RecordBatch batch = (RecordBatch) header.header(new RecordBatch());
                msg.length = batch.length();

                // noinspection UnstableApiUsage
                try (final LittleEndianDataInputStream ois =
                        new LittleEndianDataInputStream(new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size))) {
                    final MutableInt bufferOffset = new MutableInt();
                    final Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter =
                            new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                                    i -> new ChunkInputStreamGenerator.FieldNodeInfo(batch.nodes(i)));

                    final TLongArrayList bufferInfo = new TLongArrayList(batch.buffersLength());
                    for (int bi = 0; bi < batch.buffersLength(); ++bi) {
                        int offset = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).offset());
                        int length = LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi).length());
                        if (bi < batch.buffersLength() - 1) {
                            final int nextOffset =
                                    LongSizedDataStructure.intSize("BufferInfo", batch.buffers(bi + 1).offset());
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
                            msg.addColumnData[ci].data = ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                    columnChunkTypes[ci], columnTypes[ci], fieldNodeIter, bufferInfoIter, ois);
                        }
                    } else {
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            final BarrageMessage.ModColumnData mcd = msg.modColumnData[ci];
                            final int numModded = mcd.rowsModified.intSize();
                            mcd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                    columnChunkTypes[ci], columnTypes[ci], fieldNodeIter, bufferInfoIter, ois);
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

    private static RowSet extractIndex(final ByteBuffer bb) throws IOException {
        if (bb == null) {
            return RowSetFactory.empty();
        }
        // noinspection UnstableApiUsage
        try (final LittleEndianDataInputStream is =
                new LittleEndianDataInputStream(new ByteBufferBackedInputStream(bb))) {
            return ExternalizableRowSetUtils.readExternalCompressedDelta(is);
        }
    }

    private static BitSet extractBitSet(final ByteBuffer bb) {
        return BitSet.valueOf(bb);
    }

    private static RowSetShiftData extractIndexShiftData(final ByteBuffer bb) throws IOException {
        final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();

        final RowSet sRowSet, eRowSet, dRowSet;
        // noinspection UnstableApiUsage
        try (final LittleEndianDataInputStream is =
                new LittleEndianDataInputStream(new ByteBufferBackedInputStream(bb))) {
            sRowSet = ExternalizableRowSetUtils.readExternalCompressedDelta(is);
            eRowSet = ExternalizableRowSetUtils.readExternalCompressedDelta(is);
            dRowSet = ExternalizableRowSetUtils.readExternalCompressedDelta(is);
        }

        try (final RowSet.Iterator sit = sRowSet.iterator();
                final RowSet.Iterator eit = eRowSet.iterator();
                final RowSet.Iterator dit = dRowSet.iterator()) {
            while (sit.hasNext()) {
                if (!eit.hasNext() || !dit.hasNext()) {
                    throw new IllegalStateException("RowSetShiftData is inconsistent");
                }
                final long next = sit.nextLong();
                builder.shiftRange(next, eit.nextLong(), dit.nextLong() - next);
            }
        }

        return builder.build();
    }
}
