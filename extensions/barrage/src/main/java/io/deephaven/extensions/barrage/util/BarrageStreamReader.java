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
import io.deephaven.chunk.ChunkList;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
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

import static io.deephaven.engine.table.impl.sources.InMemoryColumnSource.TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD;

public class BarrageStreamReader implements StreamReader {

    private static final Logger log = LoggerFactory.getLogger(BarrageStreamReader.class);

    private long numAddRowsRead = 0;
    private long numModRowsRead = 0;
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

                        numAddRowsRead = 0;
                        numModRowsRead = 0;
                        numAddBatchesRemaining = metadata.numAddBatches();
                        numModBatchesRemaining = metadata.numModBatches();
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
                            msg.addColumnData[ci].data = new ChunkList();

                            // create an initial chunk of the correct size
                            final int chunkSize = (int)(Math.min(msg.rowsIncluded.size(), TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD));
                            msg.addColumnData[ci].data.addChunk(columnChunkTypes[ci].makeWritableChunk(chunkSize), 0, 0);
                        }

                        // if this message is a snapshot response (vs. subscription) then mod columns may be empty
                        msg.modColumnData = new BarrageMessage.ModColumnData[metadata.modColumnNodesLength()];
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            msg.modColumnData[ci] = new BarrageMessage.ModColumnData();
                            msg.modColumnData[ci].type = columnTypes[ci];
                            msg.modColumnData[ci].componentType = componentTypes[ci];
                            msg.modColumnData[ci].data = new ChunkList();

                            final BarrageModColumnMetadata mcd = metadata.modColumnNodes(ci);
                            msg.modColumnData[ci].rowsModified = extractIndex(mcd.modifiedRowsAsByteBuffer());

                            // create an initial chunk of the correct size
                            final int chunkSize = (int)(Math.min(msg.modColumnData[ci].rowsModified.size(), TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD));
                            msg.modColumnData[ci].data.addChunk(columnChunkTypes[ci].makeWritableChunk(chunkSize), 0, 0);
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
                        msg.addColumnData[ci].data = new ChunkList();

                        // create an initial chunk of the correct size
                        final int chunkSize = (int)(Math.min(msg.rowsIncluded.size(), TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD));
                        msg.addColumnData[ci].data.addChunk(columnChunkTypes[ci].makeWritableChunk(chunkSize), 0, 0);
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
                            final BarrageMessage.AddColumnData acd = msg.addColumnData[ci];

                            // need to add the batch row data to the column chunks
                            WritableChunk<Values> chunk = (WritableChunk)acd.data.lastChunk();
                            int chunkSize = chunk.size();

                            final int chunkOffset;
                            if (acd.data.lastChunkSize() == 0) {
                                chunkOffset = 0;
                            } else {
                                // reading the rows from this might overflow the existing chunk
                                if (acd.data.lastChunkSize() + batch.length() > chunkSize) {
                                    // create a new chunk before trying to write again
                                    chunkSize = (int)(Math.min(msg.rowsIncluded.size() - numAddRowsRead, TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD));
                                    chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                                    acd.data.addChunk(chunk, numAddRowsRead, numAddRowsRead);

                                    chunkOffset = 0;
                                } else {
                                    chunkOffset = (int) (numAddRowsRead - acd.data.lastStartIndex());
                                }
                            }

//                            System.out.println("numAddRowsRead: " + numAddRowsRead + ", chunkSize: " + chunkSize + ", chunkOffset: " + chunkOffset);
                            System.out.println("BSR percentage complete: " + ((double)numAddRowsRead / (double)msg.rowsIncluded.size()) * 100.0);

                            // fill the chunk, but catch overrun exceptions
                            try {
                                chunk = ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                        columnChunkTypes[ci], columnTypes[ci], componentTypes[ci], fieldNodeIter,
                                        bufferInfoIter, ois, chunk,
                                        chunkOffset, chunkSize);

                                // add the batch rows to this chunk rowset
                                acd.data.endIndex.set(acd.data.size() - 1, numAddRowsRead + batch.length() - 1);

                            } catch (Exception ex) {
                                // create a new chunk and write this batch into that chunk
                                chunkSize = (int)(Math.min(msg.rowsIncluded.size() - numAddRowsRead, TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD));
                                chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);

                                acd.data.addChunk(chunk, numAddRowsRead, numAddRowsRead);

                                chunk = ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                        columnChunkTypes[ci], columnTypes[ci], componentTypes[ci], fieldNodeIter,
                                        bufferInfoIter, ois, chunk,
                                        0, chunkSize);

                                // add the batch rows to this chunk rowset
                                acd.data.endIndex.set(acd.data.size() - 1, numAddRowsRead + batch.length() - 1);

                                System.out.println(ex.toString());
                            }
                        }
                        numAddRowsRead += batch.length();
                    } else {
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            final BarrageMessage.ModColumnData mcd = msg.modColumnData[ci];

                            // need to add the batch row data to the column chunks
                            WritableChunk<Values> chunk = (WritableChunk)mcd.data.lastChunk();
                            int chunkSize = chunk.size();

                            final int chunkOffset;
                            if (mcd.data.lastChunkSize() == 0) {
                                chunkOffset = 0;
                            } else {
                                // these row will overflow
                                if (mcd.data.lastChunkSize() + batch.length() > chunkSize) {
                                    // create a new chunk before trying to write again
                                    chunkSize = (int)(Math.min(mcd.rowsModified.size() - numModRowsRead, TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD));
                                    chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);

                                    mcd.data.addChunk(chunk, numModRowsRead, numModRowsRead);

                                    chunkOffset = 0;
                                } else {
                                    chunkOffset = (int) (numModRowsRead - mcd.data.lastStartIndex());
                                }
                            }

                            // fill the chunk, but catch overrun exceptions
                            try {
                                chunk = ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                        columnChunkTypes[ci], columnTypes[ci], componentTypes[ci], fieldNodeIter,
                                        bufferInfoIter, ois, chunk,
                                        chunkOffset, chunkSize);

                                // add the batch rows to this chunk rowset
                                mcd.data.endIndex.set(mcd.data.size() - 1, numModRowsRead + batch.length() - 1);

                            } catch (Exception ex) {
                                // create a new chunk and write this batch into that chunk
                                // create a new chunk before trying to write again
                                chunkSize = (int)(Math.min(mcd.rowsModified.size() - numModRowsRead, TWO_DIMENSIONAL_COLUMN_SOURCE_THRESHOLD));
                                chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);

                                mcd.data.addChunk(chunk, numModRowsRead, numModRowsRead);

                                chunk = ChunkInputStreamGenerator.extractChunkFromInputStream(options,
                                        columnChunkTypes[ci], columnTypes[ci], componentTypes[ci], fieldNodeIter,
                                        bufferInfoIter, ois, chunk,
                                        0, chunkSize);

                                // add the batch rows to this chunk rowset
                                mcd.data.endIndex.set(mcd.data.size() - 1, numModRowsRead + batch.length() - 1);

                                System.out.println(ex.toString());
                            }
                        }
                        numModRowsRead += batch.length();
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
