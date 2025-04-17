//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageModColumnMetadata;
import io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.impl.ExternalizableRowSetUtils;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.extensions.barrage.BarrageOptions;
import io.deephaven.extensions.barrage.BarrageTypeInfo;
import io.deephaven.extensions.barrage.chunk.ChunkWriter;
import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.DefaultChunkReaderFactory;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.chunk.ChunkType;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.function.LongConsumer;

public class BarrageMessageReaderImpl implements BarrageMessageReader {

    private static final Logger log = LoggerFactory.getLogger(BarrageMessageReaderImpl.class);

    // We would like to use jdk.internal.util.ArraysSupport.MAX_ARRAY_LENGTH, but it is not exported
    private static final int MAX_CHUNK_SIZE = ArrayUtil.MAX_ARRAY_SIZE;

    private volatile LongConsumer deserializeTmConsumer;

    private long numAddRowsRead = 0;
    private long numAddRowsTotal = 0;
    private long numModRowsRead = 0;
    private long numModRowsTotal = 0;

    private BarrageMessage msg = null;

    private final ChunkReader.Factory chunkReaderFactory = DefaultChunkReaderFactory.INSTANCE;
    private final List<ChunkReader<?>> readers = new ArrayList<>();

    public BarrageMessageReaderImpl() {
        this(tm -> {
        });
    }

    public BarrageMessageReaderImpl(final LongConsumer deserializeTmConsumer) {
        this.deserializeTmConsumer = deserializeTmConsumer;
    }

    public void setDeserializeTmConsumer(final LongConsumer deserializeTmConsumer) {
        this.deserializeTmConsumer = deserializeTmConsumer;
    }

    @Override
    public BarrageMessage safelyParseFrom(final BarrageOptions options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final InputStream stream) {
        final long startDeserTm = System.nanoTime();
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
                                    "Previous message was not complete; pending " + (numAddRowsTotal - numAddRowsRead)
                                            + " add rows and " + (numModRowsTotal - numModRowsRead) + " mod rows");
                        }

                        final BarrageUpdateMetadata metadata =
                                BarrageUpdateMetadata.getRootAsBarrageUpdateMetadata(wrapper.msgPayloadAsByteBuffer());

                        msg = new BarrageMessage();

                        msg.isSnapshot = metadata.isSnapshot();
                        msg.snapshotRowSetIsReversed = metadata.effectiveReverseViewport();

                        numAddRowsRead = 0;
                        numModRowsRead = 0;

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
                        msg.tableSize = metadata.tableSize();
                        msg.rowsAdded = extractIndex(metadata.addedRowsAsByteBuffer());
                        msg.rowsRemoved = extractIndex(metadata.removedRowsAsByteBuffer());
                        final ByteBuffer shiftData = metadata.shiftDataAsByteBuffer();
                        msg.shifted = shiftData != null ? extractIndexShiftData(shiftData) : RowSetShiftData.EMPTY;

                        final ByteBuffer rowsIncluded = metadata.addedRowsIncludedAsByteBuffer();
                        msg.rowsIncluded = rowsIncluded != null ? extractIndex(rowsIncluded) : msg.rowsAdded.copy();
                        msg.addColumnData = new BarrageMessage.AddColumnData[columnTypes.length];
                        for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                            msg.addColumnData[ci] = new BarrageMessage.AddColumnData();
                            msg.addColumnData[ci].type = columnTypes[ci];
                            msg.addColumnData[ci].componentType = componentTypes[ci];
                            msg.addColumnData[ci].data = new ArrayList<>();

                            if (options.columnsAsList()) {
                                // we can't accumulate multiple batches into the same chunk; leave the list empty
                                continue;
                            }

                            // create an initial chunk of the correct size
                            final int chunkSize = (int) (Math.min(msg.rowsIncluded.size(), MAX_CHUNK_SIZE));
                            final WritableChunk<Values> chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                            chunk.setSize(0);
                            msg.addColumnData[ci].data.add(chunk);
                        }
                        if (options.columnsAsList() && msg.addColumnData.length == 0) {
                            // there will be no more incoming record batches if there are no columns
                            numAddRowsTotal = 0;
                        } else {
                            numAddRowsTotal = msg.rowsIncluded.size();
                        }

                        // if this message is a snapshot response (vs. subscription) then mod columns may be empty
                        numModRowsTotal = 0;
                        msg.modColumnData = new BarrageMessage.ModColumnData[metadata.modColumnNodesLength()];
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            msg.modColumnData[ci] = new BarrageMessage.ModColumnData();
                            msg.modColumnData[ci].type = columnTypes[ci];
                            msg.modColumnData[ci].componentType = componentTypes[ci];
                            msg.modColumnData[ci].data = new ArrayList<>();

                            final BarrageModColumnMetadata mcd = metadata.modColumnNodes(ci);
                            msg.modColumnData[ci].rowsModified = extractIndex(mcd.modifiedRowsAsByteBuffer());
                            numModRowsTotal = Math.max(numModRowsTotal, msg.modColumnData[ci].rowsModified.size());

                            if (options.columnsAsList()) {
                                // we can't accumulate multiple batches into the same chunk; leave the list empty
                                continue;
                            }

                            // create an initial chunk of the correct size
                            final int chunkSize = (int) (Math.min(msg.modColumnData[ci].rowsModified.size(),
                                    MAX_CHUNK_SIZE));
                            final WritableChunk<Values> chunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                            chunk.setSize(0);
                            msg.modColumnData[ci].data.add(chunk);
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

                // throw an error when no app metadata (snapshots now provide by default)
                if (msg == null) {
                    throw new IllegalStateException(
                            "Missing app metadata tag; cannot decode using BarrageStreamReader");
                }

                bodyParsed = true;
                final int size = decoder.readRawVarint32();
                final RecordBatch batch = (RecordBatch) header.header(new RecordBatch());
                msg.length = batch.length();

                try (final LittleEndianDataInputStream ois =
                        new LittleEndianDataInputStream(new BarrageProtoUtil.ObjectInputStreamAdapter(decoder, size))) {
                    final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter =
                            new FlatBufferIteratorAdapter<>(batch.nodesLength(),
                                    i -> new ChunkWriter.FieldNodeInfo(batch.nodes(i)));

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

                    // add and mod rows are never combined in a batch. all added rows must be received before the first
                    // mod rows will be received.
                    if (numAddRowsRead < numAddRowsTotal) {
                        for (int ci = 0; ci < msg.addColumnData.length; ++ci) {
                            final BarrageMessage.AddColumnData acd = msg.addColumnData[ci];

                            if (options.columnsAsList()) {
                                // we cannot aggregate multiple batches into the same chunk since we're not actually
                                // sure how many rows are in the batch without reading the offsets of the list payload
                                final WritableChunk<Values> outChunk = readers.get(ci).readChunk(
                                        fieldNodeIter, bufferInfoIter, ois, null, 0, 0);
                                acd.data.add(outChunk);
                                continue;
                            }

                            final long remaining = numAddRowsTotal - numAddRowsRead;
                            if (batch.length() > remaining) {
                                throw new IllegalStateException(
                                        "Batch length exceeded the expected number of rows from app metadata");
                            }

                            // need to append the batch row data to the column chunks
                            int lastChunkIndex = acd.data.size() - 1;
                            WritableChunk<Values> inChunk = (WritableChunk<Values>) acd.data.get(lastChunkIndex);

                            if (batch.length() > inChunk.capacity() - inChunk.size()) {
                                // reading the rows from this batch will overflow the existing chunk; create a new one
                                final int chunkSize = (int) (Math.min(remaining, MAX_CHUNK_SIZE));
                                inChunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                                acd.data.add(inChunk);

                                inChunk.setSize(0);
                                ++lastChunkIndex;
                            }

                            // fill the chunk with data and assign back into the array
                            final WritableChunk<Values> outChunk = readers.get(ci).readChunk(
                                    fieldNodeIter, bufferInfoIter, ois, inChunk, inChunk.size(), (int) batch.length());
                            Assert.eq(outChunk, "outChunk", inChunk, "inChunk");
                        }

                        if (options.columnsAsList() && msg.addColumnData.length > 0) {
                            final List<Chunk<Values>> chunks = msg.addColumnData[0].data;
                            numAddRowsRead += chunks.get(chunks.size() - 1).size();
                        } else {
                            numAddRowsRead += batch.length();
                        }
                    } else {
                        int maxModRows = 0;
                        for (int ci = 0; ci < msg.modColumnData.length; ++ci) {
                            final BarrageMessage.ModColumnData mcd = msg.modColumnData[ci];

                            if (options.columnsAsList()) {
                                // we cannot aggregate multiple batches into the same chunk since we're not actually
                                // sure how many rows are in the batch without reading the offsets of the list payload
                                final WritableChunk<Values> outChunk = readers.get(ci).readChunk(
                                        fieldNodeIter, bufferInfoIter, ois, null, 0, 0);
                                mcd.data.add(outChunk);
                                maxModRows = Math.max(outChunk.size(), maxModRows);
                                continue;
                            }

                            // another column may be larger than this column
                            long remaining = Math.max(0, mcd.rowsModified.size() - numModRowsRead);

                            // need to append the batch row data to the column chunks
                            int lastChunkIndex = mcd.data.size() - 1;
                            WritableChunk<Values> inChunk = (WritableChunk<Values>) mcd.data.get(lastChunkIndex);

                            final int numRowsToRead = LongSizedDataStructure.intSize("BarrageStreamReader",
                                    Math.min(remaining, batch.length()));
                            maxModRows = Math.max(numRowsToRead, maxModRows);
                            if (numRowsToRead > inChunk.capacity() - inChunk.size()) {
                                // reading the rows from this batch will overflow the existing chunk; create a new one
                                final int chunkSize = (int) (Math.min(remaining, MAX_CHUNK_SIZE));
                                inChunk = columnChunkTypes[ci].makeWritableChunk(chunkSize);
                                mcd.data.add(inChunk);

                                inChunk.setSize(0);
                                ++lastChunkIndex;
                            }

                            // fill the chunk with data and assign back into the array
                            final WritableChunk<Values> outChunk = readers.get(ci).readChunk(
                                    fieldNodeIter, bufferInfoIter, ois, inChunk, inChunk.size(), numRowsToRead);
                            Assert.eq(outChunk, "outChunk", inChunk, "inChunk");
                        }
                        numModRowsRead += maxModRows;
                    }
                }
            }

            if (header != null && header.headerType() == MessageHeader.Schema) {
                // there is no body and our clients do not want to see schema messages, consume the schema so that we
                // can read the following messages and return null.
                ByteBuffer original = header.getByteBuffer();
                ByteBuffer copy = ByteBuffer.allocate(original.remaining()).put(original).rewind();
                Schema schema = new Schema();
                Message.getRootAsMessage(copy).header(schema);
                header.header(schema);
                for (int i = 0; i < schema.fieldsLength(); i++) {
                    Field field = schema.fields(i);

                    final Class<?> columnType = ReinterpretUtils.maybeConvertToPrimitiveDataType(columnTypes[i]);
                    readers.add(chunkReaderFactory.newReader(
                            BarrageTypeInfo.make(columnType, componentTypes[i], field), options));
                }
                return null;
            }

            if (!bodyParsed) {
                throw new IllegalStateException("Missing body tag");
            }

            deserializeTmConsumer.accept(System.nanoTime() - startDeserTm);
            if (numAddRowsRead == numAddRowsTotal && numModRowsRead == numModRowsTotal) {
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
