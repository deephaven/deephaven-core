package io.deephaven.grpc_api.barrage;

import io.deephaven.io.logger.Logger;
import com.google.common.io.LittleEndianDataInputStream;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.WireFormat;
import io.deephaven.db.backplane.CommandMarshallingException;
import io.deephaven.db.backplane.barrage.BarrageMessage;
import io.deephaven.db.backplane.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.db.backplane.util.BarrageProtoUtil;
import io.deephaven.db.util.LongSizedDataStructure;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.ExternalizableIndexUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.proto.backplane.grpc.BarrageData;
import com.fasterxml.jackson.databind.util.ByteBufferBackedInputStream;
import io.deephaven.barrage.flatbuf.BarrageFieldNode;
import io.deephaven.barrage.flatbuf.BarrageRecordBatch;
import io.deephaven.barrage.flatbuf.Buffer;
import io.deephaven.barrage.flatbuf.Message;
import io.deephaven.barrage.flatbuf.MessageHeader;

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
                    final FlatBufferFieldNodeIter fieldNodeIter = new FlatBufferFieldNodeIter(batch);
                    final FlatBufferBufferInfoIter bufferInfoIter = new FlatBufferBufferInfoIter(batch);

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
                    final int numAdded = msg.rowsIncluded.intSize();

                    msg.addColumns = extractBitSet(batch.addedColumnSetAsByteBuffer());
                    msg.addColumnData = new BarrageMessage.AddColumnData[msg.addColumns.cardinality()];
                    final int numAddColumns = msg.addColumns.cardinality();
                    for (int ii = msg.addColumns.nextSetBit(0), jj = 0; ii != -1; ii = msg.addColumns.nextSetBit(ii + 1), ++jj) {
                        final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
                        msg.addColumnData[jj] = acd;

                        acd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(options, columnChunkTypes[ii], columnTypes[ii], fieldNodeIter, bufferInfoIter, ois);
                        if (acd.data.size() != numAdded) {
                            throw new IllegalStateException("Add column data does not have the expected number of rows.");
                        }
                        acd.type = columnTypes[ii];
                    }

                    msg.modColumns = extractBitSet(batch.modifiedColumnSetAsByteBuffer());
                    msg.modColumnData = new BarrageMessage.ModColumnData[msg.modColumns.cardinality()];
                    for (int ii = msg.modColumns.nextSetBit(0), jj = 0; ii != -1; ii = msg.modColumns.nextSetBit(ii + 1), ++jj) {
                        final BarrageMessage.ModColumnData mcd = new BarrageMessage.ModColumnData();
                        msg.modColumnData[jj] = mcd;

                        final BarrageFieldNode node = batch.nodes(jj + numAddColumns);
                        final ByteBuffer bb = node.modifiedRowsAsByteBuffer();
                        mcd.rowsModified = extractIndex(bb);
                        final ByteBuffer modsIncluded = node.includedRowsAsByteBuffer();
                        mcd.rowsIncluded = modsIncluded != null ? extractIndex(modsIncluded) : mcd.rowsModified.clone();

                        final int numModded = mcd.rowsIncluded.intSize();
                        mcd.data = ChunkInputStreamGenerator.extractChunkFromInputStream(options, columnChunkTypes[ii], columnTypes[ii], fieldNodeIter, bufferInfoIter, ois);
                        if (mcd.data.size() != numModded) {
                            throw new IllegalStateException("Mod column data does not have the expected number of rows.");
                        }
                        mcd.type = columnTypes[ii];
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
            throw new CommandMarshallingException("Unable to parse BarrageMessage object", e);
        }
    }

    private Index extractIndex(final ByteBuffer bb) throws IOException {
        if (bb == null) {
            throw new IllegalArgumentException();
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

    private static class FlatBufferFieldNodeIter implements Iterator<ChunkInputStreamGenerator.FieldNodeInfo> {
        private static final String DEBUG_NAME = "FlatBufferFieldNodeIter";

        private final BarrageRecordBatch batch;
        private int fieldNodeOffset = 0;

        public FlatBufferFieldNodeIter(final BarrageRecordBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean hasNext() {
            return fieldNodeOffset < batch.nodesLength();
        }

        @Override
        public ChunkInputStreamGenerator.FieldNodeInfo next() {
            final BarrageFieldNode node = batch.nodes(fieldNodeOffset++);
            return new ChunkInputStreamGenerator.FieldNodeInfo(
                    LongSizedDataStructure.intSize(DEBUG_NAME, node.length()),
                    LongSizedDataStructure.intSize(DEBUG_NAME, node.nullCount()));
        }
    }

    private static class FlatBufferBufferInfoIter implements Iterator<ChunkInputStreamGenerator.BufferInfo> {
        private final BarrageRecordBatch batch;
        private int bufferOffset = 0;

        public FlatBufferBufferInfoIter(final BarrageRecordBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean hasNext() {
            return bufferOffset < batch.buffersLength();
        }

        @Override
        public ChunkInputStreamGenerator.BufferInfo next() {
            final Buffer node = batch.buffers(bufferOffset++);
            return new ChunkInputStreamGenerator.BufferInfo(node.offset(), node.length());
        }
    }
}
