//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import elemental2.core.ArrayBufferView;
import elemental2.core.Uint8Array;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageStreamGeneratorImpl;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.SingleElementListHeaderInputStreamGenerator;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.datastructures.SizeException;
import io.deephaven.web.client.api.parse.JsDataHandler;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.web.shared.data.ShiftedRange;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.gwtproject.nio.TypedArrayHelper;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

/**
 * Roughly mirrors BarrageStreamGeneratorImpl, except that viewports are not supported, and the grpc-web TS library is
 * used for FlightData rather than InputStream to send data to the server.
 */
public class WebBarrageStreamGenerator {
    private static final int DEFAULT_INITIAL_BATCH_SIZE = 4096;
    private static final int DEFAULT_BATCH_SIZE = 1 << 16;

    private static final int DEFAULT_MESSAGE_SIZE_LIMIT = 100 * 1024 * 1024;

    public interface View {
        void forEachStream(Consumer<FlightData> visitor) throws IOException;

        default boolean isViewport() {
            return false;
        }

        StreamReaderOptions options();

        int clientMaxMessageSize();

        RangeSet addRowOffsets();

        RangeSet modRowOffsets(int col);
    }
    public class SnapshotView implements View {
        private final BarrageSnapshotOptions options;
        // private final RangeSet viewport;
        // private final boolean reverseViewport;
        private final BitSet subscribedColumns;
        private final long numAddRows;
        private final RangeSet addRowKeys;
        private final RangeSet addRowOffsets;

        public SnapshotView(BarrageSnapshotOptions options,
                @Nullable final BitSet subscribedColumns) {
            this.options = options;

            this.subscribedColumns = subscribedColumns;

            this.addRowKeys = WebBarrageStreamGenerator.this.rowsAdded;
            this.addRowOffsets = RangeSet.ofRange(0, addRowKeys.size() - 1);

            this.numAddRows = addRowOffsets.size();
        }

        @Override
        public void forEachStream(Consumer<FlightData> visitor) {
            ByteBuffer metadata = WebBarrageStreamGenerator.this.getSnapshotMetadata(this);
            AtomicLong bytesWritten = new AtomicLong(0);

            int maxBatchSize = options().batchSize();
            if (maxBatchSize <= 0) {
                maxBatchSize = DEFAULT_BATCH_SIZE;
            }
            AtomicInteger actualBatchSize = new AtomicInteger();
            if (numAddRows == 0) {
                // we still need to send a message containing metadata when there are no rows
                FlightData inputStream = WebBarrageStreamGenerator.this
                        .getInputStream(this, 0, 0, actualBatchSize, metadata,
                                WebBarrageStreamGenerator.this::appendAddColumns);
                visitor.accept(inputStream);
            } else {
                // send the add batches
                WebBarrageStreamGenerator.this.processBatches(visitor, this, numAddRows, maxBatchSize, metadata,
                        WebBarrageStreamGenerator.this::appendAddColumns, bytesWritten);
            }
        }

        @Override
        public StreamReaderOptions options() {
            return options;
        }

        @Override
        public int clientMaxMessageSize() {
            return options.maxMessageSize();
        }

        @Override
        public RangeSet addRowOffsets() {
            return addRowOffsets;
        }

        @Override
        public RangeSet modRowOffsets(int col) {
            throw new UnsupportedOperationException("asked for mod row on SnapshotView");
        }
    }
    public static class SchemaView implements View {
        private final ByteBuffer msgBytes;

        public SchemaView(ByteBuffer msgBytes) {
            this.msgBytes = msgBytes;
        }

        @Override
        public void forEachStream(Consumer<FlightData> visitor) {
            visitor.accept(new BarrageStreamGeneratorImpl.DrainableByteArrayInputStream(msgBytes, 0, msgBytes.l));
            FlightData data = new FlightData();
            ArrayBufferView view = TypedArrayHelper.unwrap(msgBytes);
            new Uint8Array(view.buffer, msgBytes.position(), msgBytes.remaining());
            data.setDataHeader(new Uint8Array(view.buffer, msgBytes.position(), msgBytes.remaining()));
            visitor.accept(data);
        }

        @Override
        public StreamReaderOptions options() {
            return null;
        }

        @Override
        public int clientMaxMessageSize() {
            return 0;
        }

        @Override
        public RangeSet addRowOffsets() {
            return null;
        }

        @Override
        public RangeSet modRowOffsets(int col) {
            return null;
        }
    }

    private interface ColumnVisitor {
        int visit(View view, long startRange, int targetBatchSize, Consumer<DefensiveDrainable> addInputStream,
                ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener,
                ChunkInputStreamGenerator.BufferListener bufferListener) throws IOException;
    }

    public static class ModColumnGenerator {
        final RangeSet rowsModified;
        final BarrageStreamGeneratorImpl.ChunkListInputStreamGenerator data;

        public ModColumnGenerator(RangeSet rowsModified,
                BarrageStreamGeneratorImpl.ChunkListInputStreamGenerator data) {
            this.rowsModified = rowsModified;
            this.data = data;
        }
    }

    public static class Factory {
        public WebBarrageStreamGenerator newGenerator(WebBarrageMessage message) {
            return new WebBarrageStreamGenerator(message);
        }

        public SchemaView getSchemaView(ToIntFunction<FlatBufferBuilder> schemaWriter) {
            final FlatBufferBuilder builder = new FlatBufferBuilder();
            final int schemaOffset = schemaWriter.applyAsInt(builder);
            Message.startMessage(builder);
            Message.addHeaderType(builder, org.apache.arrow.flatbuf.MessageHeader.Schema);
            Message.addHeader(builder, schemaOffset);
            Message.addVersion(builder, MetadataVersion.V5);
            Message.addBodyLength(builder, 0);
            builder.finish(Message.endMessage(builder));
            return new SchemaView(builder.dataBuffer());
        }
    }

    private final WebBarrageMessage message;

    private final RangeSet rowsAdded;
    private final RangeSet rowsRemoved;
    private final ShiftedRange[] shifted;

    private final BarrageStreamGeneratorImpl.ChunkListInputStreamGenerator[] addColumnData;
    private final ModColumnGenerator[] modColumnData;

    public WebBarrageStreamGenerator(WebBarrageMessage message) {
        this.message = message;

        this.rowsAdded = message.rowsAdded;
        this.rowsRemoved = message.rowsRemoved;
        this.shifted = message.shifted;

        addColumnData = new BarrageStreamGeneratorImpl.ChunkListInputStreamGenerator[message.addColumnData.length];
        for (int i = 0; i < message.addColumnData.length; i++) {
            WebBarrageMessage.AddColumnData columnData = message.addColumnData[i];
            addColumnData[i] = new BarrageStreamGeneratorImpl.ChunkListInputStreamGenerator(columnData.chunkType,
                    columnData.type, columnData.componentType, columnData.data);
        }

        modColumnData = new ModColumnGenerator[message.modColumnData.length];
        for (int i = 0; i < modColumnData.length; i++) {
            WebBarrageMessage.ModColumnData columnData = message.modColumnData[i];
            modColumnData[i] = new ModColumnGenerator(columnData.rowsModified,
                    new BarrageStreamGeneratorImpl.ChunkListInputStreamGenerator(columnData.chunkType, columnData.type,
                            columnData.componentType, columnData.data));
        }
    }

    public SnapshotView getSnapshotView(BarrageSnapshotOptions options, BitSet snapshotColumns) {
        return new SnapshotView(options, snapshotColumns);
    }

    private FlightData getInputStream(View view, long offset, int targetBatchSize, AtomicInteger actualBatchSize,
            ByteBuffer metadata, ColumnVisitor columnVisitor) throws IOException {
        List<DefensiveDrainable> streams = new ArrayList<>();
        AtomicInteger size = new AtomicInteger();

        final Consumer<DefensiveDrainable> addStream = (final DefensiveDrainable data) -> {
            try {
                final int sz = data.available();
                if (sz == 0) {
                    data.close();
                    return;
                }

                streams.add(data);
                size.addAndGet(sz);
            } catch (final IOException e) {
                throw new UncheckedDeephavenException("Unexpected IOException", e);
            }
        };


        FlatBufferBuilder header = new FlatBufferBuilder(1024);

        final int numRows;
        final int nodesOffset;
        final int buffersOffset;
        try (SizedChunk<Values> nodeOffsets = new SizedChunk<>(ChunkType.Object);
                SizedLongChunk<Values> bufferInfos = new SizedLongChunk<>()) {
            nodeOffsets.ensureCapacity(addColumnData.length);
            nodeOffsets.get().setSize(0);
            bufferInfos.ensureCapacity(addColumnData.length * 3);
            bufferInfos.get().setSize(0);

            final AtomicLong totalBufferLength = new AtomicLong();
            final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener =
                    (numElements, nullCount) -> {
                        nodeOffsets.ensureCapacityPreserve(nodeOffsets.get().size() + 1);
                        nodeOffsets.get().asWritableObjectChunk()
                                .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount));
                    };

            final ChunkInputStreamGenerator.BufferListener bufferListener = (length) -> {
                totalBufferLength.addAndGet(length);
                bufferInfos.ensureCapacityPreserve(bufferInfos.get().size() + 1);
                bufferInfos.get().add(length);
            };


            numRows = columnVisitor.visit(view, offset, targetBatchSize, addStream, fieldNodeListener, bufferListener);
            actualBatchSize.set(numRows);

            final WritableChunk<Values> noChunk = nodeOffsets.get();
            RecordBatch.startNodesVector(header, noChunk.size());
            for (int i = noChunk.size() - 1; i >= 0; --i) {
                final ChunkInputStreamGenerator.FieldNodeInfo node =
                        (ChunkInputStreamGenerator.FieldNodeInfo) noChunk.asObjectChunk().get(i);
                FieldNode.createFieldNode(header, node.numElements, node.nullCount);
            }
            nodesOffset = header.endVector();

            final WritableLongChunk<Values> biChunk = bufferInfos.get();
            RecordBatch.startBuffersVector(header, biChunk.size());
            for (int i = biChunk.size() - 1; i >= 0; --i) {
                totalBufferLength.addAndGet(-biChunk.get(i));
                Buffer.createBuffer(header, totalBufferLength.longValue(), biChunk.get(i));
            }
            buffersOffset = header.endVector();
        }


        RecordBatch.startRecordBatch(header);
        RecordBatch.addNodes(header, nodesOffset);
        RecordBatch.addBuffers(header, buffersOffset);
        if (view.options().columnsAsList()) {
            RecordBatch.addLength(header, 1);
        } else {
            RecordBatch.addLength(header, numRows);
        }
        final int headerOffset = RecordBatch.endRecordBatch(header);

        header.finish(wrapInMessage(header, headerOffset,
                org.apache.arrow.flatbuf.MessageHeader.RecordBatch, size.intValue()));

//        // now create the proto header
//        try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
//            writeHeader(metadata, size, header, baos);
//            streams.add(0,
//                    new BarrageStreamGeneratorImpl.DrainableByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));
//
//            return new BarrageStreamGeneratorImpl.ConsecutiveDrainableStreams(streams.toArray(new InputStream[0]));
//        } catch (final IOException ex) {
//            throw new UncheckedDeephavenException("Unexpected IOException", ex);
//        }
        FlightData result = new FlightData();
        result.setDataHeader(WebBarrageUtils.bbToUint8ArrayView(header.dataBuffer()));
        return result;
    }

    public static int wrapInMessage(final FlatBufferBuilder builder, final int headerOffset, final byte headerType,
                                    final int bodyLength) {
        Message.startMessage(builder);
        Message.addHeaderType(builder, headerType);
        Message.addHeader(builder, headerOffset);
        Message.addVersion(builder, MetadataVersion.V5);
        Message.addBodyLength(builder, bodyLength);
        return Message.endMessage(builder);
    }

    private ByteBuffer getSnapshotMetadata(SnapshotView view) {
        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int effectiveViewportOffset = 0;
        // if (view.isViewport()) {
        // try (final BarrageStreamGeneratorImpl.RowSetGenerator viewportGen = new
        // BarrageStreamGeneratorImpl.RowSetGenerator(view.viewport)) {
        // effectiveViewportOffset = viewportGen.addToFlatBuffer(metadata);
        // }
        // }

        int effectiveColumnSetOffset = 0;
        if (view.subscribedColumns != null) {
            int nBits = view.subscribedColumns.previousSetBit(Integer.MAX_VALUE - 1) + 1;
            effectiveColumnSetOffset =
                    metadata.createByteVector(view.subscribedColumns.toByteArray(), 0, (int) ((long) nBits + 7) / 8);
        }


        final int rowsAddedOffset = addRangeSetToFlatBuffer(metadata, rowsAdded);

        // no shifts in a snapshot, but need to provide a valid structure
        final int shiftDataOffset = addShiftedToFlatBuffer(metadata, shifted);

        // Added Chunk Data:
        int addedRowsIncludedOffset = 0;
        // don't send `rowsIncluded` when identical to `rowsAdded`, client will infer they are the same
        // addedRowsIncludedOffset = rowsIncluded.addToFlatBuffer(view.addRowKeys, metadata);

        BarrageUpdateMetadata.startBarrageUpdateMetadata(metadata);
        BarrageUpdateMetadata.addIsSnapshot(metadata, true);
        BarrageUpdateMetadata.addFirstSeq(metadata, -1);
        BarrageUpdateMetadata.addLastSeq(metadata, -1);
        BarrageUpdateMetadata.addEffectiveViewport(metadata, effectiveViewportOffset);
        BarrageUpdateMetadata.addEffectiveColumnSet(metadata, effectiveColumnSetOffset);
        BarrageUpdateMetadata.addAddedRows(metadata, rowsAddedOffset);
        BarrageUpdateMetadata.addRemovedRows(metadata, 0);
        BarrageUpdateMetadata.addShiftData(metadata, shiftDataOffset);
        BarrageUpdateMetadata.addAddedRowsIncluded(metadata, addedRowsIncludedOffset);
        BarrageUpdateMetadata.addModColumnNodes(metadata, 0);
        BarrageUpdateMetadata.addEffectiveReverseViewport(metadata, false);
        metadata.finish(BarrageUpdateMetadata.endBarrageUpdateMetadata(metadata));

        final FlatBufferBuilder header = new FlatBufferBuilder();
        final int payloadOffset = BarrageMessageWrapper.createMsgPayloadVector(header, metadata.dataBuffer());
        BarrageMessageWrapper.startBarrageMessageWrapper(header);
        BarrageMessageWrapper.addMagic(header, BarrageUtil.FLATBUFFER_MAGIC);
        BarrageMessageWrapper.addMsgType(header, BarrageMessageType.BarrageUpdateMetadata);
        BarrageMessageWrapper.addMsgPayload(header, payloadOffset);
        header.finish(BarrageMessageWrapper.endBarrageMessageWrapper(header));

        return header.dataBuffer().slice();
    }

    private int addRangeSetToFlatBuffer(FlatBufferBuilder builder, RangeSet rangeSet) {
        return builder.createByteVector(CompressedRangeSetReader.writeRange(rangeSet));
    }

    private int addShiftedToFlatBuffer(FlatBufferBuilder builder, ShiftedRange[] ranges) {
        if (ranges.length > 0) {
            throw new UnsupportedOperationException("addShiftedToFlatBuffer");
        }
        return 0;
    }

    private void processBatches(Consumer<FlightData> visitor, View view, long numRows, int maxBatchSize,
            ByteBuffer metadata, ColumnVisitor columnVisitor, AtomicLong bytesWritten) throws IOException {
        long offset = 0;
        AtomicInteger actualBatchSize = new AtomicInteger();

        int batchSize = Math.min(DEFAULT_INITIAL_BATCH_SIZE, maxBatchSize);

        int maxMessageSize = view.clientMaxMessageSize() > 0 ? view.clientMaxMessageSize() : DEFAULT_MESSAGE_SIZE_LIMIT;

        while (offset < numRows) {
            try {
                FlightData is =
                        getInputStream(view, offset, batchSize, actualBatchSize, metadata, columnVisitor);
                int bytesToWrite = is.getAppMetadata_asU8().length + is.getDataHeader_asU8().length + is.getDataBody_asU8().length;

                if (actualBatchSize.get() == 0) {
                    throw new IllegalStateException("No data was written for a batch");
                }

                // treat this as a hard limit, exceeding fails a client or w2w (unless we are sending a single
                // row then we must send and let it potentially fail)
                if (bytesToWrite < maxMessageSize || batchSize == 1) {
                    // let's write the data
                    visitor.accept(is);

                    bytesWritten.addAndGet(bytesToWrite);
                    offset += actualBatchSize.intValue();
                    metadata = null;
                } // else, can't write this, we'll retry at the same offset as before

                // recompute the batch limit for the next message
                int bytesPerRow = bytesToWrite / actualBatchSize.intValue();
                if (bytesPerRow > 0) {
                    int rowLimit = maxMessageSize / bytesPerRow;

                    // add some margin for abnormal cell contents
                    batchSize = Math.min(maxBatchSize, Math.max(1, (int) ((double) rowLimit * 0.9)));
                }
            } catch (SizeException ex) {
                // was an overflow in the ChunkInputStream generator (probably VarBinary). We can't compute the
                // correct number of rows from this failure, so cut batch size in half and try again. This may
                // occur multiple times until the size is restricted properly
                if (batchSize == 1) {
                    // this row exceeds internal limits and can never be sent
                    throw (new UncheckedDeephavenException(
                            "BarrageStreamGenerator - single row (" + offset + ") exceeds transmissible size", ex));
                }
                final int maximumSize = LongSizedDataStructure.intSize(
                        "BarrageStreamGenerator", ex.getMaximumSize());
                batchSize = maximumSize >= batchSize ? batchSize / 2 : maximumSize;
            }
        }
    }

    private static int findGeneratorForOffset(final ChunkInputStreamGenerator[] generators, final long offset) {
        // fast path for smaller updates
        if (generators.length <= 1) {
            return 0;
        }

        int low = 0;
        int high = generators.length;

        while (low + 1 < high) {
            int mid = (low + high) / 2;
            int cmp = Long.compare(generators[mid].getRowOffset(), offset);

            if (cmp < 0) {
                // the generator's first key is low enough
                low = mid;
            } else if (cmp > 0) {
                // the generator's first key is too high
                high = mid;
            } else {
                // first key matches
                return mid;
            }
        }

        // desired generator is at low as the high is exclusive
        return low;
    }


    private int appendAddColumns(View view, long startRange, int targetBatchSize, Consumer<DefensiveDrainable> addStream,
            ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener,
            ChunkInputStreamGenerator.BufferListener bufferListener) throws IOException {
        if (addColumnData.length == 0) {
            return LongSizedDataStructure.intSize("view.addRowOffsets().size()", view.addRowOffsets().size());
        }

        // find the generator for the initial position-space key
        long startPos = view.addRowOffsets().get(startRange);
        int chunkIdx = findGeneratorForOffset(addColumnData[0].generators, startPos);

        // adjust the batch size if we would cross a chunk boundary
        long shift = 0;
        long endPos = view.addRowOffsets().get(startRange + targetBatchSize - 1);
        if (endPos == RowSet.NULL_ROW_KEY) {
            endPos = Long.MAX_VALUE;
        }
        if (addColumnData[0].generators.length > 0) {
            final ChunkInputStreamGenerator tmpGenerator = addColumnData[0].generators[chunkIdx];
            endPos = Math.min(endPos, tmpGenerator.getLastRowOffset());
            shift = -tmpGenerator.getRowOffset();
        }

        // all column generators have the same boundaries, so we can re-use the offsets internal to this chunkIdx
        final RangeSet allowedRange = RangeSet.ofRange(startPos, endPos);
        final RangeSet myAddedOffsets = view.addRowOffsets().intersect(allowedRange);
        final RangeSet adjustedOffsets = shift == 0 ? null : myAddedOffsets.shift(shift);
        // every column must write to the stream
        for (final BarrageStreamGeneratorImpl.ChunkListInputStreamGenerator data : addColumnData) {
            final int numElements = data.generators.length == 0
                    ? 0
                    : LongSizedDataStructure.intSize("myAddedOffsets.size()", myAddedOffsets.size());
            if (view.options().columnsAsList()) {
                // if we are sending columns as a list, we need to add the list buffers before each column
                final SingleElementListHeaderInputStreamGenerator listHeader =
                        new SingleElementListHeaderInputStreamGenerator(numElements);
                listHeader.visitFieldNodes(fieldNodeListener);
                listHeader.visitBuffers(bufferListener);
                addStream.accept(listHeader);
            }

            if (numElements == 0) {
                // use an empty generator to publish the column data
                try (final RowSet empty = RowSetFactory.empty()) {
                    final ChunkInputStreamGenerator.DrainableColumn drainableColumn =
                            data.emptyGenerator.getInputStream(view.options(), empty);
                    drainableColumn.visitFieldNodes(fieldNodeListener);
                    drainableColumn.visitBuffers(bufferListener);

                    // Add the drainable last as it is allowed to immediately close a row set the visitors need
                    addStream.accept(drainableColumn);
                }
            } else {
                final ChunkInputStreamGenerator generator = data.generators[chunkIdx];
                final ChunkInputStreamGenerator.DrainableColumn drainableColumn =
                        generator.getInputStream(view.options(), shift == 0 ? myAddedOffsets : adjustedOffsets);
                drainableColumn.visitFieldNodes(fieldNodeListener);
                drainableColumn.visitBuffers(bufferListener);
                // Add the drainable last as it is allowed to immediately close a row set the visitors need
                addStream.accept(drainableColumn);
            }
        }
        return LongSizedDataStructure.intSize("myAddedOffsets.size()", myAddedOffsets.size());
    }

    ////// WebBarrageUtil
    public static final BarrageSnapshotOptions DEFAULT_SNAPSHOT_DESER_OPTIONS =
            BarrageSnapshotOptions.builder().build();

    public static void sendSchema(Consumer<InputStream> stream, Map<String, String> columnsAndTypes) {
        Factory streamGeneratorFactory = new Factory();
        streamGeneratorFactory
                .getSchemaView(fbb -> makeTableSchemaPayload(fbb, DEFAULT_SNAPSHOT_DESER_OPTIONS, columnsAndTypes))
                .forEachStream(stream);
    }

    private static int makeTableSchemaPayload(FlatBufferBuilder fbb, BarrageSnapshotOptions options,
            Map<String, String> columnsAndTypes) {
        int[] fields = new int[columnsAndTypes.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : columnsAndTypes.entrySet()) {
            // Unlike BarrageUtil.java, we need to implement this ourselves rather than delegate to Arrow's own types
            String name = entry.getKey();
            String type = entry.getValue();

            // TODO this is wrong for array/vector types
            JsDataHandler writer = JsDataHandler.getHandler(type);
            if (options.columnsAsList()) {
                throw new UnsupportedOperationException("columnsAsList not supported");
            }

            int nameOffset = fbb.createString(name);
            int typeOffset = writer.writeType(fbb);
            int metadataOffset = Field.createCustomMetadataVector(fbb, new int[] {
                    KeyValue.createKeyValue(fbb, fbb.createString("deephaven:type"),
                            fbb.createString(writer.deephavenType()))
            });

            Field.startField(fbb);
            Field.addName(fbb, nameOffset);
            Field.addNullable(fbb, true);

            Field.addTypeType(fbb, writer.typeType());
            Field.addType(fbb, typeOffset);
            Field.addCustomMetadata(fbb, metadataOffset);

            fields[i++] = Field.endField(fbb);
        }

        int fieldsOffset = Schema.createFieldsVector(fbb, fields);

        Schema.startSchema(fbb);
        Schema.addFields(fbb, fieldsOffset);
        return Schema.endSchema(fbb);
    }

    public static void sendSnapshot(Consumer<InputStream> stream, BarrageSnapshotOptions options) {
        WebBarrageMessage msg = constructMessage();
        WebBarrageStreamGenerator bsg = new WebBarrageStreamGenerator(msg);
        bsg.getSnapshotView(options, null).forEachStream(stream);

    }

    private static WebBarrageMessage constructMessage() {
        return new WebBarrageMessage();// TODO need args to create this
    }
}
