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
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.ChunkListInputStreamGenerator;
import io.deephaven.extensions.barrage.DrainableByteArrayInputStream;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.SingleElementListHeaderInputStreamGenerator;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightData;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.datastructures.SizeException;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.web.client.api.parse.JsDataHandler;
import io.deephaven.web.shared.data.RangeSet;
import io.deephaven.web.shared.data.ShiftedRange;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.Field;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.KeyValue;
import org.apache.arrow.flatbuf.Message;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.MetadataVersion;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.gwtproject.nio.TypedArrayHelper;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static io.deephaven.extensions.barrage.chunk.BaseChunkInputStreamGenerator.PADDING_BUFFER;

public class WebBarrageStreamGeneratorImpl {
    private static final int DEFAULT_INITIAL_BATCH_SIZE = 4096;
    private static final int DEFAULT_BATCH_SIZE = 1 << 16;

    private static final int DEFAULT_MESSAGE_SIZE_LIMIT = 100 * 1024 * 1024;

    private native RowSet wrap(RangeSet rangeSet) /*-{
      return @io.deephaven.engine.rowset.WebRowSetImpl::new(Lio/deephaven/web/shared/data/RangeSet;)(rangeSet);
    }-*/;

    private native RangeSet unwrap(RowSet rowSet) /*-{
      return rowSet.@io.deephaven.engine.rowset.WebRowSetImpl::rangeSet;
    }-*/;

    public interface MessageView {
        List<FlightData> toFlightDataMessage() throws IOException;
    }

    public interface RecordBatchMessageView extends MessageView {
        // void forEachStream(Consumer<DefensiveDrainable> visitor) throws IOException;

        boolean isViewport();

        StreamReaderOptions options();

        RowSet addRowOffsets();

        RowSet modRowOffsets(int col);
    }

    private static final class SchemaMessageView implements MessageView {
        private final FlightData message;

        public SchemaMessageView(final ByteBuffer buffer) {
            ArrayBufferView view = TypedArrayHelper.unwrap(buffer);
            message = new FlightData();
            message.setDataHeader(new Uint8Array(view.buffer, buffer.position(), buffer.remaining()));
        }

        @Override
        public List<FlightData> toFlightDataMessage() throws IOException {
            return Collections.singletonList(message);
        }
    }

    private final class SnapshotView implements RecordBatchMessageView {
        private final BarrageSnapshotOptions options;
        private final long numAddRows;
        private final RowSet addRowKeys;
        private final RowSet addRowOffsets;

        public SnapshotView(final BarrageSnapshotOptions options,
                @Nullable final RowSet viewportIgnored,
                final boolean reverseViewportIgnored,
                @Nullable final Void keyspaceViewportIgnored,
                @Nullable final BitSet subscribedColumnsIgnored) {
            this.options = options;
            // this.viewport = viewportIgnored;
            // this.reverseViewport = reverseViewportIgnored;

            // this.subscribedColumns = subscribedColumnsIgnored;

            // precompute add row offsets (no viewport support)
            addRowKeys = rowsAdded.copy();
            addRowOffsets = RowSetFactory.flat(addRowKeys.size());


            numAddRows = addRowOffsets.size();
        }

        @Override
        public List<FlightData> toFlightDataMessage() throws IOException {
            List<FlightData> messages = new ArrayList<>();
            ByteBuffer metadata = getSnapshotMetadata();
            MutableLong bytesWritten = new MutableLong(0L);

            // batch size is maximum, will write fewer rows when needed
            int maxBatchSize = batchSize();
            final MutableInt actualBatchSize = new MutableInt();
            if (numAddRows == 0) {
                // we still need to send a message containing metadata when there are no rows
                messages.add(getInputStream(this, 0, 0, actualBatchSize, metadata,
                        WebBarrageStreamGeneratorImpl.this::appendAddColumns));
            } else {
                // send the add batches
                processBatches(messages::add, this, numAddRows, maxBatchSize, metadata,
                        WebBarrageStreamGeneratorImpl.this::appendAddColumns, bytesWritten);
            }
            addRowOffsets.close();
            addRowKeys.close();
            // writeConsumer.onWrite(bytesWritten.get(), System.nanoTime() - startTm);
            return messages;
        }

        private int batchSize() {
            int batchSize = options().batchSize();
            if (batchSize <= 0) {
                batchSize = DEFAULT_BATCH_SIZE;
            }
            return batchSize;
        }

        @Override
        public boolean isViewport() {
            return false;
        }

        @Override
        public StreamReaderOptions options() {
            return options;
        }

        @Override
        public RowSet addRowOffsets() {
            return addRowOffsets;
        }

        @Override
        public RowSet modRowOffsets(int col) {
            throw new UnsupportedOperationException("asked for mod row on SnapshotView");
        }

        private ByteBuffer getSnapshotMetadata() throws IOException {
            final FlatBufferBuilder metadata = new FlatBufferBuilder();

            int effectiveViewportOffset = 0;
            if (isViewport()) {
                // try (final RowSetGenerator viewportGen = new RowSetGenerator(viewport)) {
                // effectiveViewportOffset = viewportGen.addToFlatBuffer(metadata);
                // }
            }

            int effectiveColumnSetOffset = 0;
            // if (subscribedColumns != null) {
            // effectiveColumnSetOffset = new BitSetGenerator(subscribedColumns).addToFlatBuffer(metadata);
            // }

            final int rowsAddedOffset = addToFlatBuffer(rowsAdded, metadata);

            // no shifts in a snapshot, but need to provide a valid structure
            final int shiftDataOffset = addToFlatBuffer(shifted, metadata);

            // Added Chunk Data:
            int addedRowsIncludedOffset = 0;
            // don't send `rowsIncluded` when identical to `rowsAdded`, client will infer they are the same
            // if (isSnapshot || !addRowKeys.equals(rowsAdded)) {
            // addedRowsIncludedOffset = addToFlatBuffer(rowsIncluded, addRowKeys, metadata);
            // }

            BarrageUpdateMetadata.startBarrageUpdateMetadata(metadata);
            // BarrageUpdateMetadata.addIsSnapshot(metadata, isSnapshot);
            // BarrageUpdateMetadata.addFirstSeq(metadata, firstSeq);
            // BarrageUpdateMetadata.addLastSeq(metadata, lastSeq);
            BarrageUpdateMetadata.addEffectiveViewport(metadata, effectiveViewportOffset);
            BarrageUpdateMetadata.addEffectiveColumnSet(metadata, effectiveColumnSetOffset);
            BarrageUpdateMetadata.addAddedRows(metadata, rowsAddedOffset);
            BarrageUpdateMetadata.addRemovedRows(metadata, 0);
            BarrageUpdateMetadata.addShiftData(metadata, shiftDataOffset);
            BarrageUpdateMetadata.addAddedRowsIncluded(metadata, addedRowsIncludedOffset);
            BarrageUpdateMetadata.addModColumnNodes(metadata, 0);
            // BarrageUpdateMetadata.addEffectiveReverseViewport(metadata, reverseViewport);
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
    }

    private int addToFlatBuffer(ShiftedRange[] shifted, FlatBufferBuilder metadata) {
        return 0;
    }

    private int addToFlatBuffer(RowSet rowSet, FlatBufferBuilder metadata) {
        RangeSet rangeSet = unwrap(rowSet);
        return metadata.createByteVector(CompressedRangeSetReader.writeRange(rangeSet));
    }

    public static class ModColumnGenerator implements SafeCloseable {
        public final RangeSet rowsModified;
        public final ChunkListInputStreamGenerator data;

        public ModColumnGenerator(final WebBarrageMessage.ModColumnData col) throws IOException {
            rowsModified = col.rowsModified;
            data = new ChunkListInputStreamGenerator(col.type, col.componentType, col.data, col.chunkType);
        }

        @Override
        public void close() {
            data.close();
        }
    }

    static class Factory {
        WebBarrageStreamGeneratorImpl newGenerator(WebBarrageMessage message) throws IOException {
            return new WebBarrageStreamGeneratorImpl(message);
        }

        SchemaMessageView getSchemaView(ToIntFunction<FlatBufferBuilder> schemaWriter) {
            final FlatBufferBuilder builder = new FlatBufferBuilder();
            final int schemaOffset = schemaWriter.applyAsInt(builder);
            Message.startMessage(builder);
            Message.addHeaderType(builder, org.apache.arrow.flatbuf.MessageHeader.Schema);
            Message.addHeader(builder, schemaOffset);
            Message.addVersion(builder, MetadataVersion.V5);
            Message.addBodyLength(builder, 0);
            builder.finish(Message.endMessage(builder));
            return new SchemaMessageView(builder.dataBuffer());
        }
    }

    private final WebBarrageMessage message;

    private final boolean isSnapshot;

    private final RowSet rowsAdded;
    private final RowSet rowsRemoved;
    private final ShiftedRange[] shifted;

    private final ChunkListInputStreamGenerator[] addColumnData;
    private final ModColumnGenerator[] modColumnData;

    public WebBarrageStreamGeneratorImpl(WebBarrageMessage message) throws IOException {
        this.message = message;
        this.isSnapshot = message.isSnapshot;
        assert isSnapshot : "isSnapshot must be true at this time";

        this.rowsAdded = wrap(message.rowsAdded);
        this.rowsRemoved = wrap(message.rowsRemoved);
        this.shifted = message.shifted;

        addColumnData = new ChunkListInputStreamGenerator[message.addColumnData.length];
        for (int i = 0; i < message.addColumnData.length; i++) {
            WebBarrageMessage.AddColumnData columnData = message.addColumnData[i];
            addColumnData[i] = new ChunkListInputStreamGenerator(columnData.type, columnData.componentType,
                    columnData.data, columnData.chunkType);
        }

        modColumnData = new ModColumnGenerator[message.modColumnData.length];
        for (int i = 0; i < modColumnData.length; i++) {
            WebBarrageMessage.ModColumnData columnData = message.modColumnData[i];
            modColumnData[i] = new ModColumnGenerator(columnData);
        }
    }

    /**
     * Obtain a View of this StreamGenerator that can be sent to a single snapshot requestor.
     *
     * @param options serialization options for this specific view
     * @param viewport is the position-space viewport
     * @param reverseViewport is the viewport reversed (relative to end of table instead of beginning)
     * @param keyspaceViewport is the key-space viewport
     * @param snapshotColumns are the columns subscribed for this view
     * @return a MessageView filtered by the snapshot properties that can be sent to that subscriber
     */
    private MessageView getSnapshotView(final BarrageSnapshotOptions options,
            @Nullable final RowSet viewport,
            final boolean reverseViewport,
            @Nullable final RowSet keyspaceViewport,
            @Nullable final BitSet snapshotColumns) {
        return new SnapshotView(options, null, false, null, null);
    }

    /**
     * Obtain a Full-Snapshot View of this StreamGenerator that can be sent to a single snapshot requestor.
     *
     * @param options serialization options for this specific view
     * @return a MessageView filtered by the snapshot properties that can be sent to that subscriber
     */
    public MessageView getSnapshotView(BarrageSnapshotOptions options) {
        return getSnapshotView(options, null, false, null, null);
    }

    @FunctionalInterface
    private interface ColumnVisitor {
        int visit(final RecordBatchMessageView view, final long startRange, final int targetBatchSize,
                final Consumer<DefensiveDrainable> addStream,
                final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener,
                final ChunkInputStreamGenerator.BufferListener bufferListener) throws IOException;
    }

    /**
     * Returns an InputStream of a single FlightData message filtered to the viewport. This function accepts
     * `targetBatchSize` but may actually write fewer rows than the target (when crossing an internal chunk boundary,
     * e.g.)
     *
     * @param view the view of the overall chunk to generate a RecordBatch for
     * @param offset the start of the batch in position space w.r.t. the view (inclusive)
     * @param targetBatchSize the target (and maximum) batch size to use for this message
     * @param actualBatchSize the number of rows actually sent in this batch (will be <= targetBatchSize)
     * @param metadata the optional flight data metadata to attach to the message
     * @param columnVisitor the helper method responsible for appending the payload columns to the RecordBatch
     * @return a single FlightData message
     */
    private FlightData getInputStream(final RecordBatchMessageView view, final long offset,
            final int targetBatchSize,
            final MutableInt actualBatchSize, final ByteBuffer metadata, final ColumnVisitor columnVisitor)
            throws IOException {
        final ArrayDeque<DefensiveDrainable> streams = new ArrayDeque<>();
        final MutableInt size = new MutableInt();

        final Consumer<DefensiveDrainable> addStream = (final DefensiveDrainable is) -> {
            try {
                final int sz = is.available();
                if (sz == 0) {
                    is.close();
                    return;
                }

                streams.add(is);
                size.add(sz);
            } catch (final IOException e) {
                throw new UncheckedDeephavenException("Unexpected IOException", e);
            }

            // These buffers must be aligned to an 8-byte boundary in order for efficient alignment in languages like
            // C++.
            if (size.get() % 8 != 0) {
                final int paddingBytes = (8 - (size.get() % 8));
                size.add(paddingBytes);
                streams.add(new DrainableByteArrayInputStream(PADDING_BUFFER, 0, paddingBytes));
            }
        };

        final FlatBufferBuilder header = new FlatBufferBuilder();

        final int numRows;
        final int nodesOffset;
        final int buffersOffset;
        try (final SizedChunk<Values> nodeOffsets = new SizedChunk<>(ChunkType.Object);
                final SizedLongChunk<Values> bufferInfos = new SizedLongChunk<>()) {
            nodeOffsets.ensureCapacity(addColumnData.length);
            nodeOffsets.get().setSize(0);
            bufferInfos.ensureCapacity(addColumnData.length * 3);
            bufferInfos.get().setSize(0);

            final MutableLong totalBufferLength = new MutableLong();
            final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener =
                    (numElements, nullCount) -> {
                        nodeOffsets.ensureCapacityPreserve(nodeOffsets.get().size() + 1);
                        nodeOffsets.get().asWritableObjectChunk()
                                .add(new ChunkInputStreamGenerator.FieldNodeInfo(numElements, nullCount));
                    };

            final ChunkInputStreamGenerator.BufferListener bufferListener = (length) -> {
                totalBufferLength.add(length);
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
                totalBufferLength.subtract(biChunk.get(i));
                Buffer.createBuffer(header, totalBufferLength.get(), biChunk.get(i));
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

        header.finish(Message.createMessage(header, MetadataVersion.V5, MessageHeader.RecordBatch, headerOffset,
                size.get(), 0));
        // header.finish(wrapInMessage(header, headerOffset,
        // org.apache.arrow.flatbuf.MessageHeader.RecordBatch, size.get()));

        FlightData flightData = new FlightData();
        flightData.setDataHeader(WebBarrageUtils.bbToUint8ArrayView(header.dataBuffer().slice()));
        flightData.setAppMetadata(WebBarrageUtils.bbToUint8ArrayView(metadata));
        int sum = 0;
        for (DefensiveDrainable stream : streams) {
            int available = stream.available();
            sum += available;
        }
        ByteBuffer dataBody = ByteBuffer.allocateDirect(sum);

        // ByteBufferOutputStream outputStream = new ByteBufferOutputStream(dataBody, new NullByteBufferSink());
        for (DefensiveDrainable d : streams) {
            d.drainTo(new OutputStream() {
                @Override
                public void write(int b) throws IOException {
                    dataBody.put((byte) b);
                }
            });
        }
        dataBody.flip();
        flightData.setDataBody(WebBarrageUtils.bbToUint8ArrayView(dataBody));
        return flightData;
    }

    private void processBatches(Consumer<FlightData> visitor, final RecordBatchMessageView view,
            final long numRows, final int maxBatchSize, ByteBuffer metadata,
            final ColumnVisitor columnVisitor, final MutableLong bytesWritten) throws IOException {
        long offset = 0;
        MutableInt actualBatchSize = new MutableInt();

        int batchSize = Math.min(DEFAULT_INITIAL_BATCH_SIZE, maxBatchSize);

        // allow the client to override the default message size
        int clientMaxMessageSize = view.options().maxMessageSize();
        final int maxMessageSize = clientMaxMessageSize > 0 ? clientMaxMessageSize : DEFAULT_MESSAGE_SIZE_LIMIT;

        // TODO (deephaven-core#188): remove this when JS API can accept multiple batches
        boolean sendAllowed = numRows <= batchSize;

        while (offset < numRows) {
            try {
                final FlightData is =
                        getInputStream(view, offset, batchSize, actualBatchSize, metadata, columnVisitor);
                int approxBytesToWrite = is.getAppMetadata().asUint8Array().length
                        + is.getDataHeader().asUint8Array().length + is.getDataBody().asUint8Array().length;// is.available();

                if (actualBatchSize.get() == 0) {
                    throw new IllegalStateException("No data was written for a batch");
                }

                // treat this as a hard limit, exceeding fails a client or w2w (unless we are sending a single
                // row then we must send and let it potentially fail)
                if (sendAllowed && (approxBytesToWrite < maxMessageSize || batchSize == 1)) {
                    // let's write the data
                    visitor.accept(is);

                    bytesWritten.add(approxBytesToWrite);
                    offset += actualBatchSize.get();
                    metadata = null;
                } else {
                    // can't write this, so close the input stream and retry
                    // is.close();
                    sendAllowed = true;
                }

                // recompute the batch limit for the next message
                int bytesPerRow = approxBytesToWrite / actualBatchSize.get();
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

    private static int findGeneratorForOffset(final List<ChunkInputStreamGenerator> generators, final long offset) {
        // fast path for smaller updates
        if (generators.isEmpty()) {
            return 0;
        }

        int low = 0;
        int high = generators.size();

        while (low + 1 < high) {
            int mid = (low + high) / 2;
            int cmp = Long.compare(generators.get(mid).getRowOffset(), offset);

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

    private int appendAddColumns(final RecordBatchMessageView view, final long startRange, final int targetBatchSize,
            final Consumer<DefensiveDrainable> addStream,
            final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener,
            final ChunkInputStreamGenerator.BufferListener bufferListener) throws IOException {
        if (addColumnData.length == 0) {
            return view.addRowOffsets().intSize();
        }

        // find the generator for the initial position-space key
        long startPos = view.addRowOffsets().get(startRange);
        int chunkIdx = findGeneratorForOffset(addColumnData[0].generators(), startPos);

        // adjust the batch size if we would cross a chunk boundary
        long shift = 0;
        long endPos = view.addRowOffsets().get(startRange + targetBatchSize - 1);
        if (endPos == RowSet.NULL_ROW_KEY) {
            endPos = Long.MAX_VALUE;
        }
        if (!addColumnData[0].generators().isEmpty()) {
            final ChunkInputStreamGenerator tmpGenerator = addColumnData[0].generators().get(chunkIdx);
            endPos = Math.min(endPos, tmpGenerator.getLastRowOffset());
            shift = -tmpGenerator.getRowOffset();
        }

        // all column generators have the same boundaries, so we can re-use the offsets internal to this chunkIdx
        try (final RowSet allowedRange = RowSetFactory.fromRange(startPos, endPos);
                final WritableRowSet myAddedOffsets = view.addRowOffsets().intersect(allowedRange);
                final RowSet adjustedOffsets = shift == 0 ? null : myAddedOffsets.shift(shift)) {
            // every column must write to the stream
            for (final ChunkListInputStreamGenerator data : addColumnData) {
                final int numElements = data.generators().isEmpty()
                        ? 0
                        : myAddedOffsets.intSize("BarrageStreamGenerator");
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
                                data.empty(view.options(), empty);
                        drainableColumn.visitFieldNodes(fieldNodeListener);
                        drainableColumn.visitBuffers(bufferListener);

                        // Add the drainable last as it is allowed to immediately close a row set the visitors need
                        addStream.accept(drainableColumn);
                    }
                } else {
                    final ChunkInputStreamGenerator generator = data.generators().get(chunkIdx);
                    final ChunkInputStreamGenerator.DrainableColumn drainableColumn =
                            generator.getInputStream(view.options(), shift == 0 ? myAddedOffsets : adjustedOffsets);
                    drainableColumn.visitFieldNodes(fieldNodeListener);
                    drainableColumn.visitBuffers(bufferListener);
                    // Add the drainable last as it is allowed to immediately close a row set the visitors need
                    addStream.accept(drainableColumn);
                }
            }
            return myAddedOffsets.intSize();
        }
    }



    ////// WebBarrageUtil
    public static final BarrageSnapshotOptions DEFAULT_SNAPSHOT_DESER_OPTIONS =
            BarrageSnapshotOptions.builder().build();

    public static List<FlightData> sendSchema(Map<String, String> columnsAndTypes) throws IOException {
        Factory streamGeneratorFactory = new Factory();
        return streamGeneratorFactory
                .getSchemaView(fbb -> makeTableSchemaPayload(fbb, DEFAULT_SNAPSHOT_DESER_OPTIONS, columnsAndTypes))
                .toFlightDataMessage();
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

    public static void sendSnapshot(Consumer<FlightData> stream, BarrageSnapshotOptions options) throws IOException {
        WebBarrageMessage msg = constructMessage();
        WebBarrageStreamGeneratorImpl bsg = new WebBarrageStreamGeneratorImpl(msg);
        bsg.getSnapshotView(options).toFlightDataMessage().forEach(stream);

    }

    private static WebBarrageMessage constructMessage() {
        return new WebBarrageMessage();// TODO need args to create this
    }
}
