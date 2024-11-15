//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import com.google.common.io.LittleEndianDataOutputStream;
import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import gnu.trove.list.array.TIntArrayList;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageModColumnMetadata;
import io.deephaven.barrage.flatbuf.BarrageUpdateMetadata;
import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.impl.ExternalizableRowSetUtils;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.chunk.DefaultChunkInputStreamGeneratorFactory;
import io.deephaven.extensions.barrage.chunk.SingleElementListHeaderInputStreamGenerator;
import io.deephaven.extensions.barrage.util.ExposedByteArrayOutputStream;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.DefensiveDrainable;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.flight.util.MessageHelper;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.deephaven.util.datastructures.SizeException;
import io.deephaven.util.mutable.MutableInt;
import io.deephaven.util.mutable.MutableLong;
import org.apache.arrow.flatbuf.Buffer;
import org.apache.arrow.flatbuf.FieldNode;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.ToIntFunction;

import static io.deephaven.extensions.barrage.chunk.BaseChunkInputStreamGenerator.PADDING_BUFFER;
import static io.deephaven.proto.flight.util.MessageHelper.toIpcBytes;

public class BarrageStreamGeneratorImpl implements BarrageStreamGenerator {
    private static final Logger log = LoggerFactory.getLogger(BarrageStreamGeneratorImpl.class);

    // NB: This should likely be something smaller, such as 1<<16, but since the js api is not yet able
    // to receive multiple record batches we crank this up to MAX_INT.
    private static final int DEFAULT_BATCH_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(BarrageStreamGeneratorImpl.class, "batchSize", Integer.MAX_VALUE);

    // defaults to a small value that is likely to succeed and provide data for following batches
    private static final int DEFAULT_INITIAL_BATCH_SIZE = Configuration.getInstance()
            .getIntegerForClassWithDefault(BarrageStreamGeneratorImpl.class, "initialBatchSize", 4096);

    // default to 100MB to match 100MB java-client and w2w default incoming limits
    private static final int DEFAULT_MESSAGE_SIZE_LIMIT = Configuration.getInstance()
            .getIntegerForClassWithDefault(BarrageStreamGeneratorImpl.class, "maxOutboundMessageSize",
                    100 * 1024 * 1024);

    public interface RecordBatchMessageView extends MessageView {
        StreamReaderOptions options();

        RowSet addRowOffsets();

        RowSet modRowOffsets(int col);
    }

    public static class Factory implements BarrageStreamGenerator.Factory {
        @Override
        public BarrageStreamGenerator newGenerator(
                final BarrageMessage message, final BarragePerformanceLog.WriteMetricsConsumer metricsConsumer) {
            return new BarrageStreamGeneratorImpl(message, metricsConsumer);
        }

        @Override
        public MessageView getSchemaView(@NotNull final ToIntFunction<FlatBufferBuilder> schemaPayloadWriter) {
            final FlatBufferBuilder builder = new FlatBufferBuilder();
            final int schemaOffset = schemaPayloadWriter.applyAsInt(builder);
            builder.finish(MessageHelper.wrapInMessage(builder, schemaOffset,
                    org.apache.arrow.flatbuf.MessageHeader.Schema));
            return new SchemaMessageView(builder.dataBuffer());
        }
    }

    /**
     * This factory writes data in Arrow's IPC format which has a terse header and no room for metadata.
     */
    public static class ArrowFactory extends Factory {
        @Override
        public BarrageStreamGenerator newGenerator(
                BarrageMessage message, BarragePerformanceLog.WriteMetricsConsumer metricsConsumer) {
            return new BarrageStreamGeneratorImpl(message, metricsConsumer) {
                @Override
                protected void writeHeader(
                        ByteBuffer metadata,
                        MutableInt size,
                        FlatBufferBuilder header,
                        ExposedByteArrayOutputStream baos) throws IOException {
                    baos.write(toIpcBytes(header));
                }
            };
        }
    }

    public static class ModColumnGenerator implements SafeCloseable {
        private final RowSetGenerator rowsModified;
        private final ChunkListInputStreamGenerator data;

        ModColumnGenerator(ChunkInputStreamGenerator.Factory factory, final BarrageMessage.ModColumnData col)
                throws IOException {
            rowsModified = new RowSetGenerator(col.rowsModified);
            data = new ChunkListInputStreamGenerator(factory, col.type, col.componentType, col.data, col.chunkType);
        }

        @Override
        public void close() {
            rowsModified.close();
            data.close();
        }
    }

    private final BarrageMessage message;
    private final BarragePerformanceLog.WriteMetricsConsumer writeConsumer;

    private final long firstSeq;
    private final long lastSeq;

    private final boolean isSnapshot;

    private final RowSetGenerator rowsAdded;
    private final RowSetGenerator rowsIncluded;
    private final RowSetGenerator rowsRemoved;
    private final RowSetShiftDataGenerator shifted;

    private final ChunkListInputStreamGenerator[] addColumnData;
    private final ModColumnGenerator[] modColumnData;

    /**
     * Create a barrage stream generator that can slice and dice the barrage message for delivery to clients.
     *
     * @param message the generator takes ownership of the message and its internal objects
     * @param writeConsumer a method that can be used to record write time
     */
    public BarrageStreamGeneratorImpl(final BarrageMessage message,
            final BarragePerformanceLog.WriteMetricsConsumer writeConsumer) {
        this.message = message;
        this.writeConsumer = writeConsumer;
        try {
            firstSeq = message.firstSeq;
            lastSeq = message.lastSeq;
            isSnapshot = message.isSnapshot;

            rowsAdded = new RowSetGenerator(message.rowsAdded);
            rowsIncluded = new RowSetGenerator(message.rowsIncluded);
            rowsRemoved = new RowSetGenerator(message.rowsRemoved);
            shifted = new RowSetShiftDataGenerator(message.shifted);

            addColumnData = new ChunkListInputStreamGenerator[message.addColumnData.length];
            for (int i = 0; i < message.addColumnData.length; ++i) {
                BarrageMessage.AddColumnData columnData = message.addColumnData[i];
                // noinspection resource
                addColumnData[i] = new ChunkListInputStreamGenerator(DefaultChunkInputStreamGeneratorFactory.INSTANCE,
                        columnData.type, columnData.componentType,
                        columnData.data, columnData.chunkType);
            }

            modColumnData = new ModColumnGenerator[message.modColumnData.length];
            for (int i = 0; i < modColumnData.length; ++i) {
                // noinspection resource
                modColumnData[i] = new ModColumnGenerator(DefaultChunkInputStreamGeneratorFactory.INSTANCE,
                        message.modColumnData[i]);
            }
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("unexpected IOException while creating barrage message stream", e);
        } finally {
            if (message.snapshotRowSet != null) {
                message.snapshotRowSet.close();
            }
        }
    }

    @Override
    public BarrageMessage getMessage() {
        return message;
    }

    @Override
    public void close() {
        rowsAdded.close();
        rowsIncluded.close();
        rowsRemoved.close();

        if (addColumnData != null) {
            SafeCloseable.closeAll(addColumnData);
        }
        if (modColumnData != null) {
            SafeCloseable.closeAll(modColumnData);
        }
    }

    @Override
    public MessageView getSubView(
            final BarrageSubscriptionOptions options,
            final boolean isInitialSnapshot,
            final boolean isFullSubscription,
            @Nullable final RowSet viewport,
            final boolean reverseViewport,
            @Nullable final RowSet keyspaceViewportPrev,
            @Nullable final RowSet keyspaceViewport,
            @Nullable final BitSet subscribedColumns) {
        return new SubView(options, isInitialSnapshot, isFullSubscription, viewport, reverseViewport,
                keyspaceViewportPrev, keyspaceViewport, subscribedColumns);
    }

    @Override
    public MessageView getSubView(BarrageSubscriptionOptions options, boolean isInitialSnapshot) {
        return getSubView(options, isInitialSnapshot, true, null, false, null, null, null);
    }

    private final class SubView implements RecordBatchMessageView {
        private final BarrageSubscriptionOptions options;
        private final boolean isInitialSnapshot;
        private final boolean isFullSubscription;
        private final boolean reverseViewport;
        private final boolean hasViewport;
        private final BitSet subscribedColumns;

        private final long numClientIncludedRows;
        private final long numClientModRows;
        private final WritableRowSet clientViewport;
        private final WritableRowSet clientIncludedRows;
        private final WritableRowSet clientIncludedRowOffsets;
        private final WritableRowSet[] clientModdedRows;
        private final WritableRowSet[] clientModdedRowOffsets;
        private final WritableRowSet clientRemovedRows;

        public SubView(final BarrageSubscriptionOptions options,
                final boolean isInitialSnapshot,
                final boolean isFullSubscription,
                @Nullable final RowSet viewport,
                final boolean reverseViewport,
                @Nullable final RowSet keyspaceViewportPrev,
                @Nullable final RowSet keyspaceViewport,
                @Nullable final BitSet subscribedColumns) {
            this.options = options;
            this.isInitialSnapshot = isInitialSnapshot;
            this.isFullSubscription = isFullSubscription;
            this.clientViewport = viewport == null ? null : viewport.copy();
            this.reverseViewport = reverseViewport;
            this.hasViewport = keyspaceViewport != null;
            this.subscribedColumns = subscribedColumns;

            // precompute the included rows / offsets and viewport removed rows
            if (isFullSubscription) {
                clientRemovedRows = null; // we'll send full subscriptions the full removed set

                if (keyspaceViewport != null) {
                    // growing viewport clients need to know about all rows, including those that were scoped into view
                    clientIncludedRows = keyspaceViewport.intersect(rowsIncluded.original);
                    clientIncludedRowOffsets = rowsIncluded.original.invert(clientIncludedRows);
                } else if (!rowsAdded.original.equals(rowsIncluded.original)) {
                    // there are scoped rows that need to be removed from the data sent to the client
                    clientIncludedRows = rowsAdded.original.copy();
                    clientIncludedRowOffsets = rowsIncluded.original.invert(clientIncludedRows);
                } else {
                    clientIncludedRows = rowsAdded.original.copy();
                    clientIncludedRowOffsets = RowSetFactory.flat(rowsAdded.original.size());
                }
            } else {
                Assert.neqNull(keyspaceViewportPrev, "keyspaceViewportPrev");
                try (final SafeCloseableList toClose = new SafeCloseableList()) {
                    final WritableRowSet keyspaceClientIncludedRows =
                            toClose.add(keyspaceViewport.intersect(rowsIncluded.original));
                    // all included rows are sent to viewport clients as adds (already includes repainted rows)
                    clientIncludedRows = keyspaceViewport.invert(keyspaceClientIncludedRows);
                    clientIncludedRowOffsets = rowsIncluded.original.invert(keyspaceClientIncludedRows);

                    // A row may slide out of the viewport and back into the viewport within the same coalesced message.
                    // The coalesced adds/removes will not contain this row, but the server has recorded it as needing
                    // to be sent to the client in its entirety. The client will process this row as both removed and
                    // added.
                    final WritableRowSet keyspacePrevClientRepaintedRows =
                            toClose.add(keyspaceClientIncludedRows.copy());
                    if (!isSnapshot) {
                        // note that snapshot rowsAdded contain all rows; we "repaint" only rows shared between prev and
                        // new viewports.
                        keyspacePrevClientRepaintedRows.remove(rowsAdded.original);
                        shifted.original.unapply(keyspacePrevClientRepaintedRows);
                    }
                    keyspacePrevClientRepaintedRows.retain(keyspaceViewportPrev);

                    // any pre-existing rows that are no longer in the viewport also need to be removed
                    final WritableRowSet rowsToRetain;
                    if (isSnapshot) {
                        // for a snapshot, the goal is to calculate which rows to remove due to viewport changes
                        rowsToRetain = toClose.add(keyspaceViewport.copy());
                    } else {
                        rowsToRetain = toClose.add(keyspaceViewport.minus(rowsAdded.original));
                        shifted.original.unapply(rowsToRetain);
                    }
                    final WritableRowSet noLongerExistingRows = toClose.add(keyspaceViewportPrev.minus(rowsToRetain));
                    noLongerExistingRows.insert(keyspacePrevClientRepaintedRows);
                    clientRemovedRows = keyspaceViewportPrev.invert(noLongerExistingRows);
                }
            }
            numClientIncludedRows = clientIncludedRowOffsets.size();

            // precompute the modified column indexes, and calculate total rows needed
            if (keyspaceViewport != null) {
                clientModdedRows = new WritableRowSet[modColumnData.length];
                clientModdedRowOffsets = new WritableRowSet[modColumnData.length];
            } else {
                clientModdedRows = null;
                clientModdedRowOffsets = null;
            }

            long numModRows = 0;
            for (int ii = 0; ii < modColumnData.length; ++ii) {
                final ModColumnGenerator mcd = modColumnData[ii];

                if (keyspaceViewport == null) {
                    numModRows = Math.max(numModRows, mcd.rowsModified.original.size());
                    continue;
                }

                try (final WritableRowSet intersect = keyspaceViewport.intersect(mcd.rowsModified.original)) {
                    // some rows may be marked both as included and modified; viewport clients must be sent
                    // the full row data for these rows, so we do not also need to send them as modified
                    intersect.remove(rowsIncluded.original);
                    if (isFullSubscription) {
                        clientModdedRows[ii] = intersect.copy();
                    } else {
                        clientModdedRows[ii] = keyspaceViewport.invert(intersect);
                    }
                    clientModdedRowOffsets[ii] = mcd.rowsModified.original.invert(intersect);
                    numModRows = Math.max(numModRows, intersect.size());
                }
            }
            numClientModRows = numModRows;
        }

        @Override
        public void forEachStream(Consumer<DefensiveDrainable> visitor) throws IOException {
            final long startTm = System.nanoTime();
            ByteBuffer metadata = getSubscriptionMetadata();
            MutableLong bytesWritten = new MutableLong(0L);

            // batch size is maximum, will write fewer rows when needed
            int maxBatchSize = batchSize();

            final MutableInt actualBatchSize = new MutableInt();

            if (numClientIncludedRows == 0 && numClientModRows == 0) {
                // we still need to send a message containing metadata when there are no rows
                final DefensiveDrainable is = getInputStream(this, 0, 0, actualBatchSize, metadata,
                        BarrageStreamGeneratorImpl.this::appendAddColumns);
                bytesWritten.add(is.available());
                visitor.accept(is);
                writeConsumer.onWrite(bytesWritten.get(), System.nanoTime() - startTm);
                return;
            }

            // send the add batches (if any)
            try {
                processBatches(visitor, this, numClientIncludedRows, maxBatchSize, metadata,
                        BarrageStreamGeneratorImpl.this::appendAddColumns, bytesWritten);

                // send the mod batches (if any) but don't send metadata twice
                processBatches(visitor, this, numClientModRows, maxBatchSize,
                        numClientIncludedRows > 0 ? null : metadata,
                        BarrageStreamGeneratorImpl.this::appendModColumns, bytesWritten);
            } finally {
                SafeCloseable.closeAll(clientViewport, clientIncludedRows, clientIncludedRowOffsets, clientRemovedRows);
                if (clientModdedRowOffsets != null) {
                    SafeCloseable.closeAll(clientModdedRows);
                    SafeCloseable.closeAll(clientModdedRowOffsets);
                }
            }
            writeConsumer.onWrite(bytesWritten.get(), System.nanoTime() - startTm);
        }

        private int batchSize() {
            int batchSize = options().batchSize();
            if (batchSize <= 0) {
                batchSize = DEFAULT_BATCH_SIZE;
            }
            return batchSize;
        }

        @Override
        public StreamReaderOptions options() {
            return options;
        }

        @Override
        public RowSet addRowOffsets() {
            return clientIncludedRowOffsets;
        }

        @Override
        public RowSet modRowOffsets(int col) {
            if (clientModdedRowOffsets == null) {
                return null;
            }
            return clientModdedRowOffsets[col];
        }

        private ByteBuffer getSubscriptionMetadata() throws IOException {
            final FlatBufferBuilder metadata = new FlatBufferBuilder();

            int effectiveViewportOffset = 0;
            if (isSnapshot && clientViewport != null) {
                try (final RowSetGenerator viewportGen = new RowSetGenerator(clientViewport)) {
                    effectiveViewportOffset = viewportGen.addToFlatBuffer(metadata);
                }
            }

            int effectiveColumnSetOffset = 0;
            if (isSnapshot && subscribedColumns != null) {
                effectiveColumnSetOffset = new BitSetGenerator(subscribedColumns).addToFlatBuffer(metadata);
            }

            final int rowsAddedOffset;
            if (!isFullSubscription) {
                // viewport clients consider all included rows as added; scoped rows will also appear in the removed set
                try (final RowSetGenerator clientIncludedRowsGen = new RowSetGenerator(clientIncludedRows)) {
                    rowsAddedOffset = clientIncludedRowsGen.addToFlatBuffer(metadata);
                }
            } else if (isSnapshot && !isInitialSnapshot) {
                // Growing viewport clients don't need/want to receive the full RowSet on every snapshot
                rowsAddedOffset = EmptyRowSetGenerator.INSTANCE.addToFlatBuffer(metadata);
            } else {
                rowsAddedOffset = rowsAdded.addToFlatBuffer(metadata);
            }

            final int rowsRemovedOffset;
            if (!isFullSubscription) {
                // viewport clients need to also remove rows that were scoped out of view; computed in the constructor
                try (final RowSetGenerator clientRemovedRowsGen = new RowSetGenerator(clientRemovedRows)) {
                    rowsRemovedOffset = clientRemovedRowsGen.addToFlatBuffer(metadata);
                }
            } else {
                rowsRemovedOffset = rowsRemoved.addToFlatBuffer(metadata);
            }

            final int shiftDataOffset;
            if (!isFullSubscription) {
                // we only send shifts to full table subscriptions
                shiftDataOffset = 0;
            } else {
                shiftDataOffset = shifted.addToFlatBuffer(metadata);
            }

            // Added Chunk Data:
            int addedRowsIncludedOffset = 0;

            // don't send `rowsIncluded` to viewport clients or if identical to `rowsAdded`
            if (isFullSubscription && (isSnapshot || !clientIncludedRows.equals(rowsAdded.original))) {
                addedRowsIncludedOffset = rowsIncluded.addToFlatBuffer(clientIncludedRows, metadata);
            }

            // now add mod-column streams, and write the mod column indexes
            TIntArrayList modOffsets = new TIntArrayList(modColumnData.length);
            for (int ii = 0; ii < modColumnData.length; ++ii) {
                final int myModRowOffset;
                if (hasViewport) {
                    try (final RowSetGenerator modRowsGen = new RowSetGenerator(clientModdedRows[ii])) {
                        myModRowOffset = modRowsGen.addToFlatBuffer(metadata);
                    }
                } else {
                    myModRowOffset = modColumnData[ii].rowsModified.addToFlatBuffer(metadata);
                }
                modOffsets.add(BarrageModColumnMetadata.createBarrageModColumnMetadata(metadata, myModRowOffset));
            }

            BarrageUpdateMetadata.startModColumnNodesVector(metadata, modOffsets.size());
            modOffsets.forEachDescending(offset -> {
                metadata.addOffset(offset);
                return true;
            });
            final int nodesOffset = metadata.endVector();

            BarrageUpdateMetadata.startBarrageUpdateMetadata(metadata);
            BarrageUpdateMetadata.addIsSnapshot(metadata, isSnapshot);
            BarrageUpdateMetadata.addFirstSeq(metadata, firstSeq);
            BarrageUpdateMetadata.addLastSeq(metadata, lastSeq);
            BarrageUpdateMetadata.addEffectiveViewport(metadata, effectiveViewportOffset);
            BarrageUpdateMetadata.addEffectiveColumnSet(metadata, effectiveColumnSetOffset);
            BarrageUpdateMetadata.addAddedRows(metadata, rowsAddedOffset);
            BarrageUpdateMetadata.addRemovedRows(metadata, rowsRemovedOffset);
            BarrageUpdateMetadata.addShiftData(metadata, shiftDataOffset);
            BarrageUpdateMetadata.addAddedRowsIncluded(metadata, addedRowsIncludedOffset);
            BarrageUpdateMetadata.addModColumnNodes(metadata, nodesOffset);
            BarrageUpdateMetadata.addEffectiveReverseViewport(metadata, reverseViewport);
            BarrageUpdateMetadata.addTableSize(metadata, message.tableSize);
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

    @Override
    public MessageView getSnapshotView(final BarrageSnapshotOptions options,
            @Nullable final RowSet viewport,
            final boolean reverseViewport,
            @Nullable final RowSet keyspaceViewport,
            @Nullable final BitSet snapshotColumns) {
        return new SnapshotView(options, viewport, reverseViewport, keyspaceViewport, snapshotColumns);
    }

    @Override
    public MessageView getSnapshotView(BarrageSnapshotOptions options) {
        return getSnapshotView(options, null, false, null, null);
    }

    private final class SnapshotView implements RecordBatchMessageView {
        private final BarrageSnapshotOptions options;
        private final boolean reverseViewport;
        private final BitSet subscribedColumns;
        private final long numClientAddRows;

        private final WritableRowSet clientViewport;
        private final WritableRowSet clientAddedRows;
        private final WritableRowSet clientAddedRowOffsets;

        public SnapshotView(final BarrageSnapshotOptions options,
                @Nullable final RowSet viewport,
                final boolean reverseViewport,
                @Nullable final RowSet keyspaceViewport,
                @Nullable final BitSet subscribedColumns) {
            this.options = options;
            this.clientViewport = viewport == null ? null : viewport.copy();
            this.reverseViewport = reverseViewport;

            this.subscribedColumns = subscribedColumns;

            // precompute add row offsets
            if (keyspaceViewport != null) {
                clientAddedRows = keyspaceViewport.intersect(rowsIncluded.original);
                clientAddedRowOffsets = rowsIncluded.original.invert(clientAddedRows);
            } else {
                clientAddedRows = rowsAdded.original.copy();
                clientAddedRowOffsets = RowSetFactory.flat(clientAddedRows.size());
            }

            numClientAddRows = clientAddedRowOffsets.size();
        }

        @Override
        public void forEachStream(Consumer<DefensiveDrainable> visitor) throws IOException {
            final long startTm = System.nanoTime();
            ByteBuffer metadata = getSnapshotMetadata();
            MutableLong bytesWritten = new MutableLong(0L);

            // batch size is maximum, will write fewer rows when needed
            int maxBatchSize = batchSize();
            final MutableInt actualBatchSize = new MutableInt();
            try {
                if (numClientAddRows == 0) {
                    // we still need to send a message containing metadata when there are no rows
                    visitor.accept(getInputStream(this, 0, 0, actualBatchSize, metadata,
                            BarrageStreamGeneratorImpl.this::appendAddColumns));
                } else {
                    // send the add batches
                    processBatches(visitor, this, numClientAddRows, maxBatchSize, metadata,
                            BarrageStreamGeneratorImpl.this::appendAddColumns, bytesWritten);
                }
            } finally {
                SafeCloseable.closeAll(clientViewport, clientAddedRows, clientAddedRowOffsets);
            }

            writeConsumer.onWrite(bytesWritten.get(), System.nanoTime() - startTm);
        }

        private int batchSize() {
            int batchSize = options().batchSize();
            if (batchSize <= 0) {
                batchSize = DEFAULT_BATCH_SIZE;
            }
            return batchSize;
        }

        @Override
        public StreamReaderOptions options() {
            return options;
        }

        @Override
        public RowSet addRowOffsets() {
            return clientAddedRowOffsets;
        }

        @Override
        public RowSet modRowOffsets(int col) {
            throw new UnsupportedOperationException("asked for mod row on SnapshotView");
        }

        private ByteBuffer getSnapshotMetadata() throws IOException {
            final FlatBufferBuilder metadata = new FlatBufferBuilder();

            int effectiveViewportOffset = 0;
            if (clientViewport != null) {
                try (final RowSetGenerator viewportGen = new RowSetGenerator(clientViewport)) {
                    effectiveViewportOffset = viewportGen.addToFlatBuffer(metadata);
                }
            }

            int effectiveColumnSetOffset = 0;
            if (subscribedColumns != null) {
                effectiveColumnSetOffset = new BitSetGenerator(subscribedColumns).addToFlatBuffer(metadata);
            }

            final int rowsAddedOffset = rowsAdded.addToFlatBuffer(metadata);

            // no shifts in a snapshot, but need to provide a valid structure
            final int shiftDataOffset = shifted.addToFlatBuffer(metadata);

            // Added Chunk Data:
            int addedRowsIncludedOffset = 0;
            // don't send `rowsIncluded` when identical to `rowsAdded`, client will infer they are the same
            if (isSnapshot || !clientAddedRows.equals(rowsAdded.original)) {
                addedRowsIncludedOffset = rowsIncluded.addToFlatBuffer(clientAddedRows, metadata);
            }

            BarrageUpdateMetadata.startBarrageUpdateMetadata(metadata);
            BarrageUpdateMetadata.addIsSnapshot(metadata, isSnapshot);
            BarrageUpdateMetadata.addFirstSeq(metadata, firstSeq);
            BarrageUpdateMetadata.addLastSeq(metadata, lastSeq);
            BarrageUpdateMetadata.addEffectiveViewport(metadata, effectiveViewportOffset);
            BarrageUpdateMetadata.addEffectiveColumnSet(metadata, effectiveColumnSetOffset);
            BarrageUpdateMetadata.addAddedRows(metadata, rowsAddedOffset);
            BarrageUpdateMetadata.addRemovedRows(metadata, 0);
            BarrageUpdateMetadata.addShiftData(metadata, shiftDataOffset);
            BarrageUpdateMetadata.addAddedRowsIncluded(metadata, addedRowsIncludedOffset);
            BarrageUpdateMetadata.addModColumnNodes(metadata, 0);
            BarrageUpdateMetadata.addEffectiveReverseViewport(metadata, reverseViewport);
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

    private static final class SchemaMessageView implements MessageView {
        private final byte[] msgBytes;

        public SchemaMessageView(final ByteBuffer buffer) {
            this.msgBytes = Flight.FlightData.newBuilder()
                    .setDataHeader(ByteStringAccess.wrap(buffer))
                    .build()
                    .toByteArray();
        }

        @Override
        public void forEachStream(Consumer<DefensiveDrainable> visitor) {
            visitor.accept(new DrainableByteArrayInputStream(msgBytes, 0, msgBytes.length));
        }
    }

    @FunctionalInterface
    private interface ColumnVisitor {
        int visit(final RecordBatchMessageView view, final long startRange, final int targetBatchSize,
                final Consumer<DefensiveDrainable> addStream,
                final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener,
                final ChunkInputStreamGenerator.BufferListener bufferListener) throws IOException;
    }

    /**
     * Returns an InputStream of a single FlightData message filtered to the viewport (if provided). This function
     * accepts {@code targetBatchSize}, but may actually write fewer rows than the target (e.g. when crossing an
     * internal chunk boundary).
     *
     * @param view the view of the overall chunk to generate a RecordBatch for
     * @param offset the start of the batch in position space w.r.t. the view (inclusive)
     * @param targetBatchSize the target (and maximum) batch size to use for this message
     * @param actualBatchSize the number of rows actually sent in this batch (will be <= targetBatchSize)
     * @param metadata the optional flight data metadata to attach to the message
     * @param columnVisitor the helper method responsible for appending the payload columns to the RecordBatch
     * @return an InputStream ready to be drained by GRPC
     */
    private DefensiveDrainable getInputStream(
            final RecordBatchMessageView view,
            final long offset,
            final int targetBatchSize,
            final MutableInt actualBatchSize,
            final ByteBuffer metadata,
            final ColumnVisitor columnVisitor)
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
            // noinspection DataFlowIssue
            bufferInfos.get().setSize(0);

            final MutableLong totalBufferLength = new MutableLong();
            final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener =
                    (numElements, nullCount) -> {
                        nodeOffsets.ensureCapacityPreserve(nodeOffsets.get().size() + 1);
                        // noinspection resource
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

        header.finish(MessageHelper.wrapInMessage(header, headerOffset,
                org.apache.arrow.flatbuf.MessageHeader.RecordBatch, size.get()));

        // now create the proto header
        try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream()) {
            writeHeader(metadata, size, header, baos);
            streams.addFirst(new DrainableByteArrayInputStream(baos.peekBuffer(), 0, baos.size()));

            return new ConsecutiveDrainableStreams(streams.toArray(new DefensiveDrainable[0]));
        } catch (final IOException ex) {
            throw new UncheckedDeephavenException("Unexpected IOException", ex);
        }
    }

    /**
     * This implementation prepares the protobuf FlightData header.
     */
    protected void writeHeader(
            ByteBuffer metadata,
            MutableInt size,
            FlatBufferBuilder header,
            ExposedByteArrayOutputStream baos) throws IOException {
        final CodedOutputStream cos = CodedOutputStream.newInstance(baos);

        cos.writeByteBuffer(Flight.FlightData.DATA_HEADER_FIELD_NUMBER, header.dataBuffer().slice());
        if (metadata != null) {
            cos.writeByteBuffer(Flight.FlightData.APP_METADATA_FIELD_NUMBER, metadata);
        }

        cos.writeTag(Flight.FlightData.DATA_BODY_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
        cos.writeUInt32NoTag(size.get());
        cos.flush();
    }

    private void processBatches(Consumer<DefensiveDrainable> visitor, final RecordBatchMessageView view,
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
                final DefensiveDrainable is =
                        getInputStream(view, offset, batchSize, actualBatchSize, metadata, columnVisitor);
                int bytesToWrite = is.available();

                if (actualBatchSize.get() == 0) {
                    throw new IllegalStateException("No data was written for a batch");
                }

                // treat this as a hard limit, exceeding fails a client or w2w (unless we are sending a single
                // row then we must send and let it potentially fail)
                if (sendAllowed && (bytesToWrite < maxMessageSize || batchSize == 1)) {
                    // let's write the data
                    visitor.accept(is);

                    bytesWritten.add(bytesToWrite);
                    offset += actualBatchSize.get();
                    metadata = null;
                } else {
                    // can't write this, so close the input stream and retry
                    is.close();
                    sendAllowed = true;
                }

                // recompute the batch limit for the next message
                int bytesPerRow = bytesToWrite / actualBatchSize.get();
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
        if (generators.size() <= 1) {
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

    private int appendModColumns(final RecordBatchMessageView view, final long startRange, final int targetBatchSize,
            final Consumer<DefensiveDrainable> addStream,
            final ChunkInputStreamGenerator.FieldNodeListener fieldNodeListener,
            final ChunkInputStreamGenerator.BufferListener bufferListener) throws IOException {
        int[] columnChunkIdx = new int[modColumnData.length];

        // for each column identify the chunk that holds this startRange
        long maxLength = targetBatchSize;

        // adjust the batch size if we would cross a chunk boundary
        for (int ii = 0; ii < modColumnData.length; ++ii) {
            final ModColumnGenerator mcd = modColumnData[ii];
            final List<ChunkInputStreamGenerator> generators = mcd.data.generators();
            if (generators.isEmpty()) {
                continue;
            }

            final RowSet modOffsets = view.modRowOffsets(ii);
            // if all mods are being sent, then offsets yield an identity mapping
            final long startPos = modOffsets != null ? modOffsets.get(startRange) : startRange;
            if (startPos != RowSet.NULL_ROW_KEY) {
                final int chunkIdx = findGeneratorForOffset(generators, startPos);
                if (chunkIdx < generators.size() - 1) {
                    maxLength = Math.min(maxLength, generators.get(chunkIdx).getLastRowOffset() + 1 - startPos);
                }
                columnChunkIdx[ii] = chunkIdx;
            }
        }

        // now add mod-column streams, and write the mod column indexes
        long numRows = 0;
        for (int ii = 0; ii < modColumnData.length; ++ii) {
            final ModColumnGenerator mcd = modColumnData[ii];
            final ChunkInputStreamGenerator generator = mcd.data.generators().isEmpty()
                    ? null
                    : mcd.data.generators().get(columnChunkIdx[ii]);

            final RowSet modOffsets = view.modRowOffsets(ii);
            long startPos, endPos;
            if (modOffsets != null) {
                startPos = modOffsets.get(startRange);
                final long endRange = startRange + maxLength - 1;
                endPos = endRange >= modOffsets.size() ? modOffsets.lastRowKey() : modOffsets.get(endRange);
            } else if (startRange >= mcd.rowsModified.original.size()) {
                startPos = RowSet.NULL_ROW_KEY;
                endPos = RowSet.NULL_ROW_KEY;
            } else {
                // if all mods are being sent, then offsets yield an identity mapping
                startPos = startRange;
                endPos = startRange + maxLength - 1;
                if (generator != null) {
                    endPos = Math.min(endPos, generator.getLastRowOffset());
                }
            }

            final RowSet myModOffsets;
            if (startPos == RowSet.NULL_ROW_KEY) {
                // not all mod columns have the same length
                myModOffsets = RowSetFactory.empty();
            } else if (modOffsets != null) {
                try (final RowSet allowedRange = RowSetFactory.fromRange(startPos, endPos)) {
                    myModOffsets = modOffsets.intersect(allowedRange);
                }
            } else {
                myModOffsets = RowSetFactory.fromRange(startPos, endPos);
            }
            numRows = Math.max(numRows, myModOffsets.size());

            try {
                final int numElements = generator == null ? 0 : myModOffsets.intSize("BarrageStreamGenerator");
                if (view.options().columnsAsList()) {
                    // if we are sending columns as a list, we need to add the list buffers before each column
                    final SingleElementListHeaderInputStreamGenerator listHeader =
                            new SingleElementListHeaderInputStreamGenerator(numElements);
                    listHeader.visitFieldNodes(fieldNodeListener);
                    listHeader.visitBuffers(bufferListener);
                    addStream.accept(listHeader);
                }

                if (numElements == 0) {
                    // use the empty generator to publish the column data
                    try (final RowSet empty = RowSetFactory.empty()) {
                        final ChunkInputStreamGenerator.DrainableColumn drainableColumn =
                                mcd.data.empty(view.options(), empty);
                        drainableColumn.visitFieldNodes(fieldNodeListener);
                        drainableColumn.visitBuffers(bufferListener);
                        // Add the drainable last as it is allowed to immediately close a row set the visitors need
                        addStream.accept(drainableColumn);
                    }
                } else {
                    final long shift = -generator.getRowOffset();
                    // normalize to the chunk offsets
                    try (final WritableRowSet adjustedOffsets = shift == 0 ? null : myModOffsets.shift(shift)) {
                        final ChunkInputStreamGenerator.DrainableColumn drainableColumn =
                                generator.getInputStream(view.options(), shift == 0 ? myModOffsets : adjustedOffsets);
                        drainableColumn.visitFieldNodes(fieldNodeListener);
                        drainableColumn.visitBuffers(bufferListener);
                        // Add the drainable last as it is allowed to immediately close a row set the visitors need
                        addStream.accept(drainableColumn);
                    }
                }
            } finally {
                myModOffsets.close();
            }
        }
        return Math.toIntExact(numRows);
    }

    public static abstract class ByteArrayGenerator {
        protected int len;
        protected volatile byte[] raw;

        protected abstract void ensureComputed() throws IOException;

        protected int addToFlatBuffer(final FlatBufferBuilder builder) throws IOException {
            ensureComputed();
            return builder.createByteVector(raw, 0, len);
        }
    }

    public static class RowSetGenerator extends ByteArrayGenerator implements SafeCloseable {
        private final RowSet original;

        public RowSetGenerator(final RowSet rowSet) throws IOException {
            this.original = rowSet.copy();
        }

        @Override
        public void close() {
            original.close();
        }

        protected void ensureComputed() throws IOException {
            if (raw != null) {
                return;
            }

            synchronized (this) {
                if (raw != null) {
                    return;
                }

                try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                        final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos)) {
                    ExternalizableRowSetUtils.writeExternalCompressedDeltas(oos, original);
                    oos.flush();
                    len = baos.size();
                    raw = baos.peekBuffer();
                }
            }
        }

        /**
         * Appends the intersection of the viewport and the originally provided RowSet.
         *
         * @param viewport the key-space version of the viewport
         * @param builder the flatbuffer builder
         * @return offset of the item in the flatbuffer
         */
        protected int addToFlatBuffer(final RowSet viewport, final FlatBufferBuilder builder) throws IOException {
            if (original.subsetOf(viewport)) {
                ensureComputed();
                return addToFlatBuffer(builder);
            }

            final int nlen;
            final byte[] nraw;
            try (final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                    final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos);
                    final RowSet viewOfOriginal = original.intersect(viewport)) {
                ExternalizableRowSetUtils.writeExternalCompressedDeltas(oos, viewOfOriginal);
                oos.flush();
                nlen = baos.size();
                nraw = baos.peekBuffer();
            }

            return builder.createByteVector(nraw, 0, nlen);
        }
    }

    public static class BitSetGenerator extends ByteArrayGenerator {
        private final BitSet original;

        public BitSetGenerator(final BitSet bitset) {
            original = bitset == null ? new BitSet() : (BitSet) bitset.clone();
        }

        @Override
        protected void ensureComputed() {
            if (raw != null) {
                return;
            }

            synchronized (this) {
                if (raw != null) {
                    return;
                }

                final int nBits = original.previousSetBit(Integer.MAX_VALUE - 1) + 1;
                len = (int) ((long) nBits + 7) / 8;
                raw = original.toByteArray();
            }
        }
    }

    public static class RowSetShiftDataGenerator extends ByteArrayGenerator {
        private final RowSetShiftData original;

        public RowSetShiftDataGenerator(final RowSetShiftData shifted) throws IOException {
            original = shifted;
        }

        protected void ensureComputed() throws IOException {
            if (raw != null) {
                return;
            }

            synchronized (this) {
                if (raw != null) {
                    return;
                }

                final RowSetBuilderSequential sRangeBuilder = RowSetFactory.builderSequential();
                final RowSetBuilderSequential eRangeBuilder = RowSetFactory.builderSequential();
                final RowSetBuilderSequential destBuilder = RowSetFactory.builderSequential();

                if (original != null) {
                    for (int i = 0; i < original.size(); ++i) {
                        long s = original.getBeginRange(i);
                        final long dt = original.getShiftDelta(i);

                        if (dt < 0 && s < -dt) {
                            s = -dt;
                        }

                        sRangeBuilder.appendKey(s);
                        eRangeBuilder.appendKey(original.getEndRange(i));
                        destBuilder.appendKey(s + dt);
                    }
                }

                try (final RowSet sRange = sRangeBuilder.build();
                        final RowSet eRange = eRangeBuilder.build();
                        final RowSet dest = destBuilder.build();
                        final ExposedByteArrayOutputStream baos = new ExposedByteArrayOutputStream();
                        final LittleEndianDataOutputStream oos = new LittleEndianDataOutputStream(baos)) {
                    ExternalizableRowSetUtils.writeExternalCompressedDeltas(oos, sRange);
                    ExternalizableRowSetUtils.writeExternalCompressedDeltas(oos, eRange);
                    ExternalizableRowSetUtils.writeExternalCompressedDeltas(oos, dest);
                    oos.flush();
                    len = baos.size();
                    raw = baos.peekBuffer();
                }
            }
        }
    }

    private static final class EmptyRowSetGenerator extends RowSetGenerator {
        public static final EmptyRowSetGenerator INSTANCE;
        static {
            try {
                INSTANCE = new EmptyRowSetGenerator();
            } catch (final IOException ioe) {
                throw new UncheckedDeephavenException(ioe);
            }
        }

        EmptyRowSetGenerator() throws IOException {
            super(RowSetFactory.empty());
        }

        @Override
        public void close() {
            // no-op; this is very re-usable
        }
    }
}
