/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.arrow;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.rpc.Code;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import gnu.trove.iterator.TLongIterator;
import gnu.trove.list.array.TLongArrayList;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.chunk.ChunkType;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.*;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.flight.util.MessageHelper;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.barrage.BarrageStreamGenerator;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.RecordBatch;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;

import static io.deephaven.extensions.barrage.util.BarrageProtoUtil.DEFAULT_SER_OPTIONS;
import static io.deephaven.server.arrow.FlightServiceGrpcImpl.DEFAULT_SNAPSHOT_DESER_OPTIONS;
import static io.deephaven.server.barrage.BarrageMessageProducer.*;
import static io.deephaven.server.barrage.BarrageMessageProducer.TARGET_SNAPSHOT_PERCENTAGE;

public class ArrowFlightUtil {
    private static final Logger log = LoggerFactory.getLogger(ArrowFlightUtil.class);

    public static final int DEFAULT_MIN_UPDATE_INTERVAL_MS =
            Configuration.getInstance().getIntegerWithDefault("barrage.minUpdateInterval", 1000);

    static final BarrageMessage.ModColumnData[] ZERO_MOD_COLUMNS = new BarrageMessage.ModColumnData[0];

    public static void DoGetCustom(@Nullable final ScheduledExecutorService executorService,
            final SessionState session,
            final TicketRouter ticketRouter,
            final Flight.Ticket request,
            final StreamObserver<InputStream> observer) {

        final SessionState.ExportObject<BaseTable> export =
                ticketRouter.resolve(session, request, "request");

        final BarragePerformanceLog.SnapshotMetricsHelper metrics =
                new BarragePerformanceLog.SnapshotMetricsHelper();

        final long queueStartTm = System.nanoTime();
        session.nonExport()
                .require(export)
                .onError(observer)
                .submit(() -> {
                    metrics.queueNanos = System.nanoTime() - queueStartTm;
                    final BaseTable table = export.get();
                    metrics.tableId = Integer.toHexString(System.identityHashCode(table));
                    metrics.tableKey = BarragePerformanceLog.getKeyFor(table);

                    // create an adapter for the response observer
                    final StreamObserver<BarrageStreamGenerator.View> listener =
                            ArrowModule.provideListenerAdapter().adapt(observer);

                    // Send Schema wrapped in Message
                    final FlatBufferBuilder builder = new FlatBufferBuilder();
                    final int schemaOffset = BarrageUtil.makeSchemaPayload(builder, table.getDefinition(),
                            table.getAttributes());
                    builder.finish(MessageHelper.wrapInMessage(builder, schemaOffset,
                            org.apache.arrow.flatbuf.MessageHeader.Schema));
                    final ByteBuffer serializedMessage = builder.dataBuffer();

                    // leverage the stream generator SchemaView constructor
                    final BarrageStreamGenerator.SchemaView schemaView =
                            new BarrageStreamGenerator.SchemaView(serializedMessage);

                    // push the schema to the listener
                    listener.onNext(schemaView);

                    // shared code between `DoGet` and `BarrageSnapshotRequest`
                    createAndSendSnapshot(table, null, null, false, DEFAULT_SNAPSHOT_DESER_OPTIONS, listener, metrics);

                    listener.onCompleted();
                });
    }


    private static void createAndSendStaticSnapshot(BaseTable table, BitSet columns, RowSet viewport,
            boolean reverseViewport, BarrageSnapshotOptions snapshotRequestOptions,
            StreamObserver<BarrageStreamGenerator.View> listener, BarragePerformanceLog.SnapshotMetricsHelper metrics) {
        // start with small value and grow
        long snapshotTargetCellCount = MIN_SNAPSHOT_CELL_COUNT;
        double snapshotNanosPerCell = 0.0;

        final long columnCount =
                Math.max(1, columns != null ? columns.cardinality() : table.getDefinition().getColumns().size());

        try (final WritableRowSet snapshotViewport = RowSetFactory.empty();
                final WritableRowSet targetViewport = RowSetFactory.empty()) {
            // compute the target viewport
            if (viewport == null) {
                targetViewport.insertRange(0, table.size() - 1);
            } else if (!reverseViewport) {
                targetViewport.insert(viewport);
            } else {
                // compute the forward version of the reverse viewport
                try (final RowSet rowKeys = table.getRowSet().subSetForReversePositions(viewport);
                        final RowSet inverted = table.getRowSet().invert(rowKeys)) {
                    targetViewport.insert(inverted);
                }
            }

            try (final RowSequence.Iterator rsIt = targetViewport.getRowSequenceIterator()) {
                while (rsIt.hasMore()) {
                    // compute the next range to snapshot
                    final long cellCount =
                            Math.max(MIN_SNAPSHOT_CELL_COUNT,
                                    Math.min(snapshotTargetCellCount, MAX_SNAPSHOT_CELL_COUNT));

                    final RowSequence snapshotPartialViewport = rsIt.getNextRowSequenceWithLength(cellCount);
                    // add these ranges to the running total
                    snapshotPartialViewport.forEachRowKeyRange((start, end) -> {
                        snapshotViewport.insertRange(start, end);
                        return true;
                    });

                    // grab the snapshot and measure elapsed time for next projections
                    long start = System.nanoTime();
                    final BarrageMessage msg =
                            ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(log, table,
                                    columns, snapshotPartialViewport, null);
                    msg.modColumnData = ZERO_MOD_COLUMNS; // no mod column data for DoGet
                    long elapsed = System.nanoTime() - start;
                    // accumulate snapshot time in the metrics
                    metrics.snapshotNanos += elapsed;

                    // send out the data. Note that although a `BarrageUpdateMetaData` object will
                    // be provided with each unique snapshot, vanilla Flight clients will ignore
                    // these and see only an incoming stream of batches
                    try (final BarrageStreamGenerator bsg = new BarrageStreamGenerator(msg, metrics)) {
                        if (rsIt.hasMore()) {
                            listener.onNext(bsg.getSnapshotView(snapshotRequestOptions,
                                    snapshotViewport, false,
                                    msg.rowsIncluded, columns));
                        } else {
                            listener.onNext(bsg.getSnapshotView(snapshotRequestOptions,
                                    viewport, reverseViewport,
                                    msg.rowsIncluded, columns));
                        }
                    }

                    if (msg.rowsIncluded.size() > 0) {
                        // very simplistic logic to take the last snapshot and extrapolate max
                        // number of rows that will not exceed the target UGP processing time
                        // percentage
                        long targetNanos = (long) (TARGET_SNAPSHOT_PERCENTAGE
                                * UpdateGraphProcessor.DEFAULT.getTargetCycleDurationMillis()
                                * 1000000);

                        long nanosPerCell = elapsed / (msg.rowsIncluded.size() * columnCount);

                        // apply an exponential moving average to filter the data
                        if (snapshotNanosPerCell == 0) {
                            snapshotNanosPerCell = nanosPerCell; // initialize to first value
                        } else {
                            // EMA smoothing factor is 0.1 (N = 10)
                            snapshotNanosPerCell =
                                    (snapshotNanosPerCell * 0.9) + (nanosPerCell * 0.1);
                        }

                        snapshotTargetCellCount =
                                (long) (targetNanos / Math.max(1, snapshotNanosPerCell));
                    }
                }
            }
        }
    }

    private static void createAndSendSnapshot(BaseTable table, BitSet columns, RowSet viewport, boolean reverseViewport,
            BarrageSnapshotOptions snapshotRequestOptions, StreamObserver<BarrageStreamGenerator.View> listener,
            BarragePerformanceLog.SnapshotMetricsHelper metrics) {

        // if the table is static and a full snapshot is requested, we can make and send multiple
        // snapshots to save memory and operate more efficiently
        if (!table.isRefreshing()) {
            createAndSendStaticSnapshot(table, columns, viewport, reverseViewport, snapshotRequestOptions, listener,
                    metrics);
            return;
        }

        // otherwise snapshot the entire request and send to the client
        final BarrageMessage msg;

        final long snapshotStartTm = System.nanoTime();
        if (reverseViewport) {
            msg = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(log, table,
                    columns, null, viewport);
        } else {
            msg = ConstructSnapshot.constructBackplaneSnapshotInPositionSpace(log, table,
                    columns, viewport, null);
        }
        metrics.snapshotNanos = System.nanoTime() - snapshotStartTm;

        msg.modColumnData = ZERO_MOD_COLUMNS; // no mod column data

        // translate the viewport to keyspace and make the call
        try (final BarrageStreamGenerator bsg = new BarrageStreamGenerator(msg, metrics);
                final RowSet keySpaceViewport =
                        viewport != null
                                ? msg.rowsAdded.subSetForPositions(viewport, reverseViewport)
                                : null) {
            listener.onNext(
                    bsg.getSnapshotView(snapshotRequestOptions, viewport,
                            reverseViewport, keySpaceViewport, columns));
        }
    }

    /**
     * This is a stateful observer; a DoPut stream begins with its schema.
     */
    public static class DoPutObserver implements StreamObserver<InputStream>, Closeable {

        private final ScheduledExecutorService executorService;
        private final SessionState session;
        private final TicketRouter ticketRouter;
        private final StreamObserver<Flight.PutResult> observer;

        private long totalRowsRead = 0;
        private BarrageTable resultTable;
        private SessionState.ExportBuilder<Table> resultExportBuilder;
        private Flight.FlightDescriptor flightDescriptor;

        private ChunkType[] columnChunkTypes;
        private int[] columnConversionFactors;
        private Class<?>[] columnTypes;
        private Class<?>[] componentTypes;

        private BarrageSubscriptionOptions options = DEFAULT_SER_OPTIONS;

        public DoPutObserver(
                @Nullable final ScheduledExecutorService executorService,
                final SessionState session,
                final TicketRouter ticketRouter,
                final StreamObserver<Flight.PutResult> observer) {
            this.executorService = executorService;
            this.session = session;
            this.ticketRouter = ticketRouter;
            this.observer = observer;

            this.session.addOnCloseCallback(this);
            if (observer instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver<Flight.PutResult>) observer).setOnCancelHandler(this::onCancel);
            }
        }

        // this is the entry point for client-streams
        @Override
        public void onNext(final InputStream request) {
            GrpcUtil.rpcWrapper(log, observer, () -> {
                final BarrageProtoUtil.MessageInfo mi = BarrageProtoUtil.parseProtoMessage(request);
                if (mi.descriptor != null) {
                    if (flightDescriptor != null) {
                        if (!flightDescriptor.equals(mi.descriptor)) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    "additional flight descriptor sent does not match original descriptor");
                        }
                    } else {
                        flightDescriptor = mi.descriptor;
                        resultExportBuilder = ticketRouter
                                .<Table>publish(session, mi.descriptor, "Flight.Descriptor")
                                .onError(observer);
                    }
                }

                if (mi.app_metadata != null
                        && mi.app_metadata.msgType() == BarrageMessageType.BarrageSerializationOptions) {
                    options = BarrageSubscriptionOptions.of(BarrageSubscriptionRequest
                            .getRootAsBarrageSubscriptionRequest(mi.app_metadata.msgPayloadAsByteBuffer()));
                }

                if (mi.header == null) {
                    return; // nothing to do!
                }

                if (mi.header.headerType() == MessageHeader.Schema) {
                    parseSchema((Schema) mi.header.header(new Schema()));
                    return;
                }

                if (mi.header.headerType() != MessageHeader.RecordBatch) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Only schema/record-batch messages supported");
                }

                final int numColumns = resultTable.getColumnSources().size();
                final BarrageMessage msg = new BarrageMessage();
                final RecordBatch batch = (RecordBatch) mi.header.header(new RecordBatch());

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
                    bufferInfo.add(length);
                }
                final TLongIterator bufferInfoIter = bufferInfo.iterator();

                msg.rowsRemoved = RowSetFactory.empty();
                msg.shifted = RowSetShiftData.EMPTY;

                // include all columns as add-columns
                int numRowsAdded = LongSizedDataStructure.intSize("RecordBatch.length()", batch.length());
                msg.addColumnData = new BarrageMessage.AddColumnData[numColumns];
                for (int ci = 0; ci < numColumns; ++ci) {
                    final BarrageMessage.AddColumnData acd = new BarrageMessage.AddColumnData();
                    msg.addColumnData[ci] = acd;
                    msg.addColumnData[ci].data = new ArrayList<>();
                    final int factor = (columnConversionFactors == null) ? 1 : columnConversionFactors[ci];
                    try {
                        acd.data.add(ChunkInputStreamGenerator.extractChunkFromInputStream(options, factor,
                                columnChunkTypes[ci], columnTypes[ci], componentTypes[ci], fieldNodeIter,
                                bufferInfoIter, mi.inputStream, null, 0, 0));
                    } catch (final IOException unexpected) {
                        throw new UncheckedDeephavenException(unexpected);
                    }

                    if (acd.data.get(0).size() != numRowsAdded) {
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "Inconsistent num records per column: " + numRowsAdded + " != " + acd.data.size());
                    }
                    acd.type = columnTypes[ci];
                    acd.componentType = componentTypes[ci];
                }

                msg.rowsAdded = RowSetFactory.fromRange(totalRowsRead, totalRowsRead + numRowsAdded - 1);
                msg.rowsIncluded = msg.rowsAdded.copy();
                msg.modColumnData = ZERO_MOD_COLUMNS;
                totalRowsRead += numRowsAdded;

                resultTable.handleBarrageMessage(msg);

                // no app_metadata to report; but ack the processing
                GrpcUtil.safelyExecuteLocked(observer, () -> observer.onNext(Flight.PutResult.getDefaultInstance()));
            });
        }

        private void onCancel() {
            if (resultTable != null) {
                resultTable.dropReference();
                resultTable = null;
            }
            if (resultExportBuilder != null) {
                // this thrown error propagates to observer
                resultExportBuilder.submit(() -> {
                    throw GrpcUtil.statusRuntimeException(Code.CANCELLED, "cancelled");
                });
                resultExportBuilder = null;
            }

            session.removeOnCloseCallback(this);
        }

        @Override
        public void onError(Throwable t) {
            // ok; we're done then
            if (resultTable != null) {
                resultTable.dropReference();
                resultTable = null;
            }
            if (resultExportBuilder != null) {
                // this thrown error propagates to observer
                resultExportBuilder.submit(() -> {
                    throw new UncheckedDeephavenException(t);
                });
                resultExportBuilder = null;
            }

            session.removeOnCloseCallback(this);
        }

        public void onCompleted() {
            GrpcUtil.rpcWrapper(log, observer, () -> {
                if (resultExportBuilder == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Result flight descriptor never provided");
                }
                if (resultTable == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Result flight schema never provided");
                }

                final BarrageTable localResultTable = resultTable;
                resultTable = null;
                final SessionState.ExportBuilder<Table> localExportBuilder = resultExportBuilder;
                resultExportBuilder = null;


                // gRPC is about to remove its hard reference to this observer. We must keep the result table hard
                // referenced until the export is complete, so that the export can properly be satisfied. ExportObject's
                // LivenessManager enforces strong reachability.
                if (!localExportBuilder.getExport().tryManage(localResultTable)) {
                    GrpcUtil.safelyError(observer, Code.DATA_LOSS, "Result export already destroyed");
                    localResultTable.dropReference();
                    session.removeOnCloseCallback(this);
                    return;
                }
                localResultTable.dropReference();

                // no more changes allowed; this is officially static content
                localResultTable.sealTable(() -> localExportBuilder.submit(() -> {
                    GrpcUtil.safelyExecuteLocked(observer, observer::onCompleted);
                    session.removeOnCloseCallback(this);
                    return localResultTable;
                }), () -> {
                    GrpcUtil.safelyError(observer, Code.DATA_LOSS, "Do put could not be sealed");
                    session.removeOnCloseCallback(this);
                });
            });
        }

        @Override
        public void close() {
            // close() is intended to be invoked only though session expiration
            GrpcUtil.safelyError(observer, Code.UNAUTHENTICATED, "Session expired");
        }

        private void parseSchema(final Schema header) {
            if (resultTable != null) {
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Schema evolution not supported");
            }

            final BarrageUtil.ConvertedArrowSchema result = BarrageUtil.convertArrowSchema(header);
            resultTable = BarrageTable.make(executorService, result.tableDef, result.attributes, -1);
            columnConversionFactors = result.conversionFactors;
            columnChunkTypes = resultTable.getWireChunkTypes();
            columnTypes = resultTable.getWireTypes();
            componentTypes = resultTable.getWireComponentTypes();

            // retain reference until we can pass this result to be owned by the export object
            resultTable.retainReference();
        }
    }

    /**
     * Helper class that maintains a subscription whether it was created by a bi-directional stream request or the
     * no-client-streaming request. If the SubscriptionRequest sets the sequence, then it treats sequence as a watermark
     * and will not send out-of-order requests (due to out-of-band requests). The client should already anticipate
     * subscription changes may be coalesced by the BarrageMessageProducer.
     */
    public static class DoExchangeMarshaller extends SingletonLivenessManager
            implements StreamObserver<InputStream>, Closeable {

        @AssistedFactory
        public interface Factory {
            DoExchangeMarshaller openExchange(SessionState session, StreamObserver<InputStream> observer);
        }

        private final String myPrefix;
        private final SessionState session;

        private final StreamObserver<BarrageStreamGenerator.View> listener;

        private boolean isClosed = false;

        private boolean isFirstMsg = true;

        private final TicketRouter ticketRouter;
        private final BarrageMessageProducer.Operation.Factory<BarrageStreamGenerator.View> operationFactory;
        private final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, BarrageSubscriptionOptions> subscriptionOptAdapter;
        private final BarrageMessageProducer.Adapter<BarrageSnapshotRequest, BarrageSnapshotOptions> snapshotOptAdapter;

        /**
         * Interface for the individual handlers for the DoExchange.
         */
        interface Handler extends Closeable {
            void handleMessage(@NotNull BarrageProtoUtil.MessageInfo message);
        }

        private Handler requestHandler = null;

        @AssistedInject
        public DoExchangeMarshaller(
                final TicketRouter ticketRouter,
                final BarrageMessageProducer.Operation.Factory<BarrageStreamGenerator.View> operationFactory,
                final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<BarrageStreamGenerator.View>> listenerAdapter,
                final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, BarrageSubscriptionOptions> subscriptionOptAdapter,
                final BarrageMessageProducer.Adapter<BarrageSnapshotRequest, BarrageSnapshotOptions> snapshotOptAdapter,
                @Assisted final SessionState session, @Assisted final StreamObserver<InputStream> responseObserver) {

            this.myPrefix = "DoExchangeMarshaller{" + Integer.toHexString(System.identityHashCode(this)) + "}: ";
            this.ticketRouter = ticketRouter;
            this.operationFactory = operationFactory;
            this.subscriptionOptAdapter = subscriptionOptAdapter;
            this.snapshotOptAdapter = snapshotOptAdapter;
            this.session = session;
            this.listener = listenerAdapter.adapt(responseObserver);

            this.session.addOnCloseCallback(this);
            if (responseObserver instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver<InputStream>) responseObserver).setOnCancelHandler(this::onCancel);
            }
        }

        // this entry is used for client-streaming requests
        @Override
        public void onNext(final InputStream request) {
            GrpcUtil.rpcWrapper(log, listener, () -> {
                BarrageProtoUtil.MessageInfo message = BarrageProtoUtil.parseProtoMessage(request);
                synchronized (this) {

                    // `FlightData` messages from Barrage clients will provide app_metadata describing the request but
                    // official Flight implementations may force a NULL metadata field in the first message. In that
                    // case, identify a valid Barrage connection by verifying the `FlightDescriptor.CMD` field contains
                    // the `Barrage` magic bytes

                    if (requestHandler != null) {
                        // rely on the handler to verify message type
                        requestHandler.handleMessage(message);
                        return;
                    }

                    if (message.app_metadata != null) {
                        // handle the different message types that can come over DoExchange
                        switch (message.app_metadata.msgType()) {
                            case BarrageMessageType.BarrageSubscriptionRequest:
                                requestHandler = new SubscriptionRequestHandler();
                                break;
                            case BarrageMessageType.BarrageSnapshotRequest:
                                requestHandler = new SnapshotRequestHandler();
                                break;
                            default:
                                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                        myPrefix + "received a message with unhandled BarrageMessageType");
                        }
                        requestHandler.handleMessage(message);
                        return;
                    }

                    // handle the possible error cases
                    if (!isFirstMsg) {
                        // only the first messages is allowed to have null metadata
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                myPrefix + "failed to receive Barrage request metadata");
                    }

                    isFirstMsg = false;

                    // The magic value is '0x6E687064'. It is the numerical representation of the ASCII "dphn".
                    int size = message.descriptor.getCmd().size();
                    if (size == 4) {
                        ByteBuffer bb = message.descriptor.getCmd().asReadOnlyByteBuffer();

                        // set the order to little-endian (FlatBuffers default)
                        bb.order(ByteOrder.LITTLE_ENDIAN);

                        // read and compare the value to the "magic" bytes
                        long value = (long) bb.getInt(0) & 0xFFFFFFFFL;
                        if (value != BarrageUtil.FLATBUFFER_MAGIC) {
                            throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    myPrefix + "expected BarrageMessageWrapper magic bytes in FlightDescriptor.cmd");
                        }
                    } else {
                        throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                                myPrefix + "expected BarrageMessageWrapper magic bytes in FlightDescriptor.cmd");
                    }
                }
            });
        }

        public void onCancel() {
            log.debug().append(myPrefix).append("cancel requested").endl();
            tryClose();
        }

        @Override
        public void onError(final Throwable t) {
            boolean doLog = true;
            if (t instanceof StatusRuntimeException) {
                final Status status = ((StatusRuntimeException) t).getStatus();
                if (status.getCode() == Status.Code.CANCELLED || status.getCode() == Status.Code.ABORTED) {
                    doLog = false;
                }
            }
            if (doLog) {
                log.error().append(myPrefix).append("unexpected error; force closing subscription: caused by ")
                        .append(t).endl();
            }
            tryClose();
        }

        @Override
        public void onCompleted() {
            log.debug().append(myPrefix).append("client stream closed subscription").endl();
            tryClose();
        }

        @Override
        public void close() {
            synchronized (this) {
                if (isClosed) {
                    return;
                }

                isClosed = true;
            }

            try {
                if (requestHandler != null) {
                    requestHandler.close();
                }
            } catch (IOException ioException) {
                throw new UncheckedDeephavenException("IOException closing handler", ioException);
            }

            release();
        }

        private void tryClose() {
            if (session.removeOnCloseCallback(this)) {
                close();
            }
        }

        /**
         * Handler for DoGetRequest over DoExchange.
         */
        private class SnapshotRequestHandler
                implements Handler {

            public SnapshotRequestHandler() {}

            @Override
            public void handleMessage(BarrageProtoUtil.MessageInfo message) {
                // verify this is the correct type of message for this handler
                if (message.app_metadata.msgType() != BarrageMessageType.BarrageSnapshotRequest) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Request type cannot be changed after initialization, expected BarrageSnapshotRequest metadata");
                }

                // ensure synchronization with parent class functions
                synchronized (DoExchangeMarshaller.this) {
                    final BarrageSnapshotRequest snapshotRequest = BarrageSnapshotRequest
                            .getRootAsBarrageSnapshotRequest(message.app_metadata.msgPayloadAsByteBuffer());

                    final SessionState.ExportObject<BaseTable> parent =
                            ticketRouter.resolve(session, snapshotRequest.ticketAsByteBuffer(), "ticket");

                    final BarragePerformanceLog.SnapshotMetricsHelper metrics =
                            new BarragePerformanceLog.SnapshotMetricsHelper();

                    final long queueStartTm = System.nanoTime();
                    session.nonExport()
                            .require(parent)
                            .onError(listener)
                            .submit(() -> {
                                metrics.queueNanos = System.nanoTime() - queueStartTm;
                                final BaseTable table = parent.get();
                                metrics.tableId = Integer.toHexString(System.identityHashCode(table));
                                metrics.tableKey = BarragePerformanceLog.getKeyFor(table);

                                // Send Schema wrapped in Message
                                final FlatBufferBuilder builder = new FlatBufferBuilder();
                                final int schemaOffset = BarrageUtil.makeSchemaPayload(builder, table.getDefinition(),
                                        table.getAttributes());
                                builder.finish(MessageHelper.wrapInMessage(builder, schemaOffset,
                                        org.apache.arrow.flatbuf.MessageHeader.Schema));
                                final ByteBuffer serializedMessage = builder.dataBuffer();

                                // leverage the stream generator SchemaView constructor
                                final BarrageStreamGenerator.SchemaView schemaView =
                                        new BarrageStreamGenerator.SchemaView(serializedMessage);

                                // push the schema to the listener
                                listener.onNext(schemaView);

                                // collect the viewport and columnsets (if provided)
                                final boolean hasColumns = snapshotRequest.columnsVector() != null;
                                final BitSet columns =
                                        hasColumns ? BitSet.valueOf(snapshotRequest.columnsAsByteBuffer()) : null;

                                final boolean hasViewport = snapshotRequest.viewportVector() != null;
                                RowSet viewport =
                                        hasViewport
                                                ? BarrageProtoUtil.toRowSet(snapshotRequest.viewportAsByteBuffer())
                                                : null;

                                final boolean reverseViewport = snapshotRequest.reverseViewport();

                                // leverage common code for `DoGet` and `BarrageSnapshotOptions`
                                createAndSendSnapshot(table, columns, viewport, reverseViewport,
                                        snapshotOptAdapter.adapt(snapshotRequest), listener, metrics);

                                listener.onCompleted();
                            });
                }
            }

            @Override
            public void close() {
                // no work to do for DoGetRequest close
            }
        }


        /**
         * Handler for BarrageSubscriptionRequest over DoExchange.
         */
        private class SubscriptionRequestHandler
                implements Handler {

            private BarrageMessageProducer<BarrageStreamGenerator.View> bmp;
            private Queue<BarrageSubscriptionRequest> preExportSubscriptions;
            private SessionState.ExportObject<?> onExportResolvedContinuation;

            public SubscriptionRequestHandler() {}

            @Override
            public void handleMessage(BarrageProtoUtil.MessageInfo message) {
                // verify this is the correct type of message for this handler
                if (message.app_metadata.msgType() != BarrageMessageType.BarrageSubscriptionRequest) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Request type cannot be changed after initialization, expected BarrageSubscriptionRequest metadata");
                }

                if (message.app_metadata.msgPayloadVector() == null) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Subscription request not supplied");
                }

                // ensure synchronization with parent class functions
                synchronized (DoExchangeMarshaller.this) {

                    final BarrageSubscriptionRequest subscriptionRequest = BarrageSubscriptionRequest
                            .getRootAsBarrageSubscriptionRequest(message.app_metadata.msgPayloadAsByteBuffer());

                    if (bmp != null) {
                        apply(subscriptionRequest);
                        return;
                    }

                    if (isClosed) {
                        return;
                    }

                    // have we already created the queue?
                    if (preExportSubscriptions != null) {
                        preExportSubscriptions.add(subscriptionRequest);
                        return;
                    }

                    if (subscriptionRequest.ticketVector() == null) {
                        GrpcUtil.safelyError(listener, Code.INVALID_ARGUMENT, "Ticket not specified.");
                        return;
                    }

                    preExportSubscriptions = new ArrayDeque<>();
                    preExportSubscriptions.add(subscriptionRequest);
                    final SessionState.ExportObject<Object> parent =
                            ticketRouter.resolve(session, subscriptionRequest.ticketAsByteBuffer(), "ticket");

                    onExportResolvedContinuation = session.nonExport()
                            .require(parent)
                            .onError(listener)
                            .submit(() -> onExportResolved(parent));
                }
            }

            private synchronized void onExportResolved(final SessionState.ExportObject<Object> parent) {
                onExportResolvedContinuation = null;

                if (isClosed) {
                    preExportSubscriptions = null;
                    return;
                }

                // we know there is at least one request; it was put there when we knew which parent to wait on
                final BarrageSubscriptionRequest subscriptionRequest = preExportSubscriptions.remove();

                final Object export = parent.get();
                if (export instanceof QueryTable) {
                    final QueryTable table = (QueryTable) export;
                    final io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions options =
                            subscriptionRequest.subscriptionOptions();
                    long minUpdateIntervalMs = options == null ? 0 : options.minUpdateIntervalMs();
                    if (minUpdateIntervalMs == 0) {
                        minUpdateIntervalMs = DEFAULT_MIN_UPDATE_INTERVAL_MS;
                    }
                    bmp = table.getResult(operationFactory.create(table, minUpdateIntervalMs));
                    if (bmp.isRefreshing()) {
                        manage(bmp);
                    }
                } else {
                    GrpcUtil.safelyError(listener, Code.FAILED_PRECONDITION, "Ticket ("
                            + ExportTicketHelper.toReadableString(subscriptionRequest.ticketAsByteBuffer(), "ticket")
                            + ") is not a subscribable table.");
                    return;
                }

                log.info().append(myPrefix).append("processing initial subscription").endl();

                final boolean hasColumns = subscriptionRequest.columnsVector() != null;
                final BitSet columns =
                        hasColumns ? BitSet.valueOf(subscriptionRequest.columnsAsByteBuffer()) : null;

                final boolean hasViewport = subscriptionRequest.viewportVector() != null;
                final RowSet viewport =
                        hasViewport ? BarrageProtoUtil.toRowSet(subscriptionRequest.viewportAsByteBuffer()) : null;

                final boolean reverseViewport = subscriptionRequest.reverseViewport();

                bmp.addSubscription(listener, subscriptionOptAdapter.adapt(subscriptionRequest), columns, viewport,
                        reverseViewport);

                for (final BarrageSubscriptionRequest request : preExportSubscriptions) {
                    apply(request);
                }

                // we will now process requests as they are received
                preExportSubscriptions = null;
            }

            /**
             * Update the existing subscription to match the new request.
             *
             * @param subscriptionRequest the requested view change
             */
            private void apply(final BarrageSubscriptionRequest subscriptionRequest) {
                final boolean hasColumns = subscriptionRequest.columnsVector() != null;
                final BitSet columns =
                        hasColumns ? BitSet.valueOf(subscriptionRequest.columnsAsByteBuffer()) : null;

                final boolean hasViewport = subscriptionRequest.viewportVector() != null;
                final RowSet viewport =
                        hasViewport ? BarrageProtoUtil.toRowSet(subscriptionRequest.viewportAsByteBuffer()) : null;

                final boolean reverseViewport = subscriptionRequest.reverseViewport();


                final boolean subscriptionFound = bmp.updateSubscription(listener, viewport, columns, reverseViewport);

                if (!subscriptionFound) {
                    throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND, "Subscription was not found.");
                }
            }

            @Override
            public synchronized void close() {
                if (onExportResolvedContinuation != null) {
                    onExportResolvedContinuation.cancel();
                    onExportResolvedContinuation = null;
                }

                if (bmp != null) {
                    bmp.removeSubscription(listener);
                    bmp = null;
                } else {
                    GrpcUtil.safelyExecuteLocked(listener, listener::onCompleted);
                }

                if (preExportSubscriptions != null) {
                    preExportSubscriptions = null;
                }
            }
        }
    }
}
