/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.arrow;

import com.google.rpc.Code;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageStreamGenerator;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.ArrowToTableConverter;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil.MessageInfo;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.extensions.barrage.BarrageStreamGeneratorImpl;
import io.deephaven.server.hierarchicaltable.HierarchicalTableView;
import io.deephaven.server.hierarchicaltable.HierarchicalTableViewSubscription;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

import static io.deephaven.extensions.barrage.util.BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS;

public class ArrowFlightUtil {
    private static final Logger log = LoggerFactory.getLogger(ArrowFlightUtil.class);

    public static final int DEFAULT_MIN_UPDATE_INTERVAL_MS =
            Configuration.getInstance().getIntegerWithDefault("barrage.minUpdateInterval", 1000);

    public static void DoGetCustom(
            final BarrageStreamGenerator.Factory<BarrageStreamGeneratorImpl.View> streamGeneratorFactory,
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
                    final BaseTable<?> table = export.get();
                    metrics.tableId = Integer.toHexString(System.identityHashCode(table));
                    metrics.tableKey = BarragePerformanceLog.getKeyFor(table);

                    // create an adapter for the response observer
                    final StreamObserver<BarrageStreamGeneratorImpl.View> listener =
                            ArrowModule.provideListenerAdapter().adapt(observer);

                    // push the schema to the listener
                    listener.onNext(streamGeneratorFactory.getSchemaView(
                            fbb -> BarrageUtil.makeTableSchemaPayload(fbb, table.getDefinition(), table.getAttributes())));

                    // shared code between `DoGet` and `BarrageSnapshotRequest`
                    BarrageUtil.createAndSendSnapshot(streamGeneratorFactory, table, null, null, false,
                            DEFAULT_SNAPSHOT_DESER_OPTIONS, listener, metrics);

                    listener.onCompleted();
                });
    }

    /**
     * This is a stateful observer; a DoPut stream begins with its schema.
     */
    public static class DoPutObserver extends ArrowToTableConverter implements StreamObserver<InputStream>, Closeable {

        private final SessionState session;
        private final TicketRouter ticketRouter;
        private final StreamObserver<Flight.PutResult> observer;

        private SessionState.ExportBuilder<Table> resultExportBuilder;
        private Flight.FlightDescriptor flightDescriptor;

        public DoPutObserver(
                final SessionState session,
                final TicketRouter ticketRouter,
                final StreamObserver<Flight.PutResult> observer) {
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
                final MessageInfo mi = BarrageProtoUtil.parseProtoMessage(request);
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
                final BarrageMessage msg = createBarrageMessage(mi, numColumns);
                msg.rowsAdded = RowSetFactory.fromRange(totalRowsRead, totalRowsRead + msg.length - 1);
                msg.rowsIncluded = msg.rowsAdded.copy();
                msg.modColumnData = BarrageMessage.ZERO_MOD_COLUMNS;
                totalRowsRead += msg.length;
                resultTable.handleBarrageMessage(msg);

                // no app_metadata to report; but ack the processing
                GrpcUtil.safelyOnNext(observer, Flight.PutResult.getDefaultInstance());
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

        @Override
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
                    GrpcUtil.safelyComplete(observer);
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

        private final StreamObserver<BarrageStreamGeneratorImpl.View> listener;

        private boolean isClosed = false;

        private boolean isFirstMsg = true;

        private final TicketRouter ticketRouter;
        private final BarrageStreamGenerator.Factory<BarrageStreamGeneratorImpl.View> streamGeneratorFactory;
        private final BarrageMessageProducer.Operation.Factory<BarrageStreamGeneratorImpl.View> bmpOperationFactory;
        private final HierarchicalTableViewSubscription.Factory htvsFactory;
        private final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, BarrageSubscriptionOptions> subscriptionOptAdapter;
        private final BarrageMessageProducer.Adapter<BarrageSnapshotRequest, BarrageSnapshotOptions> snapshotOptAdapter;

        /**
         * Interface for the individual handlers for the DoExchange.
         */
        interface Handler extends Closeable {
            void handleMessage(@NotNull MessageInfo message);
        }

        private Handler requestHandler = null;

        @AssistedInject
        public DoExchangeMarshaller(
                final TicketRouter ticketRouter,
                final BarrageStreamGenerator.Factory<BarrageStreamGeneratorImpl.View> streamGeneratorFactory,
                final BarrageMessageProducer.Operation.Factory<BarrageStreamGeneratorImpl.View> bmpOperationFactory,
                final HierarchicalTableViewSubscription.Factory htvsFactory,
                final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<BarrageStreamGeneratorImpl.View>> listenerAdapter,
                final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, BarrageSubscriptionOptions> subscriptionOptAdapter,
                final BarrageMessageProducer.Adapter<BarrageSnapshotRequest, BarrageSnapshotOptions> snapshotOptAdapter,
                @Assisted final SessionState session,
                @Assisted final StreamObserver<InputStream> responseObserver) {

            this.myPrefix = "DoExchangeMarshaller{" + Integer.toHexString(System.identityHashCode(this)) + "}: ";
            this.ticketRouter = ticketRouter;
            this.streamGeneratorFactory = streamGeneratorFactory;
            this.bmpOperationFactory = bmpOperationFactory;
            this.htvsFactory = htvsFactory;
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
                MessageInfo message = BarrageProtoUtil.parseProtoMessage(request);
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
            public void handleMessage(@NotNull final BarrageProtoUtil.MessageInfo message) {
                // verify this is the correct type of message for this handler
                if (message.app_metadata.msgType() != BarrageMessageType.BarrageSnapshotRequest) {
                    throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Request type cannot be changed after initialization, expected BarrageSnapshotRequest metadata");
                }

                // ensure synchronization with parent class functions
                synchronized (DoExchangeMarshaller.this) {
                    final BarrageSnapshotRequest snapshotRequest = BarrageSnapshotRequest
                            .getRootAsBarrageSnapshotRequest(message.app_metadata.msgPayloadAsByteBuffer());

                    final SessionState.ExportObject<BaseTable<?>> parent =
                            ticketRouter.resolve(session, snapshotRequest.ticketAsByteBuffer(), "ticket");

                    final BarragePerformanceLog.SnapshotMetricsHelper metrics =
                            new BarragePerformanceLog.SnapshotMetricsHelper();

                    final long queueStartTm = System.nanoTime();
                    session.nonExport()
                            .require(parent)
                            .onError(listener)
                            .submit(() -> {
                                metrics.queueNanos = System.nanoTime() - queueStartTm;
                                final BaseTable<?> table = parent.get();
                                metrics.tableId = Integer.toHexString(System.identityHashCode(table));
                                metrics.tableKey = BarragePerformanceLog.getKeyFor(table);

                                // push the schema to the listener
                                listener.onNext(streamGeneratorFactory.getSchemaView(
                                        fbb -> BarrageUtil.makeTableSchemaPayload(fbb,
                                                table.getDefinition(), table.getAttributes())));

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
                                BarrageUtil.createAndSendSnapshot(streamGeneratorFactory, table, columns, viewport,
                                        reverseViewport, snapshotOptAdapter.adapt(snapshotRequest), listener, metrics);
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

            private BarrageMessageProducer<BarrageStreamGeneratorImpl.View> bmp;
            private HierarchicalTableViewSubscription htvs;

            private Queue<BarrageSubscriptionRequest> preExportSubscriptions;
            private SessionState.ExportObject<?> onExportResolvedContinuation;

            public SubscriptionRequestHandler() {}

            @Override
            public void handleMessage(@NotNull final MessageInfo message) {
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

                    if (bmp != null || htvs != null) {
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

                final io.deephaven.barrage.flatbuf.BarrageSubscriptionOptions options =
                        subscriptionRequest.subscriptionOptions();
                long minUpdateIntervalMs = options == null ? 0 : options.minUpdateIntervalMs();
                if (minUpdateIntervalMs == 0) {
                    minUpdateIntervalMs = DEFAULT_MIN_UPDATE_INTERVAL_MS;
                }

                final Object export = parent.get();
                if (export instanceof QueryTable) {
                    final QueryTable table = (QueryTable) export;
                    bmp = table.getResult(bmpOperationFactory.create(table, minUpdateIntervalMs));
                    if (bmp.isRefreshing()) {
                        manage(bmp);
                    }
                } else if (export instanceof HierarchicalTableView) {
                    final HierarchicalTableView hierarchicalTableView = (HierarchicalTableView) export;
                    htvs = htvsFactory.create(hierarchicalTableView, listener,
                            subscriptionOptAdapter.adapt(subscriptionRequest), minUpdateIntervalMs);
                    if (hierarchicalTableView.getHierarchicalTable().getSource().isRefreshing()) {
                        manage(htvs);
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

                if (bmp != null) {
                    bmp.addSubscription(listener, subscriptionOptAdapter.adapt(subscriptionRequest), columns, viewport,
                            reverseViewport);
                } else if (htvs != null) {
                    htvs.setViewport(columns, viewport, reverseViewport);
                } else {
                    // noinspection ThrowableNotThrown
                    Assert.statementNeverExecuted();
                }

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

                final boolean subscriptionFound;
                if (bmp != null) {
                    subscriptionFound = bmp.updateSubscription(listener, viewport, columns, reverseViewport);
                } else if (htvs != null) {
                    subscriptionFound = true;
                    htvs.setViewport(columns, viewport, reverseViewport);
                } else {
                    subscriptionFound = false;
                }

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
                } else if (htvs != null) {
                    htvs.completed();
                    htvs = null;
                } else {
                    GrpcUtil.safelyComplete(listener);
                }

                if (preExportSubscriptions != null) {
                    preExportSubscriptions = null;
                }
            }
        }
    }
}
