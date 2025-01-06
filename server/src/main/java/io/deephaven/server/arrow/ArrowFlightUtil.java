//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.updategraph.UpdateGraph;
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
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.barrage.BarrageMessageProducer;
import io.deephaven.server.hierarchicaltable.HierarchicalTableView;
import io.deephaven.server.hierarchicaltable.HierarchicalTableViewSubscription;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static io.deephaven.extensions.barrage.util.BarrageUtil.DEFAULT_SNAPSHOT_DESER_OPTIONS;

public class ArrowFlightUtil {
    private static final Logger log = LoggerFactory.getLogger(ArrowFlightUtil.class);

    private static class MessageViewAdapter implements StreamObserver<BarrageStreamGenerator.MessageView> {
        private final StreamObserver<InputStream> delegate;

        private MessageViewAdapter(StreamObserver<InputStream> delegate) {
            this.delegate = delegate;
        }

        public void onNext(BarrageStreamGenerator.MessageView value) {
            synchronized (delegate) {
                try {
                    value.forEachStream(delegate::onNext);
                } catch (IOException e) {
                    throw new UncheckedDeephavenException(e);
                }
            }
        }

        @Override
        public void onError(Throwable t) {
            synchronized (delegate) {
                delegate.onError(t);
            }
        }

        @Override
        public void onCompleted() {
            synchronized (delegate) {
                delegate.onCompleted();
            }
        }
    }

    public static final int DEFAULT_MIN_UPDATE_INTERVAL_MS =
            Configuration.getInstance().getIntegerWithDefault("barrage.minUpdateInterval", 1000);

    public static void DoGetCustom(
            final BarrageStreamGenerator.Factory streamGeneratorFactory,
            final SessionState session,
            final TicketRouter ticketRouter,
            final Flight.Ticket request,
            final StreamObserver<InputStream> observer) {

        final String ticketLogName = ticketRouter.getLogNameFor(request, "table");
        final String description = "FlightService#DoGet(table=" + ticketLogName + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<?> tableExport =
                    ticketRouter.resolve(session, request, "table");

            final BarragePerformanceLog.SnapshotMetricsHelper metrics =
                    new BarragePerformanceLog.SnapshotMetricsHelper();

            final long queueStartTm = System.nanoTime();
            session.nonExport()
                    .queryPerformanceRecorder(queryPerformanceRecorder)
                    .require(tableExport)
                    .onError(observer)
                    .onSuccess(observer)
                    .submit(() -> {
                        metrics.queueNanos = System.nanoTime() - queueStartTm;
                        Object export = tableExport.get();
                        if (export instanceof Table) {
                            export = ((Table) export).coalesce();
                        }
                        if (!(export instanceof BaseTable)) {
                            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Ticket ("
                                    + ticketLogName + ") is not a subscribable table.");
                        }
                        final BaseTable<?> table = (BaseTable<?>) export;
                        metrics.tableId = Integer.toHexString(System.identityHashCode(table));
                        metrics.tableKey = BarragePerformanceLog.getKeyFor(table);

                        // create an adapter for the response observer
                        final StreamObserver<BarrageStreamGenerator.MessageView> listener =
                                new MessageViewAdapter(observer);

                        // push the schema to the listener
                        listener.onNext(streamGeneratorFactory.getSchemaView(
                                fbb -> BarrageUtil.makeTableSchemaPayload(fbb, DEFAULT_SNAPSHOT_DESER_OPTIONS,
                                        table.getDefinition(), table.getAttributes(), table.isFlat())));

                        // shared code between `DoGet` and `BarrageSnapshotRequest`
                        BarrageUtil.createAndSendSnapshot(streamGeneratorFactory, table, null, null, false,
                                DEFAULT_SNAPSHOT_DESER_OPTIONS, listener, metrics);
                    });
        }
    }

    /**
     * This is a stateful observer; a DoPut stream begins with its schema.
     */
    public static class DoPutObserver extends ArrowToTableConverter implements StreamObserver<InputStream>, Closeable {

        private final SessionState session;
        private final TicketRouter ticketRouter;
        private final SessionService.ErrorTransformer errorTransformer;
        private final StreamObserver<Flight.PutResult> observer;

        private SessionState.ExportBuilder<Table> resultExportBuilder;
        private Flight.FlightDescriptor flightDescriptor;

        public DoPutObserver(
                final SessionState session,
                final TicketRouter ticketRouter,
                final SessionService.ErrorTransformer errorTransformer,
                final StreamObserver<Flight.PutResult> observer) {
            this.session = session;
            this.ticketRouter = ticketRouter;
            this.errorTransformer = errorTransformer;
            this.observer = observer;

            this.session.addOnCloseCallback(this);
            if (observer instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver<Flight.PutResult>) observer).setOnCancelHandler(this::onCancel);
            }
        }

        // this is the entry point for client-streams
        @Override
        public void onNext(final InputStream request) {
            final MessageInfo mi;
            try {
                mi = BarrageProtoUtil.parseProtoMessage(request);
            } catch (final IOException err) {
                throw errorTransformer.transform(err);
            }

            if (mi.descriptor != null) {
                if (flightDescriptor != null) {
                    if (!flightDescriptor.equals(mi.descriptor)) {
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                "additional flight descriptor sent does not match original descriptor");
                    }
                } else {
                    flightDescriptor = mi.descriptor;
                    resultExportBuilder = ticketRouter
                            .<Table>publish(session, mi.descriptor, "Flight.Descriptor", null)
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
                configureWithSchema(parseArrowSchema(mi));
                return;
            }

            if (mi.header.headerType() != MessageHeader.RecordBatch) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Only schema/record-batch messages supported, instead got "
                                + MessageHeader.name(mi.header.headerType()));
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
        }

        private void onCancel() {
            if (resultTable != null) {
                resultTable.dropReference();
                resultTable = null;
            }
            if (resultExportBuilder != null) {
                // this thrown error propagates to observer
                resultExportBuilder.submit(() -> {
                    throw Exceptions.statusRuntimeException(Code.CANCELLED, "cancelled");
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
            if (resultExportBuilder == null) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Result flight descriptor never provided");
            }
            if (resultTable == null) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Result flight schema never provided");
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

            // let's finally export the table to our destination export
            localExportBuilder
                    .onSuccess(() -> GrpcUtil.safelyComplete(observer))
                    .submit(() -> {
                        session.removeOnCloseCallback(this);
                        return localResultTable;
                    });
        }

        @Override
        public void close() {
            // close() is intended to be invoked only though session expiration
            GrpcUtil.safelyError(observer, Code.UNAUTHENTICATED, "Session expired");
        }
    }

    /**
     * Represents states for a DoExchange stream where the server must not close until the client has half closed.
     */
    enum HalfClosedState {
        /**
         * Client has not half-closed, server should not half close until the client has done so.
         */
        DONT_CLOSE,
        /**
         * Indicates that the client has half-closed, and the server should half close immediately after finishing
         * sending data.
         */
        CLIENT_HALF_CLOSED,

        /**
         * The server has no more data to send, but client hasn't half-closed.
         */
        FINISHED_SENDING,

        /**
         * Streaming finished and client half-closed.
         */
        CLOSED
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

        private final StreamObserver<BarrageStreamGenerator.MessageView> listener;

        private boolean isClosed = false;

        private boolean isFirstMsg = true;

        private final TicketRouter ticketRouter;
        private final BarrageStreamGenerator.Factory streamGeneratorFactory;
        private final BarrageMessageProducer.Operation.Factory bmpOperationFactory;
        private final HierarchicalTableViewSubscription.Factory htvsFactory;
        private final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, BarrageSubscriptionOptions> subscriptionOptAdapter;
        private final BarrageMessageProducer.Adapter<BarrageSnapshotRequest, BarrageSnapshotOptions> snapshotOptAdapter;
        private final SessionService.ErrorTransformer errorTransformer;

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
                final BarrageStreamGenerator.Factory streamGeneratorFactory,
                final BarrageMessageProducer.Operation.Factory bmpOperationFactory,
                final HierarchicalTableViewSubscription.Factory htvsFactory,
                final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, BarrageSubscriptionOptions> subscriptionOptAdapter,
                final BarrageMessageProducer.Adapter<BarrageSnapshotRequest, BarrageSnapshotOptions> snapshotOptAdapter,
                final SessionService.ErrorTransformer errorTransformer,
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
            this.listener = new MessageViewAdapter(responseObserver);
            this.errorTransformer = errorTransformer;

            this.session.addOnCloseCallback(this);
            if (responseObserver instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver<InputStream>) responseObserver).setOnCancelHandler(this::onCancel);
            }
        }

        // this entry is used for client-streaming requests
        @Override
        public void onNext(final InputStream request) {
            MessageInfo message;
            try {
                message = BarrageProtoUtil.parseProtoMessage(request);
            } catch (final IOException err) {
                throw errorTransformer.transform(err);
            }
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
                            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                    myPrefix + "received a message with unhandled BarrageMessageType");
                    }
                    requestHandler.handleMessage(message);
                    return;
                }

                // handle the possible error cases
                if (!isFirstMsg) {
                    // only the first messages is allowed to have null metadata
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            myPrefix + "failed to receive Barrage request metadata");
                }

                isFirstMsg = false;
            }
        }

        public void onCancel() {
            log.debug().append(myPrefix).append("cancel requested").endl();
            tryClose();
        }

        @Override
        public void onError(final Throwable t) {
            GrpcUtil.safelyError(listener, errorTransformer.transform(t));
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
            } catch (final IOException err) {
                throw errorTransformer.transform(err);
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
            private final AtomicReference<HalfClosedState> halfClosedState =
                    new AtomicReference<>(HalfClosedState.DONT_CLOSE);

            public SnapshotRequestHandler() {}

            @Override
            public void handleMessage(@NotNull final BarrageProtoUtil.MessageInfo message) {
                // verify this is the correct type of message for this handler
                if (message.app_metadata.msgType() != BarrageMessageType.BarrageSnapshotRequest) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Request type cannot be changed after initialization, expected BarrageSnapshotRequest metadata");
                }

                // ensure synchronization with parent class functions
                synchronized (DoExchangeMarshaller.this) {
                    final BarrageSnapshotRequest snapshotRequest = BarrageSnapshotRequest
                            .getRootAsBarrageSnapshotRequest(message.app_metadata.msgPayloadAsByteBuffer());

                    final String ticketLogName =
                            ticketRouter.getLogNameFor(snapshotRequest.ticketAsByteBuffer(), "table");
                    final String description = "FlightService#DoExchange(snapshot, table=" + ticketLogName + ")";
                    final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                            description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

                    try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
                        final SessionState.ExportObject<?> tableExport =
                                ticketRouter.resolve(session, snapshotRequest.ticketAsByteBuffer(), "table");

                        final BarragePerformanceLog.SnapshotMetricsHelper metrics =
                                new BarragePerformanceLog.SnapshotMetricsHelper();

                        final long queueStartTm = System.nanoTime();
                        session.nonExport()
                                .queryPerformanceRecorder(queryPerformanceRecorder)
                                .require(tableExport)
                                .onError(listener)
                                .onSuccess(() -> {
                                    final HalfClosedState newState = halfClosedState.updateAndGet(current -> {
                                        switch (current) {
                                            case DONT_CLOSE:
                                                // record that we have finished sending
                                                return HalfClosedState.FINISHED_SENDING;
                                            case CLIENT_HALF_CLOSED:
                                                // since streaming has now finished, and client already half-closed,
                                                // time to half close from server
                                                return HalfClosedState.CLOSED;
                                            case FINISHED_SENDING:
                                            case CLOSED:
                                                throw new IllegalStateException("Can't finish streaming twice");
                                            default:
                                                throw new IllegalStateException("Unknown state " + current);
                                        }
                                    });
                                    if (newState == HalfClosedState.CLOSED) {
                                        GrpcUtil.safelyComplete(listener);
                                    }
                                })
                                .submit(() -> {
                                    metrics.queueNanos = System.nanoTime() - queueStartTm;
                                    Object export = tableExport.get();
                                    if (export instanceof Table) {
                                        export = ((Table) export).coalesce();
                                    }
                                    if (!(export instanceof BaseTable)) {
                                        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Ticket ("
                                                + ticketLogName + ") is not a subscribable table.");
                                    }
                                    final BaseTable<?> table = (BaseTable<?>) export;
                                    metrics.tableId = Integer.toHexString(System.identityHashCode(table));
                                    metrics.tableKey = BarragePerformanceLog.getKeyFor(table);

                                    if (table.isFailed()) {
                                        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                                                "Table is already failed");
                                    }

                                    // push the schema to the listener
                                    listener.onNext(streamGeneratorFactory.getSchemaView(
                                            fbb -> BarrageUtil.makeTableSchemaPayload(fbb,
                                                    snapshotOptAdapter.adapt(snapshotRequest),
                                                    table.getDefinition(), table.getAttributes(), table.isFlat())));

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
                                            reverseViewport, snapshotOptAdapter.adapt(snapshotRequest), listener,
                                            metrics);
                                });
                    }
                }
            }

            @Override
            public void close() {
                // no work to do for DoGetRequest close
                // possibly safely complete if finished sending data
                final HalfClosedState newState = halfClosedState.updateAndGet(current -> {
                    switch (current) {
                        case DONT_CLOSE:
                            // record that we have half closed
                            return HalfClosedState.CLIENT_HALF_CLOSED;
                        case FINISHED_SENDING:
                            // since client has now half closed, and we're done sending, time to half-close from server
                            return HalfClosedState.CLOSED;
                        case CLIENT_HALF_CLOSED:
                        case CLOSED:
                            throw new IllegalStateException("Can't close twice");
                        default:
                            throw new IllegalStateException("Unknown state " + current);
                    }
                });
                if (newState == HalfClosedState.CLOSED) {
                    GrpcUtil.safelyComplete(listener);
                }
            }
        }

        /**
         * Handler for BarrageSubscriptionRequest over DoExchange.
         */
        private class SubscriptionRequestHandler
                implements Handler {

            private BarrageMessageProducer bmp;
            private HierarchicalTableViewSubscription htvs;

            private Queue<BarrageSubscriptionRequest> preExportSubscriptions;
            private SessionState.ExportObject<?> onExportResolvedContinuation;

            public SubscriptionRequestHandler() {}

            @Override
            public void handleMessage(@NotNull final MessageInfo message) {
                // verify this is the correct type of message for this handler
                if (message.app_metadata.msgType() != BarrageMessageType.BarrageSubscriptionRequest) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "Request type cannot be changed after initialization, expected BarrageSubscriptionRequest metadata");
                }

                if (message.app_metadata.msgPayloadVector() == null) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Subscription request not supplied");
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

                    final String description = "FlightService#DoExchange(subscription, table="
                            + ticketRouter.getLogNameFor(subscriptionRequest.ticketAsByteBuffer(), "table") + ")";
                    final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                            description, session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

                    try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
                        final SessionState.ExportObject<Object> table =
                                ticketRouter.resolve(session, subscriptionRequest.ticketAsByteBuffer(), "table");

                        synchronized (this) {
                            onExportResolvedContinuation = session.nonExport()
                                    .queryPerformanceRecorder(queryPerformanceRecorder)
                                    .require(table)
                                    .onErrorHandler(DoExchangeMarshaller.this::onError)
                                    .submit(() -> onExportResolved(table));
                        }
                    }
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
                final long minUpdateIntervalMs;
                if (options == null || options.minUpdateIntervalMs() == 0) {
                    minUpdateIntervalMs = DEFAULT_MIN_UPDATE_INTERVAL_MS;
                } else {
                    minUpdateIntervalMs = options.minUpdateIntervalMs();
                }

                Object export = parent.get();
                if (export instanceof Table) {
                    final QueryTable table = (QueryTable) ((Table) export).coalesce();

                    if (table.isFailed()) {
                        throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION,
                                "Table is already failed");
                    }

                    final UpdateGraph ug = table.getUpdateGraph();
                    try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(ug).open()) {
                        bmp = table.getResult(bmpOperationFactory.create(table, minUpdateIntervalMs));
                        if (bmp.isRefreshing()) {
                            manage(bmp);
                        }
                    }
                } else if (export instanceof HierarchicalTableView) {
                    final HierarchicalTableView hierarchicalTableView = (HierarchicalTableView) export;
                    final UpdateGraph ug = hierarchicalTableView.getHierarchicalTable().getSource().getUpdateGraph();
                    try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(ug).open()) {
                        htvs = htvsFactory.create(hierarchicalTableView, listener,
                                subscriptionOptAdapter.adapt(subscriptionRequest), minUpdateIntervalMs);
                        if (hierarchicalTableView.getHierarchicalTable().getSource().isRefreshing()) {
                            manage(htvs);
                        }
                    }
                } else {
                    GrpcUtil.safelyError(listener, Code.FAILED_PRECONDITION, "Ticket ("
                            + ExportTicketHelper.toReadableString(subscriptionRequest.ticketAsByteBuffer(), "ticket")
                            + ") is not a subscribable table.");
                    return;
                }

                log.debug().append(myPrefix).append("processing initial subscription").endl();

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
                    throw Exceptions.statusRuntimeException(Code.NOT_FOUND, "Subscription was not found.");
                }
            }

            @Override
            public synchronized void close() {
                if (bmp != null) {
                    bmp.removeSubscription(listener);
                    bmp = null;
                } else if (htvs != null) {
                    htvs.completed();
                    htvs = null;
                } else {
                    GrpcUtil.safelyComplete(listener);
                }

                // After we've signaled that the stream should close, cancel the export
                if (onExportResolvedContinuation != null) {
                    onExportResolvedContinuation.cancel();
                    onExportResolvedContinuation = null;
                }

                if (preExportSubscriptions != null) {
                    preExportSubscriptions = null;
                }
            }
        }
    }
}
