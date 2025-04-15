//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.google.rpc.Code;
import dagger.assisted.Assisted;
import dagger.assisted.AssistedFactory;
import dagger.assisted.AssistedInject;
import gnu.trove.map.hash.TByteObjectHashMap;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.engine.liveness.SingletonLivenessManager;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.BaseTable;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
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
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flatbuf.MessageHeader;
import org.apache.arrow.flatbuf.Schema;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class ArrowFlightUtil {
    private static final Logger log = LoggerFactory.getLogger(ArrowFlightUtil.class);

    private static class MessageViewAdapter implements StreamObserver<BarrageMessageWriter.MessageView> {
        private final StreamObserver<InputStream> delegate;

        private MessageViewAdapter(final StreamObserver<InputStream> delegate) {
            this.delegate = delegate;
        }

        public void onNext(final BarrageMessageWriter.MessageView value) {
            synchronized (delegate) {
                try {
                    value.forEachStream(delegate::onNext);
                } catch (IOException e) {
                    throw new UncheckedDeephavenException(e);
                }
            }
        }

        @Override
        public void onError(final Throwable t) {
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

    public static void DoGetCustom(
            final BarrageMessageWriter.Factory streamGeneratorFactory,
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
                        final StreamObserver<BarrageMessageWriter.MessageView> listener =
                                new MessageViewAdapter(observer);

                        // push the schema to the listener
                        listener.onNext(streamGeneratorFactory.getSchemaView(
                                fbb -> BarrageUtil.makeTableSchemaPayload(fbb, BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS,
                                        table.getDefinition(), table.getAttributes(), table.isFlat())));

                        // shared code between `DoGet` and `BarrageSnapshotRequest`
                        BarrageUtil.createAndSendSnapshot(streamGeneratorFactory, table, null, null, false,
                                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, listener, metrics);
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
        private Schema schema;

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
                schema = parseArrowSchema(mi);
                return;
            }

            if (resultTable == null && schema != null) {
                configureWithSchema(schema);
            }
            if (resultTable == null) {
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Schema must be processed before record-batch messages");
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
        public void onError(final Throwable t) {
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
            if (resultTable == null && schema != null) {
                configureWithSchema(schema);
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

        private final StreamObserver<BarrageMessageWriter.MessageView> listener;

        private boolean isClosed = false;

        private boolean isFirstMsg = true;

        private final TicketRouter ticketRouter;
        private final BarrageMessageWriter.Factory streamGeneratorFactory;
        private final SessionService.ErrorTransformer errorTransformer;


        /**
         * A map from the wire request type to the handler factory for a DoExchange request.
         */
        private final TByteObjectHashMap<ExchangeRequestHandlerFactory> requestHandlerFactories;


        /**
         * This is the set of marshallers that are consulted for each exported object when handling a DoExchange. The
         * marshallers are processed in order, with the first marshaller to claim an object the one responsible.
         */
        private final List<ExchangeMarshaller> marshallers;

        /**
         * Interface for the individual handlers for the DoExchange.
         */
        public interface Handler extends Closeable {
            void handleMessage(@NotNull MessageInfo message);
        }

        private Handler requestHandler = null;

        @AssistedInject
        public DoExchangeMarshaller(
                final TicketRouter ticketRouter,
                final BarrageMessageWriter.Factory streamGeneratorFactory,
                final List<ExchangeMarshaller> exchangeMarshallers,
                final Set<ExchangeRequestHandlerFactory> requestHandlerFactories,
                final SessionService.ErrorTransformer errorTransformer,
                @Assisted final SessionState session,
                @Assisted final StreamObserver<InputStream> responseObserver) {

            this.myPrefix = "DoExchangeMarshaller{" + Integer.toHexString(System.identityHashCode(this)) + "}: ";
            this.ticketRouter = ticketRouter;
            this.streamGeneratorFactory = streamGeneratorFactory;
            this.session = session;
            this.listener = new MessageViewAdapter(responseObserver);
            this.errorTransformer = errorTransformer;
            this.marshallers = exchangeMarshallers;

            this.requestHandlerFactories = new TByteObjectHashMap<>(requestHandlerFactories.size());
            for (final ExchangeRequestHandlerFactory factory : requestHandlerFactories) {
                final ExchangeRequestHandlerFactory old = this.requestHandlerFactories.put(factory.type(), factory);
                if (old != null) {
                    throw new IllegalStateException("Cannot have multiple registered factories for type "
                            + factory.type() + ", existing=" + old + ", new=" + factory);
                }
            }

            this.session.addOnCloseCallback(this);
            if (responseObserver instanceof ServerCallStreamObserver) {
                ((ServerCallStreamObserver<InputStream>) responseObserver).setOnCancelHandler(this::onCancel);
            }
        }

        // this entry is used for client-streaming requests
        @Override
        public void onNext(final InputStream request) {
            final MessageInfo message;
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
                    final byte type = message.app_metadata.msgType();
                    final ExchangeRequestHandlerFactory factory = requestHandlerFactories.get(type);
                    if (factory == null) {
                        throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                                myPrefix + "received a message with unhandled BarrageMessageType: " + type);
                    }
                    requestHandler = factory.create(this, listener);
                    if (requestHandler == null) {
                        throw new IllegalStateException(
                                "ExchangeRequestHandlerFactory returned null for message of type " + type);
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

        public List<ExchangeMarshaller> getMarshallers() {
            return marshallers;
        }

        public boolean isClosed() {
            return isClosed;
        }

        public BarrageMessageWriter.Factory getStreamGeneratorFactory() {
            return streamGeneratorFactory;
        }

        public TicketRouter getTicketRouter() {
            return ticketRouter;
        }

        public SessionState getSession() {
            return session;
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

    }
}
