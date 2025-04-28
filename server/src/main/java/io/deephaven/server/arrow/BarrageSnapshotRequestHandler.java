//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.google.auto.service.AutoService;
import com.google.rpc.Code;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Handler for DoGetRequest over DoExchange.
 */
@ReflexiveUse(referrers = "ArrowFlightUtil")
public class BarrageSnapshotRequestHandler implements ArrowFlightUtil.DoExchangeMarshaller.Handler {
    private final AtomicReference<ArrowFlightUtil.HalfClosedState> halfClosedState =
            new AtomicReference<>(ArrowFlightUtil.HalfClosedState.DONT_CLOSE);
    private final ArrowFlightUtil.DoExchangeMarshaller marshaller;
    private final TicketRouter ticketRouter;
    private final SessionState session;
    private final StreamObserver<BarrageMessageWriter.MessageView> listener;
    private final BarrageMessageWriter.Factory streamGeneratorFactory;

    public BarrageSnapshotRequestHandler(final ArrowFlightUtil.DoExchangeMarshaller marshaller,
            final TicketRouter ticketRouter,
            final SessionState session,
            final BarrageMessageWriter.Factory streamGeneratorFactory,
            final StreamObserver<BarrageMessageWriter.MessageView> listener) {
        this.marshaller = marshaller;
        this.ticketRouter = ticketRouter;
        this.session = session;
        this.listener = listener;
        this.streamGeneratorFactory = streamGeneratorFactory;
    }

    @Override
    public void handleMessage(@NotNull final BarrageProtoUtil.MessageInfo message) {
        validateMessage(message);

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
                        final ArrowFlightUtil.HalfClosedState newState = halfClosedState.updateAndGet(current -> {
                            switch (current) {
                                case DONT_CLOSE:
                                    // record that we have finished sending
                                    return ArrowFlightUtil.HalfClosedState.FINISHED_SENDING;
                                case CLIENT_HALF_CLOSED:
                                    // since streaming has now finished, and client already half-closed,
                                    // time to half close from server
                                    return ArrowFlightUtil.HalfClosedState.CLOSED;
                                case FINISHED_SENDING:
                                case CLOSED:
                                    throw new IllegalStateException("Can't finish streaming twice");
                                default:
                                    throw new IllegalStateException("Unknown state " + current);
                            }
                        });
                        if (newState == ArrowFlightUtil.HalfClosedState.CLOSED) {
                            GrpcUtil.safelyComplete(listener);
                        }
                    })
                    .submit(() -> {
                        metrics.queueNanos = System.nanoTime() - queueStartTm;
                        final Object export = tableExport.get();

                        final ExchangeMarshaller marshallerForExport =
                                ExchangeMarshaller.getMarshaller(export, marshaller.getMarshallers());
                        if (marshallerForExport == null) {
                            throw Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Ticket ("
                                    + ticketLogName + ") is has no associated exchange marshaller.");
                        }

                        final BarrageSnapshotOptions options = BarrageSnapshotOptions.of(snapshotRequest);

                        marshallerForExport.snapshot(snapshotRequest, options, export, metrics,
                                listener, ticketLogName, streamGeneratorFactory);
                    });
        }
    }

    /**
     * Called at the start of {@link #handleMessage(BarrageProtoUtil.MessageInfo)} to validate that the message is of
     * the correct type and is initialized properly.
     * 
     * @param message the message to validate
     */
    protected void validateMessage(@NotNull final BarrageProtoUtil.MessageInfo message) {
        // verify this is the correct type of message for this handler
        if (message.app_metadata.msgType() != BarrageMessageType.BarrageSnapshotRequest) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Request type cannot be changed after initialization, expected BarrageSnapshotRequest metadata");
        }
    }

    @Override
    public void close() {
        // no work to do for DoGetRequest close
        // possibly safely complete if finished sending data
        final ArrowFlightUtil.HalfClosedState newState = halfClosedState.updateAndGet(current -> {
            switch (current) {
                case DONT_CLOSE:
                    // record that we have half closed
                    return ArrowFlightUtil.HalfClosedState.CLIENT_HALF_CLOSED;
                case FINISHED_SENDING:
                    // since client has now half closed, and we're done sending, time to half-close from server
                    return ArrowFlightUtil.HalfClosedState.CLOSED;
                case CLIENT_HALF_CLOSED:
                case CLOSED:
                    throw new IllegalStateException("Can't close twice");
                default:
                    throw new IllegalStateException("Unknown state " + current);
            }
        });
        if (newState == ArrowFlightUtil.HalfClosedState.CLOSED) {
            GrpcUtil.safelyComplete(listener);
        }
    }

    /**
     * Factory to handle Barrage DoExchange snapshot requests.
     */
    @AutoService(ExchangeRequestHandlerFactory.class)
    public static class BarrageSnapshotRequestHandlerFactory implements ExchangeRequestHandlerFactory {
        @Override
        public byte type() {
            return BarrageMessageType.BarrageSnapshotRequest;
        }

        @Override
        public ArrowFlightUtil.DoExchangeMarshaller.Handler create(
                final ArrowFlightUtil.DoExchangeMarshaller marshaller,
                final StreamObserver<BarrageMessageWriter.MessageView> listener) {
            return new BarrageSnapshotRequestHandler(marshaller,
                    marshaller.getTicketRouter(),
                    marshaller.getSession(),
                    marshaller.getStreamGeneratorFactory(),
                    listener);
        }
    }
}
