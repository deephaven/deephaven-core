//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.google.auto.service.AutoService;
import com.google.rpc.Code;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.annotations.ReflexiveUse;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayDeque;
import java.util.Queue;

/**
 * Handler for BarrageSubscriptionRequest over DoExchange.
 */
@ReflexiveUse(referrers = "ArrowFlightUtil")
public class BarrageSubscriptionRequestHandler implements ArrowFlightUtil.DoExchangeMarshaller.Handler {

    private final ArrowFlightUtil.DoExchangeMarshaller marshaller;
    private final TicketRouter ticketRouter;
    private final SessionState session;
    private final StreamObserver<BarrageMessageWriter.MessageView> listener;

    /**
     * The object representing the subscription.
     */
    private ExchangeMarshaller.Subscription subscriptionObject;

    private Queue<BarrageSubscriptionRequest> preExportSubscriptions;
    private SessionState.ExportObject<?> onExportResolvedContinuation;

    public BarrageSubscriptionRequestHandler(final ArrowFlightUtil.DoExchangeMarshaller marshaller,
            final TicketRouter ticketRouter,
            final SessionState session,
            final StreamObserver<BarrageMessageWriter.MessageView> listener) {
        this.marshaller = marshaller;
        this.ticketRouter = ticketRouter;
        this.session = session;
        this.listener = listener;
    }

    @Override
    public void handleMessage(@NotNull final BarrageProtoUtil.MessageInfo message) {
        validateMessage(message);

        final BarrageSubscriptionRequest subscriptionRequest = BarrageSubscriptionRequest
                .getRootAsBarrageSubscriptionRequest(message.app_metadata.msgPayloadAsByteBuffer());

        synchronized (this) {
            if (subscriptionObject != null) {
                apply(subscriptionRequest);
                return;
            }

            if (marshaller.isClosed()) {
                return;
            }

            // have we already created the queue?
            if (preExportSubscriptions != null) {
                preExportSubscriptions.add(subscriptionRequest);
                return;
            }
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
                        .onErrorHandler(marshaller::onError)
                        .submit(() -> onExportResolved(table));
            }
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
        if (message.app_metadata.msgType() != BarrageMessageType.BarrageSubscriptionRequest) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                    "Request type cannot be changed after initialization, expected BarrageSubscriptionRequest metadata");
        }

        if (message.app_metadata.msgPayloadVector() == null) {
            throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Subscription request not supplied");
        }
    }

    private synchronized void onExportResolved(final SessionState.ExportObject<Object> parent) {
        onExportResolvedContinuation = null;

        if (marshaller.isClosed()) {
            preExportSubscriptions = null;
            return;
        }

        // we know there is at least one request; it was put there when we knew which parent to wait on
        final BarrageSubscriptionRequest subscriptionRequest = preExportSubscriptions.remove();

        final Object export = parent.get();

        final ExchangeMarshaller marshallerForExport =
                ExchangeMarshaller.getMarshaller(export, marshaller.getMarshallers());
        if (marshallerForExport == null) {
            GrpcUtil.safelyError(listener, Code.FAILED_PRECONDITION, "Ticket ("
                    + ExportTicketHelper.toReadableString(subscriptionRequest.ticketAsByteBuffer(), "ticket")
                    + ") is not a subscribable table.");
            return;
        }

        final BarrageSubscriptionOptions options = BarrageSubscriptionOptions.of(subscriptionRequest);

        subscriptionObject =
                marshallerForExport.subscribe(subscriptionRequest, options, export, listener);

        final LivenessReferent subscriptionManagedReference = subscriptionObject.toManage();
        if (subscriptionManagedReference != null) {
            marshaller.manage(subscriptionManagedReference);
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
        final boolean subscriptionFound =
                subscriptionObject != null && subscriptionObject.update(subscriptionRequest);

        if (!subscriptionFound) {
            throw Exceptions.statusRuntimeException(Code.NOT_FOUND, "Subscription was not found.");
        }
    }

    @Override
    public synchronized void close() {
        if (subscriptionObject != null) {
            subscriptionObject.close();
            subscriptionObject = null;
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

    /**
     * Factory to handle Barrage DoExchange subscription requests.
     */
    @AutoService(ExchangeRequestHandlerFactory.class)
    public static class BarrageSubscriptionRequestHandlerFactory implements ExchangeRequestHandlerFactory {
        @Override
        public byte type() {
            return BarrageMessageType.BarrageSubscriptionRequest;
        }

        @Override
        public ArrowFlightUtil.DoExchangeMarshaller.Handler create(
                final ArrowFlightUtil.DoExchangeMarshaller marshaller,
                final StreamObserver<BarrageMessageWriter.MessageView> listener) {
            return new BarrageSubscriptionRequestHandler(marshaller,
                    marshaller.getTicketRouter(),
                    marshaller.getSession(),
                    listener);
        }
    }
}
