package io.deephaven.grpc_api.barrage;

import com.google.rpc.Code;
import com.google.rpc.Status;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.util.liveness.SingletonLivenessManager;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.BarrageServiceGrpc;
import io.deephaven.proto.backplane.grpc.OutOfBandSubscriptionResponse;
import io.deephaven.proto.backplane.grpc.SubscriptionRequest;
import io.grpc.protobuf.StatusProto;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Queue;

@Singleton
public class BarrageServiceGrpcImpl<Options, View> extends BarrageServiceGrpc.BarrageServiceImplBase {
    private static final int DEFAULT_UPDATE_INTERVAL_MS = Configuration.getInstance().getIntegerWithDefault("barrage.updateInterval", 1000);

    private static final Logger log = LoggerFactory.getLogger(BarrageServiceGrpcImpl.class);

    private final TicketRouter ticketRouter;
    private final SessionService sessionService;
    private final BarrageMessageProducer.Operation.Factory<Options, View> operationFactory;
    private final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<View>> listenerAdapter;
    private final BarrageMessageProducer.Adapter<SubscriptionRequest, Options> optionsAdapter;

    @Inject
    public BarrageServiceGrpcImpl(
            final TicketRouter ticketRouter,
            final SessionService sessionService,
            final BarrageMessageProducer.Operation.Factory<Options, View> operationFactory,
            final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<View>> listenerAdapter,
            final BarrageMessageProducer.Adapter<SubscriptionRequest, Options> optionsAdapter) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.operationFactory = operationFactory;
        this.listenerAdapter = listenerAdapter;
        this.optionsAdapter = optionsAdapter;
    }

    /**
     * Receive an out-of-band subscription update for an existing subscription that was
     * @param request the request to submit as if it was sent on the client-streaming side of the export
     * @param responseObserver the response observer to notify of any errors or successes
     */
    @Override
    public void doUpdateSubscription(final SubscriptionRequest request, final StreamObserver<OutOfBandSubscriptionResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            if (!request.hasExportId()) {
                responseObserver.onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Export ID not specified."));
                return;
            }

            final SessionState session = sessionService.getCurrentSession();
            final SessionState.ExportObject<SubscriptionObserver> subscription = ticketRouter.resolve(session, request.getExportId());

            session.nonExport()
                    .require(subscription)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        subscription.get().onNext(request);
                        responseObserver.onNext(OutOfBandSubscriptionResponse.newBuilder().setSubscriptionFound(true).build());
                        responseObserver.onCompleted();
                    });
        });
    }

    /**
     * Subscribe with a normal bi-directional stream.
     * @param responseObserver the observer to send subscription events to
     * @return the observer that grpc can delegate updates to
     */
    public StreamObserver<SubscriptionRequest> doSubscribeCustom(final StreamObserver<InputStream> responseObserver) {
        return GrpcUtil.rpcWrapper(log, responseObserver, () -> new SubscriptionObserver(sessionService.getCurrentSession(), responseObserver));
    }

    /**
     * Subscribe with server-side streaming only. (Updates may be sent out of band, if subscription is also exported.)
     * @param request the initial one-shot subscription request to get this subscription started
     * @param responseObserver the observer to send subscription events to
     */
    public void doSubscribeCustom(final SubscriptionRequest request, final StreamObserver<InputStream> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SubscriptionObserver observer = new SubscriptionObserver(sessionService.getCurrentSession(), responseObserver);
            observer.onNext(request);
        });
    }

    /**
     * Helper class that maintains a subscription whether it was created by a bi-directional stream request or the
     * no-client-streaming request. If the SubscriptionRequest sets the sequence, then it treats sequence as a watermark
     * and will not send out-of-order requests (due to out-of-band requests). The client should already anticipate
     * subscription changes may be coalesced by the BarrageMessageProducer.
     */
    private class SubscriptionObserver extends SingletonLivenessManager implements StreamObserver<SubscriptionRequest>, Closeable {
        private final String myPrefix;
        private final SessionState session;

        private long seqWatermark;
        private boolean isViewport;
        private BarrageMessageProducer<Options, View> bmp;
        private Queue<SubscriptionRequest> preExportSubscriptions;

        private final StreamObserver<View> listener;

        private boolean isClosed = false;
        private SessionState.ExportObject<SubscriptionObserver> subscriptionExport;

        public SubscriptionObserver(final SessionState session, final StreamObserver<InputStream> responseObserver) {
            this.myPrefix = "SubscriptionObserver{" + Integer.toHexString(System.identityHashCode(this)) + "}: ";
            this.session = session;
            this.listener = listenerAdapter.adapt(responseObserver);
            this.session.addOnCloseCallback(this);
            ((ServerCallStreamObserver<InputStream>) responseObserver).setOnCancelHandler(this::tryClose);
        }

        @Override
        public synchronized void onNext(final SubscriptionRequest subscriptionRequest) {
            GrpcUtil.rpcWrapper(log, listener, () -> {
                if (bmp == null) {
                    synchronized (this) {
                        if (bmp == null) {
                            queueRequest(subscriptionRequest);
                            return;
                        }
                    }
                }

                apply(subscriptionRequest);
            });
        }

        /**
         * Update the existing subscription to match the new request.
         * @param subscriptionRequest the requested view change
         */
        private void apply(final SubscriptionRequest subscriptionRequest) {
            if (seqWatermark > 0 && seqWatermark >= subscriptionRequest.getSequence()) {
                return;
            }
            seqWatermark = subscriptionRequest.getSequence();
            log.info().append(myPrefix).append("applying subscription request w/seq ").append(seqWatermark).endl();

            final boolean hasColumns = !subscriptionRequest.getColumns().isEmpty();
            final BitSet columns = hasColumns ? BarrageProtoUtil.toBitSet(subscriptionRequest.getColumns()) : new BitSet();

            final boolean hasViewport = !subscriptionRequest.getViewport().isEmpty();
            final Index viewport = isViewport ? BarrageProtoUtil.toIndex(subscriptionRequest.getViewport()) : null;

            final boolean subscriptionFound;
            if (isViewport && hasColumns && hasViewport) {
                subscriptionFound = bmp.updateViewportAndColumns(listener, viewport, columns);
            } else if (isViewport && hasViewport) {
                subscriptionFound = bmp.updateViewport(listener, viewport);
            } else if (hasColumns) {
                subscriptionFound = bmp.updateSubscription(listener, columns);
            } else {
                subscriptionFound = true;
            }

            if (!subscriptionFound) {
                throw GrpcUtil.statusRuntimeException(Code.INTERNAL, "Subscription was not found.");
            }
        }

        private synchronized void queueRequest(final SubscriptionRequest subscriptionRequest) {
            if (preExportSubscriptions != null) {
                preExportSubscriptions.add(subscriptionRequest);
                return;
            }

            preExportSubscriptions = new ArrayDeque<>();
            preExportSubscriptions.add(subscriptionRequest);

            if (!subscriptionRequest.hasTicket()) {
                listener.onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Ticket not specified."));
                return;
            }

            final SessionState.ExportObject<Object> parent = ticketRouter.resolve(session, subscriptionRequest.getTicket());

            final SessionState.ExportBuilder<SubscriptionObserver> exportBuilder;
            if (subscriptionRequest.hasExportId()) {
                exportBuilder = session.newExport(subscriptionRequest.getExportId());
            } else {
                exportBuilder = session.nonExport();
            }

            log.info().append(myPrefix).append("awaiting parent table").endl();
            subscriptionExport = exportBuilder
                    .require(parent)
                    .onError(listener::onError)
                    .submit(() -> {
                        synchronized (SubscriptionObserver.this) {
                            subscriptionExport = null;

                            if (isClosed) {
                                return null;
                            }

                            final Object export = parent.get();
                            if (export instanceof QueryTable) {
                                final QueryTable table = (QueryTable) export;
                                long updateIntervalMs = subscriptionRequest.getUpdateIntervalMs();
                                if (updateIntervalMs == 0) {
                                    updateIntervalMs = DEFAULT_UPDATE_INTERVAL_MS;
                                }
                                bmp = table.getResult(operationFactory.create(table, updateIntervalMs));
                                manage(bmp);
                            } else {
                                listener.onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION,
                                        "Ticket (" + subscriptionRequest.getTicket().getTicket() + ") is not a subscribable table."));
                                return null;
                            }

                            log.info().append(myPrefix).append("processing initial subscription").endl();
                            final SubscriptionRequest initial = preExportSubscriptions.remove();
                            seqWatermark = initial.getSequence();

                            final boolean hasColumns = !subscriptionRequest.getColumns().isEmpty();
                            final BitSet columns = hasColumns ? BarrageProtoUtil.toBitSet(subscriptionRequest.getColumns()) : new BitSet();

                            isViewport = !subscriptionRequest.getViewport().isEmpty();
                            final Index viewport = isViewport ? BarrageProtoUtil.toIndex(subscriptionRequest.getViewport()) : null;

                            if (!bmp.addSubscription(listener, optionsAdapter.adapt(initial), columns, viewport)) {
                                throw new IllegalStateException("listener is already a subscriber!");
                            }

                            for (final SubscriptionRequest request : preExportSubscriptions) {
                                apply(request);
                            }

                            // we will now process requests as they are received
                            preExportSubscriptions = null;
                            return SubscriptionObserver.this;
                        }
                    });
        }

        @Override
        public void onError(final Throwable t) {
            log.error().append(myPrefix).append("unexpected error; force closing subscription: caused by ").append(t).endl();
            tryClose();
        }

        @Override
        public void onCompleted() {
            log.error().append(myPrefix).append("client stream closed subscription").endl();
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

            if (subscriptionExport != null) {
                subscriptionExport.cancel();
                subscriptionExport = null;
            }

            if (bmp != null) {
                bmp.removeSubscription(listener);
                bmp = null;
            }
            release();
        }

        private void tryClose() {
            if (session.removeOnCloseCallback(this) != null) {
                close();
            }
        }
    }
}
