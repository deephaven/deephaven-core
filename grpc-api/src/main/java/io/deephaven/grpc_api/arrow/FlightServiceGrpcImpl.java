/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.arrow;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import com.google.rpc.Code;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.grpc_api.barrage.BarrageMessageProducer;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.db.util.liveness.SingletonLivenessManager;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.remote.ConstructSnapshot;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.grpc_api.barrage.BarrageStreamGenerator;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api.session.TicketRouter;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.BitSet;
import java.util.Queue;

import static io.deephaven.grpc_api.arrow.ArrowFlightUtil.parseProtoMessage;

@Singleton
public class FlightServiceGrpcImpl<Options, View> extends FlightServiceGrpc.FlightServiceImplBase {
    public static final int DEFAULT_UPDATE_INTERVAL_MS = Configuration.getInstance().getIntegerWithDefault("barrage.updateInterval", 1000);

    // TODO NATE: pull app_metadata off of DoGet -- what about doPut? (core#412): use app_metadata to communicate serialization options
    private static final ChunkInputStreamGenerator.Options DEFAULT_DESER_OPTIONS = new ChunkInputStreamGenerator.Options.Builder().build();

    private static final Logger log = LoggerFactory.getLogger(FlightServiceGrpcImpl.class);

    private final SessionService sessionService;
    private final TicketRouter ticketRouter;

    private final BarrageMessageProducer.Operation.Factory<Options, View> operationFactory;
    private final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<View>> listenerAdapter;
    private final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, Options> optionsAdapter;

    @Inject()
    public FlightServiceGrpcImpl(final SessionService sessionService,
                                 final TicketRouter ticketRouter,
                                 final BarrageMessageProducer.Operation.Factory<Options, View> operationFactory,
                                 final BarrageMessageProducer.Adapter<StreamObserver<InputStream>, StreamObserver<View>> listenerAdapter,
                                 final BarrageMessageProducer.Adapter<BarrageSubscriptionRequest, Options> optionsAdapter) {
        this.ticketRouter = ticketRouter;
        this.sessionService = sessionService;
        this.operationFactory = operationFactory;
        this.listenerAdapter = listenerAdapter;
        this.optionsAdapter = optionsAdapter;
    }

    @Override
    public void listFlights(final Flight.Criteria request, final StreamObserver<Flight.FlightInfo> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            ticketRouter.visitFlightInfo(sessionService.getOptionalSession(), responseObserver::onNext);
            responseObserver.onCompleted();
        });
    }

    @Override
    public void getFlightInfo(final Flight.FlightDescriptor request, final StreamObserver<Flight.FlightInfo> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getOptionalSession();

            final SessionState.ExportObject<Flight.FlightInfo> export = ticketRouter.flightInfoFor(session, request);

            if (session != null) {
                session.nonExport()
                        .require(export)
                        .onError(responseObserver::onError)
                        .submit(() -> {
                            responseObserver.onNext(export.get());
                            responseObserver.onCompleted();
                        });
            } else {
                if (export.tryRetainReference()) {
                    try {
                        if (export.getState() == ExportNotification.State.EXPORTED) {
                            responseObserver.onNext(export.get());
                            responseObserver.onCompleted();
                        }
                    } finally {
                        export.dropReference();
                    }
                } else {
                    responseObserver.onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not find flight info"));
                }
            }
        });
    }

    @Override
    public void getSchema(final Flight.FlightDescriptor request, final StreamObserver<Flight.SchemaResult> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getOptionalSession();

            final SessionState.ExportObject<Flight.FlightInfo> export = ticketRouter.flightInfoFor(session, request);

            if (session != null) {
                session.nonExport()
                        .require(export)
                        .onError(responseObserver::onError)
                        .submit(() -> {
                            responseObserver.onNext(Flight.SchemaResult.newBuilder()
                                    .setSchema(export.get().getSchema())
                                    .build());
                            responseObserver.onCompleted();
                        });
            } else {
                if (export.tryRetainReference()) {
                    try {
                        if (export.getState() == ExportNotification.State.EXPORTED) {
                            responseObserver.onNext(Flight.SchemaResult.newBuilder()
                                    .setSchema(export.get().getSchema())
                                    .build());
                            responseObserver.onCompleted();
                        }
                    } finally {
                        export.dropReference();
                    }
                } else {
                    responseObserver.onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not find flight info"));
                }
            }
        });
    }

    public void doGetCustom(final Flight.Ticket request, final StreamObserver<InputStream> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();
            final SessionState.ExportObject<BaseTable> export = ticketRouter.resolve(session, request);
            session.nonExport()
                    .require(export)
                    .onError(responseObserver::onError)
                    .submit(() -> {
                        final BaseTable table = export.get();

                        // Send Schema wrapped in Message
                        final FlatBufferBuilder builder = new FlatBufferBuilder();
                        final int schemaOffset = BarrageSchemaUtil.makeSchemaPayload(builder, table.getDefinition(), table.getAttributes());
                        builder.finish(BarrageStreamGenerator.wrapInMessage(builder, schemaOffset,  org.apache.arrow.flatbuf.MessageHeader.Schema));
                        final ByteBuffer serializedMessage = builder.dataBuffer();

                        final byte[] msgBytes = Flight.FlightData.newBuilder()
                                .setDataHeader(ByteStringAccess.wrap(serializedMessage))
                                .build()
                                .toByteArray();
                        responseObserver.onNext(new BarrageStreamGenerator.DrainableByteArrayInputStream(msgBytes, 0, msgBytes.length));

                        // get ourselves some data!
                        final BarrageMessage msg = ConstructSnapshot.constructBackplaneSnapshot(this, table);
                        msg.modColumnData = new BarrageMessage.ModColumnData[0]; // actually no mod column data for DoGet

                        try (final BarrageStreamGenerator bsg = new BarrageStreamGenerator(msg)) {
                            bsg.forEachDoGetStream(bsg.getSubView(DEFAULT_DESER_OPTIONS, false), responseObserver::onNext);
                        } catch (final IOException e) {
                            throw new UncheckedDeephavenException(e); // unexpected
                        }

                        responseObserver.onCompleted();
                    });
        });
    }

    public StreamObserver<InputStream> doPutCustom(final StreamObserver<Flight.PutResult> responseObserver) {
        return GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = sessionService.getCurrentSession();

            return new StreamObserver<InputStream>() {
                private final ArrowFlightUtil.DoPutObserver marshaller = new ArrowFlightUtil.DoPutObserver(session, ticketRouter, responseObserver);

                @Override
                public void onNext(final InputStream request) {
                    GrpcUtil.rpcWrapper(log, responseObserver, () -> {
                        try {
                            marshaller.process(parseProtoMessage(request));
                        } catch (final IOException unexpected) {
                            throw GrpcUtil.securelyWrapError(log, unexpected);
                        }
                    });
                }

                @Override
                public void onError(final Throwable t) {
                    marshaller.onError(t);
                }

                @Override
                public void onCompleted() {
                    marshaller.onComplete();
                }
            };
        });
    }

    /**
     * Establish a new DoExchange bi-directional stream.
     *
     * @param responseObserver the observer to reply to
     * @return the observer that grpc can delegate received messages to
     */
    public StreamObserver<InputStream> doExchangeCustom(final StreamObserver<InputStream> responseObserver) {
//        return GrpcUtil.rpcWrapper(log, responseObserver, () -> new SubscriptionObserver(sessionService.getCurrentSession(), responseObserver));
        throw new UnsupportedOperationException("TODO: check metadata of first request to see if it is a subscription, etc");
    }

    /**
     * Receive an out-of-band exchange update for an existing exchange call.
     *
     * @param request the request to submit as if it was sent on the client-streaming side of the export
     * @param responseObserver the response observer to notify of any errors or successes
     */
//    public void doUpdateDoExchange(final Flight.FlightData request, final StreamObserver<Flight.OOBPutResult> responseObserver) {
//        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
//            final ByteString metadataByteString = request.getAppMetadata();
//            final BarrageMessageWrapper wrapper = BarrageMessageWrapper.getRootAsBarrageMessageWrapper(metadataByteString.asReadOnlyByteBuffer());
//            if (wrapper.magic() != BarrageStreamGenerator.FLATBUFFER_MAGIC) {
//                responseObserver.onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
//                        "Out-of-band support requires BarrageMessageWrapper app_metadata with correct magic \"dhvn\""));
//                return;
//            }
//
//            if (wrapper.rpcTicketVector() == null) {
//                responseObserver.onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Export ticket not specified; cannot locate DoExchange"));
//                return;
//            }
//
//            final SessionState session = sessionService.getCurrentSession();
//            final SessionState.ExportObject<SubscriptionObserver> subscription = ticketRouter.resolve(session, wrapper.rpcTicketAsByteBuffer());
//
//            session.nonExport()
//                    .require(subscription)
//                    .onError(responseObserver::onError)
//                    .submit(() -> {
//                        subscription.get().onNext(request);
//                        responseObserver.onNext(Flight.OOBPutResult.newBuilder().build());
//                        responseObserver.onCompleted();
//                    });
//        });
//    }



//    /**
//     * Subscribe with server-side streaming only. (Updates may be sent out of band, if subscription is also exported.)
//     * @param request the initial one-shot subscription request to get this subscription started
//     * @param responseObserver the observer to send subscription events to
//     */
//    public void doExchangeCustom(final Flight.FlightData request, final StreamObserver<InputStream> responseObserver) {
//        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
//            final SubscriptionObserver observer = new SubscriptionObserver(sessionService.getCurrentSession(), responseObserver);
//            observer.onNext(request);
//        });
//    }

    /**
     * Helper class that maintains a subscription whether it was created by a bi-directional stream request or the
     * no-client-streaming request. If the SubscriptionRequest sets the sequence, then it treats sequence as a watermark
     * and will not send out-of-order requests (due to out-of-band requests). The client should already anticipate
     * subscription changes may be coalesced by the BarrageMessageProducer.
     */
    private class SubscriptionObserver extends SingletonLivenessManager implements StreamObserver<Flight.FlightData>, Closeable {
        private final String myPrefix;
        private final SessionState session;

        private long seqWatermark;
        private boolean isViewport;
        private BarrageMessageProducer<Options, View> bmp;
        private Queue<BarrageMessageWrapper> preExportSubscriptions;

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
        public synchronized void onNext(final Flight.FlightData subscriptionRequest) {
            GrpcUtil.rpcWrapper(log, listener, () -> {
                final BarrageMessageWrapper msg = BarrageMessageWrapper.getRootAsBarrageMessageWrapper(subscriptionRequest.getAppMetadata().asReadOnlyByteBuffer());

                if (bmp == null) {
                    synchronized (this) {
                        if (bmp == null) {
                            queueRequest(msg);
                            return;
                        }
                    }
                }

                apply(msg);
            });
        }

        /**
         * Update the existing subscription to match the new request.
         * @param msg the requested view change
         */
        private void apply(final BarrageMessageWrapper msg) {
            if (msg.magic() != BarrageStreamGenerator.FLATBUFFER_MAGIC || msg.msgType() != BarrageMessageType.BarrageSubscriptionRequest) {
                return;
            }

            if (seqWatermark > 0 && seqWatermark >= msg.sequence()) {
                return;
            }
            seqWatermark = msg.sequence();
            log.info().append(myPrefix).append("applying subscription request w/seq ").append(seqWatermark).endl();

            final BarrageSubscriptionRequest subscriptionRequest = BarrageSubscriptionRequest.getRootAsBarrageSubscriptionRequest(msg.msgPayloadAsByteBuffer());
            final boolean hasColumns = subscriptionRequest.columnsVector() != null;
            final BitSet columns = hasColumns ? BitSet.valueOf(subscriptionRequest.columnsAsByteBuffer()) : new BitSet();

            final boolean hasViewport = subscriptionRequest.viewportVector() != null;
            final Index viewport = isViewport ? BarrageProtoUtil.toIndex(subscriptionRequest.viewportAsByteBuffer()) : null;

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

        private synchronized void queueRequest(final BarrageMessageWrapper msg) {
            if (preExportSubscriptions != null) {
                preExportSubscriptions.add(msg);
                return;
            }

            preExportSubscriptions = new ArrayDeque<>();
            preExportSubscriptions.add(msg);

            final BarrageSubscriptionRequest subscriptionRequest = BarrageSubscriptionRequest.getRootAsBarrageSubscriptionRequest(msg.msgPayloadAsByteBuffer());
            if (subscriptionRequest.ticketVector() == null) {
                listener.onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Ticket not specified."));
                return;
            }

            final SessionState.ExportObject<Object> parent = ticketRouter.resolve(session, subscriptionRequest.ticketAsByteBuffer());

            final SessionState.ExportBuilder<SubscriptionObserver> exportBuilder;
            if (msg.rpcTicketVector() != null) {
                exportBuilder = session.newExport(ExportTicketHelper.ticketToExportId(msg.rpcTicketAsByteBuffer()));
            } else {
                exportBuilder = session.nonExport();
            }

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
                                long updateIntervalMs = subscriptionRequest.updateIntervalMs();
                                if (updateIntervalMs == 0) {
                                    updateIntervalMs = DEFAULT_UPDATE_INTERVAL_MS;
                                }
                                bmp = table.getResult(operationFactory.create(table, updateIntervalMs));
                                manage(bmp);
                            } else {
                                listener.onError(GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "Ticket ("
                                        + ExportTicketHelper.toReadableString(subscriptionRequest.ticketAsByteBuffer())
                                        + ") is not a subscribable table."));
                                return null;
                            }

                            log.info().append(myPrefix).append("processing initial subscription").endl();
                            final BarrageMessageWrapper initial = preExportSubscriptions.remove();
                            seqWatermark = initial.sequence();

                            final boolean hasColumns = subscriptionRequest.columnsVector() != null;
                            final BitSet columns = hasColumns ? BitSet.valueOf(subscriptionRequest.columnsAsByteBuffer()) : new BitSet();

                            isViewport = subscriptionRequest.viewportVector() != null;
                            final Index viewport = isViewport ? BarrageProtoUtil.toIndex(subscriptionRequest.viewportAsByteBuffer()) : null;

                            if (!bmp.addSubscription(listener, optionsAdapter.adapt(subscriptionRequest), columns, viewport)) {
                                throw new IllegalStateException("listener is already a subscriber!");
                            }

                            for (final BarrageMessageWrapper request : preExportSubscriptions) {
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
