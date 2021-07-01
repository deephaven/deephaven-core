/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.barrage;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.table.BarrageSourcedTable;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.proto.backplane.grpc.SubscriptionRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.BitSet;

/**
 * This is a client side representation of a backplane subscription.
 */
public class BarrageClientSubscription implements LogOutputAppendable {

    private static final Logger log = LoggerFactory.getLogger(BarrageClientSubscription.class);

    private volatile boolean connected = false;

    private final String logName;
    private final Flight.Ticket handle;
    private final boolean isViewport;

    private final ClientCall<SubscriptionRequest, BarrageMessage> call;

    public BarrageClientSubscription(
//            final AuthSessionClientManager authClientManager,
            final String logName,
            final Channel channel,
            final Flight.Ticket handle,
            final SubscriptionRequest initialRequest,
            final BarrageMessageConsumer.StreamReader<ChunkInputStreamGenerator.Options> streamReader,
            final BarrageSourcedTable resultTable) {
        this(logName, channel, handle, initialRequest, streamReader,
                resultTable.getWireChunkTypes(),
                resultTable.getWireTypes(),
                resultTable.getWireComponentTypes(),
                new WeakReference<>(resultTable));
    }

    public BarrageClientSubscription(
//            final AuthSessionClientManager authClientManager,
            final String logName,
            final Channel channel,
            final Flight.Ticket handle,
            final SubscriptionRequest initialRequest,
            final BarrageMessageConsumer.StreamReader<ChunkInputStreamGenerator.Options> streamReader,
            final ChunkType[] wireChunkTypes,
            final Class<?>[] wireTypes,
            final Class<?>[] wireComponentTypes,
            final WeakReference<BarrageMessage.Listener> weakListener) {
        this.logName = logName;
        this.handle = handle;
        this.isViewport = !initialRequest.getViewport().isEmpty();

//        final Channel channel = authClientManager.getAuthChannel();

        final BarrageMessage.Listener rt = weakListener.get();
        if (rt == null) {
            this.call = null;
            log.error().append(this).append(": replicated table already garbage collected not requesting subscription").endl();
            return;
        }

        final ChunkInputStreamGenerator.Options options = new ChunkInputStreamGenerator.Options.Builder()
                .setIsViewport(isViewport)
                .setUseDeephavenNulls(initialRequest.getUseDeephavenNulls())
                .build();
        final MethodDescriptor<SubscriptionRequest, BarrageMessage> subscribeDescriptor =
                BarrageServiceGrpcBinding.getClientDoSubscribeDescriptor(options, wireChunkTypes, wireTypes, wireComponentTypes, streamReader);
        this.call = channel.newCall(subscribeDescriptor, CallOptions.DEFAULT);

        ClientCalls.asyncBidiStreamingCall(call, new ClientResponseObserver<SubscriptionRequest, BarrageMessage>() {
            @Override
            public void beforeStart(final ClientCallStreamObserver<SubscriptionRequest> requestStream) {
                // IDS-6890-3: control flow may be needed here
                requestStream.disableAutoInboundFlowControl();
            }

            @Override
            public void onNext(final BarrageMessage barrageMessage) {
                if (barrageMessage == null) {
                    return;
                }
                try {
                    final BarrageMessage.Listener listener = getListener();
                    if (!connected || listener == null) {
                        return;
                    }
                    listener.handleBarrageMessage(barrageMessage);
                } finally {
                    barrageMessage.close();
                }
            }

            @Override
            public void onError(final Throwable t) {
                log.error().append(BarrageClientSubscription.this)
                        .append(": Error detected in subscription: ")
                        .append(t).endl();

                final BarrageMessage.Listener listener = getListener();
                if (!connected || listener == null) {
                    return;
                }
                listener.handleBarrageError(t);
                handleDisconnect();
            }

            @Override
            public void onCompleted() {
                handleDisconnect();
            }

            @Nullable
            private BarrageMessage.Listener getListener() {
                final BarrageMessage.Listener listener = weakListener.get();
                if (listener == null) {
                    close();
                }
                return listener;
            }
        });

        // Set connected here before we initialize the request.
        this.connected = true;

        // Send the initial subscription:
        call.sendMessage(initialRequest);

        // Allow the server to send us all of the commands when there is bandwidth:
        call.request(Integer.MAX_VALUE);
    }

    private synchronized void handleDisconnect() {
        if (!connected) {
            return;
        }
        connected = false;
        log.error().append(this).append(": unexpectedly closed by other host").endl();
    }

    public synchronized void close() {
        if (!connected) {
            return;
        }
        call.halfClose();
        this.connected = false;
    }

    public synchronized void update(final BitSet columns) {
        final SubscriptionRequest request = SubscriptionRequest.newBuilder()
                .setColumns(BarrageProtoUtil.toByteString(columns))
                .build();
        call.sendMessage(request);
    }

    public synchronized void update(final Index viewport) {
        if (!isViewport) {
            throw new IllegalStateException("Cannot set viewport on a full subscription.");
        }

        final SubscriptionRequest request = SubscriptionRequest.newBuilder()
                .setViewport(BarrageProtoUtil.toByteString(viewport))
                .build();
        call.sendMessage(request);
    }

    public synchronized void update(final Index viewport, final BitSet columns) {
        if (!isViewport) {
            throw new IllegalStateException("Cannot set viewport on a full subscription.");
        }

        final SubscriptionRequest request = SubscriptionRequest.newBuilder()
                .setViewport(BarrageProtoUtil.toByteString(viewport))
                .setColumns(BarrageProtoUtil.toByteString(columns))
                .build();
        call.sendMessage(request);
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append("Barrage/").append("/ClientSubscription/").append(logName).append("/")
                .append(System.identityHashCode(this)).append("/");
    }
}
