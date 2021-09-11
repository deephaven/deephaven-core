/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.barrage;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.grpc_api.arrow.FlightServiceGrpcBinding;
import io.deephaven.grpc_api_client.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.grpc_api_client.table.BarrageTable;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.io.logger.Logger;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.proto.backplane.grpc.Ticket;
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
import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * This is a client side representation of a backplane subscription.
 */
public class BarrageClientSubscription implements LogOutputAppendable {

    private static final Logger log = LoggerFactory.getLogger(BarrageClientSubscription.class);

    private volatile boolean connected = false;

    private final String logName;
    private final boolean isViewport;

    private final ClientCall<Flight.FlightData, BarrageMessage> call;

    public BarrageClientSubscription(
            final String logName,
            final Channel channel,
            final BarrageSubscriptionRequest initialRequest,
            final BarrageMessageConsumer.StreamReader<ChunkInputStreamGenerator.Options> streamReader,
            final BarrageTable resultTable) {
        this(logName, channel, initialRequest, streamReader,
                resultTable.getWireChunkTypes(),
                resultTable.getWireTypes(),
                resultTable.getWireComponentTypes(),
                new WeakReference<>(resultTable));
    }

    public BarrageClientSubscription(
            final String logName,
            final Channel channel,
            final BarrageSubscriptionRequest initialRequest,
            final BarrageMessageConsumer.StreamReader<ChunkInputStreamGenerator.Options> streamReader,
            final ChunkType[] wireChunkTypes,
            final Class<?>[] wireTypes,
            final Class<?>[] wireComponentTypes,
            final WeakReference<BarrageMessage.Listener> weakListener) {
        this.logName = logName;
        this.isViewport = initialRequest.viewportVector() != null;

        // final Channel channel = authClientManager.getAuthChannel();

        final BarrageMessage.Listener rt = weakListener.get();
        if (rt == null) {
            this.call = null;
            log.error().append(this).append(": replicated table already garbage collected not requesting subscription")
                    .endl();
            return;
        }

        final ChunkInputStreamGenerator.Options options = ChunkInputStreamGenerator.Options.of(initialRequest);

        final MethodDescriptor<Flight.FlightData, BarrageMessage> subscribeDescriptor =
                FlightServiceGrpcBinding.getClientDoExchangeDescriptor(options, wireChunkTypes, wireTypes,
                        wireComponentTypes, streamReader);
        this.call = channel.newCall(subscribeDescriptor, CallOptions.DEFAULT);

        ClientCalls.asyncBidiStreamingCall(call, new ClientResponseObserver<Flight.FlightData, BarrageMessage>() {
            @Override
            public void beforeStart(final ClientCallStreamObserver<Flight.FlightData> requestStream) {
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
        final FlatBufferBuilder message = new FlatBufferBuilder();
        final int requestOffset = message.createByteVector(initialRequest.getByteBuffer());
        message.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                message,
                BarrageStreamGenerator.FLATBUFFER_MAGIC,
                BarrageMessageType.BarrageSubscriptionRequest,
                requestOffset,
                0, // no ticket -- now unused anyway
                0, // no sequence -- now unused anyway
                false // don't half close -- now unused anyway
        ));

        call.sendMessage(Flight.FlightData.newBuilder()
                .setAppMetadata(ByteStringAccess.wrap(message.dataBuffer()))
                .build());

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
        update(null, columns);

    }

    public synchronized void update(final Index viewport) {
        update(viewport, null);
    }

    public synchronized void update(final Index viewport, final BitSet columns) {
        if (viewport != null && !isViewport) {
            throw new IllegalStateException("Cannot set viewport on a full subscription.");
        }

        throw new UnsupportedOperationException("Viewports not yet supported");
//        call.sendMessage(Flight.FlightData.newBuilder()
//                .setAppMetadata(ByteStringAccess.wrap(makeRequestInternal(viewport, columns)))
//                .build());
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append("Barrage/").append("/ClientSubscription/").append(logName).append("/")
                .append(System.identityHashCode(this)).append("/");
    }

    public static BarrageSubscriptionRequest makeRequest(final Ticket ticket, final Index viewport, final BitSet columns) {
        return BarrageSubscriptionRequest.getRootAsBarrageSubscriptionRequest(makeRequestInternal(ticket, viewport, columns));
    }

    private static ByteBuffer makeRequestInternal(final Ticket ticket, final Index viewport, final BitSet columns) {
        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int colOffset = 0;
        if (columns != null) {
            colOffset = BarrageSubscriptionRequest.createColumnsVector(metadata, columns.toByteArray());
        }
        int vpOffset = 0;
        if (viewport != null) {
            vpOffset =
                    BarrageSubscriptionRequest.createViewportVector(metadata, BarrageProtoUtil.toByteBuffer(viewport));
        }
        int ticketOffset = BarrageSubscriptionRequest.createTicketVector(metadata, ticket.getTicket().asReadOnlyByteBuffer());

        BarrageSubscriptionRequest.startBarrageSubscriptionRequest(metadata);
        BarrageSubscriptionRequest.addColumns(metadata, colOffset);
        BarrageSubscriptionRequest.addViewport(metadata, vpOffset);
        BarrageSubscriptionRequest.addTicket(metadata, ticketOffset);
        metadata.finish(BarrageSubscriptionRequest.endBarrageSubscriptionRequest(metadata));

        return metadata.dataBuffer();
    }
}
