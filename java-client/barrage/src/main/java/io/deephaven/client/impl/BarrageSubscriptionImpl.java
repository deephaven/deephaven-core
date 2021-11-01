/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.base.log.LogOutput;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.BarrageMessageConsumer;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.BarrageStreamReader;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.util.liveness.ReferenceCountedLivenessNode;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class BarrageSubscriptionImpl extends ReferenceCountedLivenessNode implements BarrageSubscription {
    private static final Logger log = LoggerFactory.getLogger(BarrageSubscriptionImpl.class);

    private final String logName;
    private final TableHandle tableHandle;
    private final BarrageSubscriptionOptions options;
    private final ClientCall<Flight.FlightData, BarrageMessage> call;

    private BarrageTable resultTable;

    private boolean subscribed = false;
    private volatile boolean connected = false;

    /**
     * Represents a BarrageSubscription.
     *
     * @param session the Deephaven session that this export belongs to
     * @param tableHandle the tableHandle to subscribe to (ownership is transferred to the subscription)
     * @param options the transport level options for this subscription
     */
    public BarrageSubscriptionImpl(
            final BarrageSession session, final TableHandle tableHandle, final BarrageSubscriptionOptions options) {
        super(false);

        this.logName = ExportTicketHelper.toReadableString(tableHandle.ticket(), "tableHandle.ticket()");
        this.options = options;
        this.tableHandle = tableHandle;

        final TableDefinition tableDefinition = BarrageUtil.convertArrowSchema(tableHandle.response()).tableDef;
        resultTable = BarrageTable.make(tableDefinition, false);
        resultTable.addParentReference(this);

        final MethodDescriptor<Flight.FlightData, BarrageMessage> subscribeDescriptor =
                getClientDoExchangeDescriptor(options, resultTable.getWireChunkTypes(), resultTable.getWireTypes(),
                        resultTable.getWireComponentTypes(), new BarrageStreamReader());

        this.call = session.channel().newCall(subscribeDescriptor, CallOptions.DEFAULT);

        ClientCalls.asyncBidiStreamingCall(call, new ClientResponseObserver<Flight.FlightData, BarrageMessage>() {
            @Override
            public void beforeStart(final ClientCallStreamObserver<Flight.FlightData> requestStream) {
                requestStream.disableAutoInboundFlowControl();
            }

            @Override
            public void onNext(final BarrageMessage barrageMessage) {
                if (barrageMessage == null) {
                    return;
                }
                try {
                    final BarrageMessage.Listener listener = resultTable;
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
                log.error().append(BarrageSubscriptionImpl.this)
                        .append(": Error detected in subscription: ")
                        .append(t).endl();

                final BarrageMessage.Listener listener = resultTable;
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
        });

        // Allow the server to send us all commands when there is sufficient bandwidth:
        call.request(Integer.MAX_VALUE);

        // Although this is a white lie, the call is established
        this.connected = true;
    }

    @Override
    public synchronized BarrageTable entireTable() {
        if (!connected) {
            throw new UncheckedDeephavenException(
                    this + " is no longer an active subscription and cannot be retained further");
        }
        if (!subscribed) {
            // Send the initial subscription:
            call.sendMessage(Flight.FlightData.newBuilder()
                    .setAppMetadata(ByteStringAccess.wrap(makeRequestInternal(null, null, options)))
                    .build());
            subscribed = true;
        }

        return resultTable;
    }

    @Override
    protected synchronized void destroy() {
        super.destroy();
        close();
    }

    private synchronized void handleDisconnect() {
        if (!connected) {
            return;
        }
        log.error().append(this).append(": unexpectedly closed by other host").endl();
        cleanup();
    }

    @Override
    public synchronized void close() {
        if (!connected) {
            return;
        }
        call.halfClose();
        cleanup();
    }

    private void cleanup() {
        this.connected = false;
        this.tableHandle.close();
        resultTable = null;
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append("Barrage/ClientSubscription/").append(logName).append("/")
                .append(System.identityHashCode(this)).append("/");
    }

    private ByteBuffer makeRequestInternal(
            @Nullable final Index viewport,
            @Nullable final BitSet columns,
            @Nullable BarrageSubscriptionOptions options) {
        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int colOffset = 0;
        if (columns != null) {
            colOffset = BarrageSubscriptionRequest.createColumnsVector(metadata, columns.toByteArray());
        }
        int vpOffset = 0;
        if (viewport != null) {
            vpOffset = BarrageSubscriptionRequest.createViewportVector(
                    metadata, BarrageProtoUtil.toByteBuffer(viewport));
        }
        int optOffset = 0;
        if (options != null) {
            optOffset = options.appendTo(metadata);
        }

        final int ticOffset = BarrageSubscriptionRequest.createTicketVector(metadata,
                tableHandle.ticket().getTicket().asReadOnlyByteBuffer());
        BarrageSubscriptionRequest.startBarrageSubscriptionRequest(metadata);
        BarrageSubscriptionRequest.addColumns(metadata, colOffset);
        BarrageSubscriptionRequest.addViewport(metadata, vpOffset);
        BarrageSubscriptionRequest.addSubscriptionOptions(metadata, optOffset);
        BarrageSubscriptionRequest.addTicket(metadata, ticOffset);
        metadata.finish(BarrageSubscriptionRequest.endBarrageSubscriptionRequest(metadata));

        final FlatBufferBuilder wrapper = new FlatBufferBuilder();
        final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
        wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                wrapper,
                BarrageUtil.FLATBUFFER_MAGIC,
                BarrageMessageType.BarrageSubscriptionRequest,
                innerOffset));
        return wrapper.dataBuffer();
    }

    public static <ReqT, RespT> MethodDescriptor<ReqT, RespT> descriptorFor(
            final MethodDescriptor.MethodType methodType,
            final String serviceName,
            final String methodName,
            final MethodDescriptor.Marshaller<ReqT> requestMarshaller,
            final MethodDescriptor.Marshaller<RespT> responseMarshaller,
            final MethodDescriptor<?, ?> descriptor) {

        return MethodDescriptor.<ReqT, RespT>newBuilder()
                .setType(methodType)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(serviceName, methodName))
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(responseMarshaller)
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build();
    }

    /**
     * Fetch the client side descriptor for a specific table schema.
     *
     * @param options the set of options that last across the entire life of the subscription
     * @param columnChunkTypes the chunk types per column
     * @param columnTypes the class type per column
     * @param componentTypes the component class type per column
     * @param streamReader the stream reader - intended to be thread safe and re-usable
     * @param <Options> the options related to deserialization
     * @return the client side method descriptor
     */
    public static <Options> MethodDescriptor<Flight.FlightData, BarrageMessage> getClientDoExchangeDescriptor(
            final Options options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final BarrageMessageConsumer.StreamReader<Options> streamReader) {
        return descriptorFor(
                MethodDescriptor.MethodType.BIDI_STREAMING, FlightServiceGrpc.SERVICE_NAME, "DoExchange",
                ProtoUtils.marshaller(Flight.FlightData.getDefaultInstance()),
                new BarrageDataMarshaller<>(options, columnChunkTypes, columnTypes, componentTypes, streamReader),
                FlightServiceGrpc.getDoExchangeMethod());
    }

    public static class BarrageDataMarshaller<Options> implements MethodDescriptor.Marshaller<BarrageMessage> {
        private final Options options;
        private final ChunkType[] columnChunkTypes;
        private final Class<?>[] columnTypes;
        private final Class<?>[] componentTypes;
        private final BarrageMessageConsumer.StreamReader<Options> streamReader;

        public BarrageDataMarshaller(
                final Options options,
                final ChunkType[] columnChunkTypes,
                final Class<?>[] columnTypes,
                final Class<?>[] componentTypes,
                final BarrageMessageConsumer.StreamReader<Options> streamReader) {
            this.options = options;
            this.columnChunkTypes = columnChunkTypes;
            this.columnTypes = columnTypes;
            this.componentTypes = componentTypes;
            this.streamReader = streamReader;
        }

        @Override
        public InputStream stream(final BarrageMessage value) {
            throw new UnsupportedOperationException(
                    "BarrageDataMarshaller unexpectedly used to directly convert BarrageMessage to InputStream");
        }

        @Override
        public BarrageMessage parse(final InputStream stream) {
            return streamReader.safelyParseFrom(options, columnChunkTypes, columnTypes, componentTypes, stream);
        }
    }
}
