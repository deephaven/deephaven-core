/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.DoGetRequest;
import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.BarrageMessage.Listener;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.BarrageMessageConsumer;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.BarrageStreamReader;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.Context;
import io.grpc.MethodDescriptor;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientCalls;
import io.grpc.stub.ClientResponseObserver;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.CountDownLatch;

public class BarrageSnapshotImpl extends ReferenceCountedLivenessNode implements BarrageSnapshot {
    private static final Logger log = LoggerFactory.getLogger(BarrageSnapshotImpl.class);

    private final String logName;
    private final TableHandle tableHandle;
    private final BarrageSubscriptionOptions options; // TODO: clone and modify this in barrage.fbs to BarrageSnapshotOptions
    private final ClientCallStreamObserver<FlightData> observer;

    private final BarrageTable resultTable;
    private final CountDownLatch resultLatch = new CountDownLatch(1);
    private Throwable exceptionWhileSealing = null;

    private volatile boolean connected = true;

    /**
     * Represents a BarrageSnapshot.
     *
     * @param session the Deephaven session that this export belongs to
     * @param tableHandle the tableHandle to snapshot (ownership is transferred to the snapshot)
     * @param options the transport level options for this snapshot
     */
    public BarrageSnapshotImpl(
            final BarrageSession session, final TableHandle tableHandle, final BarrageSubscriptionOptions options) {
        super(false);

        this.logName = tableHandle.exportId().toString();
        this.options = options;
        this.tableHandle = tableHandle;

        final TableDefinition tableDefinition = BarrageUtil.convertArrowSchema(tableHandle.response()).tableDef;
        resultTable = BarrageTable.make(tableDefinition, false);
        resultTable.addParentReference(this);

        final MethodDescriptor<FlightData, BarrageMessage> snapshotDescriptor =
                getClientDoExchangeDescriptor(options, resultTable.getWireChunkTypes(), resultTable.getWireTypes(),
                        resultTable.getWireComponentTypes(), new BarrageStreamReader());

        // We need to ensure that the DoExchange RPC does not get attached to the server RPC when this is being called
        // from a Deephaven server RPC thread. If we need to generalize this in the future, we may wrap this logic in a
        // Channel or interceptor; inject the appropriate Context to use; or have the server RPC set a more appropriate
        // Context along the stack.
        final ClientCall<FlightData, BarrageMessage> call;
        final Context previous = Context.ROOT.attach();
        try {
            call = session.channel().newCall(snapshotDescriptor, CallOptions.DEFAULT);
        } finally {
            Context.ROOT.detach(previous);
        }
        observer = (ClientCallStreamObserver<FlightData>) ClientCalls
                .asyncBidiStreamingCall(call, new DoExchangeObserver());

        // Allow the server to send us all commands when there is sufficient bandwidth:
        observer.request(Integer.MAX_VALUE);
    }

    private class DoExchangeObserver implements ClientResponseObserver<FlightData, BarrageMessage> {
        @Override
        public void beforeStart(final ClientCallStreamObserver<FlightData> requestStream) {
            requestStream.disableAutoInboundFlowControl();
        }

        @Override
        public void onNext(final BarrageMessage barrageMessage) {
            if (barrageMessage == null) {
                return;
            }
            try (barrageMessage) {
                final Listener listener = resultTable;
                if (!connected || listener == null) {
                    return;
                }

                // override server-supplied data and regenerate local rowsets
                barrageMessage.rowsAdded = RowSetFactory.fromRange(0, barrageMessage.addColumnData[0].data.size() - 1);
                barrageMessage.rowsIncluded = barrageMessage.rowsAdded.copy();

                listener.handleBarrageMessage(barrageMessage);
            }
        }

        @Override
        public void onError(final Throwable t) {
            log.error().append(BarrageSnapshotImpl.this)
                    .append(": Error detected in subscription: ")
                    .append(t).endl();

            final Listener listener = resultTable;
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
    }

    @Override
    public BarrageTable entireTable() throws InterruptedException {
        if (!connected) {
            throw new UncheckedDeephavenException(
                    this + " is not connected");
        }
        // Send the snapshot request:
        observer.onNext(FlightData.newBuilder()
                .setAppMetadata(ByteStringAccess.wrap(makeRequestInternal(null, null, options)))
                .build());

        observer.onCompleted();

        resultLatch.await();

        if (exceptionWhileSealing == null) {
            return resultTable;
        } else {
            throw new RuntimeException("Error while handling snapshot:", exceptionWhileSealing);
        }
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

        resultTable.sealTable(() -> {
            resultLatch.countDown();
        }, () -> {
            exceptionWhileSealing = new Exception();
            resultLatch.countDown();
        });
    }

    @Override
    public synchronized void close() {
        if (!connected) {
            return;
        }
//        observer.onCompleted();
        cleanup();
    }

    private void cleanup() {
        this.connected = false;
        this.tableHandle.close();
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append("Barrage/ClientSnapshot/").append(logName).append("/")
                .append(System.identityHashCode(this)).append("/");
    }

    private ByteBuffer makeRequestInternal(
            @Nullable final RowSet viewport,
            @Nullable final BitSet columns,
            @Nullable BarrageSubscriptionOptions options) {
        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int colOffset = 0;
        if (columns != null) {
            colOffset = DoGetRequest.createColumnsVector(metadata, columns.toByteArray());
        }
        int vpOffset = 0;
        if (viewport != null) {
            vpOffset = DoGetRequest.createViewportVector(
                    metadata, BarrageProtoUtil.toByteBuffer(viewport));
        }
        int optOffset = 0;
        if (options != null) {
            optOffset = options.appendTo(metadata);
        }

        final int ticOffset = DoGetRequest.createTicketVector(metadata, tableHandle.ticketId().bytes());
        DoGetRequest.startDoGetRequest(metadata);
        DoGetRequest.addColumns(metadata, colOffset);
        DoGetRequest.addViewport(metadata, vpOffset);
        DoGetRequest.addSubscriptionOptions(metadata, optOffset);
        DoGetRequest.addTicket(metadata, ticOffset);
        metadata.finish(DoGetRequest.endDoGetRequest(metadata));

        final FlatBufferBuilder wrapper = new FlatBufferBuilder();
        final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
        wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                wrapper,
                BarrageUtil.FLATBUFFER_MAGIC,
                BarrageMessageType.DoGetRequest,
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
    public static <Options> MethodDescriptor<FlightData, BarrageMessage> getClientDoExchangeDescriptor(
            final Options options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final BarrageMessageConsumer.StreamReader<Options> streamReader) {
        return descriptorFor(
                MethodDescriptor.MethodType.BIDI_STREAMING, FlightServiceGrpc.SERVICE_NAME, "DoExchange",
                ProtoUtils.marshaller(FlightData.getDefaultInstance()),
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
