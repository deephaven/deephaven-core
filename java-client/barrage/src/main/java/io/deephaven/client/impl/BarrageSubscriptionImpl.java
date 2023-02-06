/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.barrage.flatbuf.BarrageMessageWrapper;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.BarrageMessage.Listener;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.*;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.ReferentialIntegrity;
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
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ScheduledExecutorService;

public class BarrageSubscriptionImpl extends ReferenceCountedLivenessNode implements BarrageSubscription {
    private static final Logger log = LoggerFactory.getLogger(BarrageSubscriptionImpl.class);

    private final String logName;
    private final TableHandle tableHandle;
    private final BarrageSubscriptionOptions options;
    private final ClientCallStreamObserver<FlightData> observer;

    private BarrageTable resultTable;

    private volatile Condition completedCondition;
    private volatile boolean completed = false;
    private volatile long rowsReceived = 0L;
    private volatile Throwable exceptionWhileCompleting = null;
    private InstrumentedTableUpdateListener listener = null;

    private boolean subscribed = false;
    private volatile boolean connected = true;
    private boolean isSnapshot = false;

    /**
     * Represents a BarrageSubscription.
     *
     * @param session the Deephaven session that this export belongs to
     * @param executorService an executor service used to flush stats
     * @param tableHandle the tableHandle to subscribe to (ownership is transferred to the subscription)
     * @param options the transport level options for this subscription
     */
    public BarrageSubscriptionImpl(
            final BarrageSession session, final ScheduledExecutorService executorService,
            final TableHandle tableHandle, final BarrageSubscriptionOptions options) {
        super(false);

        this.logName = tableHandle.exportId().toString();
        this.options = options;
        this.tableHandle = tableHandle;

        final BarrageUtil.ConvertedArrowSchema schema = BarrageUtil.convertArrowSchema(tableHandle.response());
        final TableDefinition tableDefinition = schema.tableDef;
        resultTable = BarrageTable.make(executorService, tableDefinition, schema.attributes, -1);
        resultTable.addParentReference(this);

        final MethodDescriptor<FlightData, BarrageMessage> subscribeDescriptor =
                getClientDoExchangeDescriptor(options, resultTable.getWireChunkTypes(), resultTable.getWireTypes(),
                        resultTable.getWireComponentTypes(),
                        new BarrageStreamReader(resultTable.getDeserializationTmConsumer()));

        // We need to ensure that the DoExchange RPC does not get attached to the server RPC when this is being called
        // from a Deephaven server RPC thread. If we need to generalize this in the future, we may wrap this logic in a
        // Channel or interceptor; inject the appropriate Context to use; or have the server RPC set a more appropriate
        // Context along the stack.
        final ClientCall<FlightData, BarrageMessage> call;
        final Context previous = Context.ROOT.attach();
        try {
            call = session.channel().newCall(subscribeDescriptor, CallOptions.DEFAULT);
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

                rowsReceived += barrageMessage.rowsIncluded.size();

                listener.handleBarrageMessage(barrageMessage);
            }
        }

        @Override
        public void onError(final Throwable t) {
            log.error().append(BarrageSubscriptionImpl.this)
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
    public boolean isCompleted() {
        return completed;
    }

    @Override
    public long getRowsReceived() {
        return rowsReceived;
    }

    @Override
    public BarrageTable entireTable() throws InterruptedException {
        return entireTable(true);
    }

    @Override
    public BarrageTable entireTable(boolean blockUntilComplete) throws InterruptedException {
        return partialTable(null, null, false, blockUntilComplete);
    }

    @Override
    public BarrageTable partialTable(RowSet viewport, BitSet columns) throws InterruptedException {
        return partialTable(viewport, columns, false, true);
    }

    @Override
    public BarrageTable partialTable(RowSet viewport, BitSet columns, boolean reverseViewport)
            throws InterruptedException {
        return partialTable(viewport, columns, reverseViewport, true);
    }

    @Override
    public synchronized BarrageTable partialTable(RowSet viewport, BitSet columns, boolean reverseViewport,
            boolean blockUntilComplete) throws InterruptedException {
        if (!connected) {
            throw new UncheckedDeephavenException(
                    this + " is no longer an active subscription and cannot be retained further");
        }
        if (subscribed) {
            throw new UncheckedDeephavenException(
                    "BarrageSubscription objects cannot be reused.");
        } else {
            // test lock conditions
            if (UpdateGraphProcessor.DEFAULT.sharedLock().isHeldByCurrentThread()) {
                throw new UnsupportedOperationException(
                        "Cannot create subscription while holding the UpdateGraphProcessor shared lock");
            }

            if (UpdateGraphProcessor.DEFAULT.exclusiveLock().isHeldByCurrentThread()) {
                completedCondition = UpdateGraphProcessor.DEFAULT.exclusiveLock().newCondition();
            }

            // update the viewport size for initial snapshot completion
            resultTable.setInitialSnapshotViewportRowCount(viewport == null ? -1 : viewport.size());

            // Send the initial subscription:
            observer.onNext(FlightData.newBuilder()
                    .setAppMetadata(
                            ByteStringAccess.wrap(makeRequestInternal(viewport, columns, reverseViewport, options)))
                    .build());
            subscribed = true;

            // use a listener to decide when the table is complete
            listener = new InstrumentedTableUpdateListener("completeness-listener") {
                @ReferentialIntegrity
                final BarrageTable tableRef = resultTable;
                {
                    // Maintain a liveness ownership relationship with resultTable for the lifetime of the
                    // listener
                    manage(tableRef);
                }

                @Override
                protected void destroy() {
                    super.destroy();
                    tableRef.removeUpdateListener(this);
                }

                @Override
                protected void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
                    exceptionWhileCompleting = originalException;
                    if (completedCondition != null) {
                        UpdateGraphProcessor.DEFAULT.requestSignal(completedCondition);
                    } else {
                        synchronized (BarrageSubscriptionImpl.this) {
                            BarrageSubscriptionImpl.this.notifyAll();
                        }
                    }
                }

                @Override
                public void onUpdate(final TableUpdate upstream) {
                    boolean isComplete = false;

                    // test to see if the viewport matches the requested
                    if (viewport == null && resultTable.getServerViewport() == null) {
                        isComplete = true;
                    } else if (viewport != null && resultTable.getServerViewport() != null
                            && reverseViewport == resultTable.getServerReverseViewport()) {
                        isComplete = viewport.subsetOf(resultTable.getServerViewport());
                    }

                    if (isComplete) {
                        if (isSnapshot) {
                            resultTable.sealTable(() -> {
                                // signal that we are closing the connection
                                observer.onCompleted();
                                signalCompletion();
                            }, () -> {
                                exceptionWhileCompleting = new Exception();
                            });
                        } else {
                            signalCompletion();
                        }

                        // no longer need to listen for completion
                        resultTable.removeUpdateListener(this);
                        listener = null;
                    }
                }
            };

            resultTable.addUpdateListener(listener);

            if (blockUntilComplete) {
                while (!completed && exceptionWhileCompleting == null) {
                    // handle the condition where this function may have the exclusive lock
                    if (completedCondition != null) {
                        completedCondition.await();
                    } else {
                        wait(); // barragesubscriptionimpl lock
                    }
                }
            }
        }

        if (exceptionWhileCompleting == null) {
            return resultTable;
        } else {
            throw new UncheckedDeephavenException("Error while handling subscription:", exceptionWhileCompleting);
        }
    }

    private void signalCompletion() {
        completed = true;
        if (completedCondition != null) {
            UpdateGraphProcessor.DEFAULT.requestSignal(completedCondition);
        } else {
            synchronized (BarrageSubscriptionImpl.this) {
                BarrageSubscriptionImpl.this.notifyAll();
            }
        }
    }

    @Override
    public BarrageTable snapshotEntireTable() throws InterruptedException {
        return snapshotEntireTable(true);
    }

    @Override
    public BarrageTable snapshotEntireTable(boolean blockUntilComplete) throws InterruptedException {
        return snapshotPartialTable(null, null, false, blockUntilComplete);
    }

    @Override
    public BarrageTable snapshotPartialTable(RowSet viewport, BitSet columns) throws InterruptedException {
        return snapshotPartialTable(viewport, columns, false, true);
    }

    @Override
    public BarrageTable snapshotPartialTable(RowSet viewport, BitSet columns, boolean reverseViewport)
            throws InterruptedException {
        return snapshotPartialTable(viewport, columns, reverseViewport, true);
    }

    @Override
    public synchronized BarrageTable snapshotPartialTable(RowSet viewport, BitSet columns, boolean reverseViewport,
            boolean blockUntilComplete) throws InterruptedException {
        isSnapshot = true;
        return partialTable(viewport, columns, reverseViewport, blockUntilComplete);
    }

    @Override
    protected void destroy() {
        super.destroy();
        close();
    }

    private void handleDisconnect() {
        if (!connected) {
            return;
        }
        // log an error only when doing a true subscription (not snapshot)
        if (!isSnapshot) {
            log.error().append(this).append(": unexpectedly closed by other host").endl();
        }
        cleanup();
    }

    @Override
    public synchronized void close() {
        if (!connected) {
            return;
        }
        GrpcUtil.safelyComplete(observer);
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
            @Nullable final RowSet viewport,
            @Nullable final BitSet columns,
            boolean reverseViewport,
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

        final int ticOffset = BarrageSubscriptionRequest.createTicketVector(metadata, tableHandle.ticketId().bytes());
        BarrageSubscriptionRequest.startBarrageSubscriptionRequest(metadata);
        BarrageSubscriptionRequest.addColumns(metadata, colOffset);
        BarrageSubscriptionRequest.addViewport(metadata, vpOffset);
        BarrageSubscriptionRequest.addSubscriptionOptions(metadata, optOffset);
        BarrageSubscriptionRequest.addTicket(metadata, ticOffset);
        BarrageSubscriptionRequest.addReverseViewport(metadata, reverseViewport);
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
     * @return the client side method descriptor
     */
    public static MethodDescriptor<FlightData, BarrageMessage> getClientDoExchangeDescriptor(
            final BarrageSubscriptionOptions options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final StreamReader streamReader) {
        return descriptorFor(
                MethodDescriptor.MethodType.BIDI_STREAMING, FlightServiceGrpc.SERVICE_NAME, "DoExchange",
                ProtoUtils.marshaller(FlightData.getDefaultInstance()),
                new BarrageDataMarshaller(options, columnChunkTypes, columnTypes, componentTypes, streamReader),
                FlightServiceGrpc.getDoExchangeMethod());
    }

    public static class BarrageDataMarshaller implements MethodDescriptor.Marshaller<BarrageMessage> {
        private final BarrageSubscriptionOptions options;
        private final ChunkType[] columnChunkTypes;
        private final Class<?>[] columnTypes;
        private final Class<?>[] componentTypes;
        private final StreamReader streamReader;

        public BarrageDataMarshaller(
                final BarrageSubscriptionOptions options,
                final ChunkType[] columnChunkTypes,
                final Class<?>[] columnTypes,
                final Class<?>[] componentTypes,
                final StreamReader streamReader) {
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
            return streamReader.safelyParseFrom(options, null, columnChunkTypes, columnTypes, componentTypes, stream);
        }
    }
}
