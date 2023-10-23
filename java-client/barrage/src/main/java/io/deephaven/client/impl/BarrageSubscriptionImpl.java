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
import io.deephaven.engine.exceptions.RequestCancelledException;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.*;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.annotations.VisibleForTesting;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.ScheduledExecutorService;

/**
 * This class is an intermediary helper class that uses a {@code DoExchange} to populate a {@link BarrageTable} using
 * subscription data from a remote server, propagating updates if the request is a subscription.
 * <p>
 * For Subscriptions (refreshing result tables):
 * <p>
 * Users may call {@link #entireTable} or {@link #partialTable} to initiate the gRPC call to the server. These methods
 * return the eventually populated {@code BarrageTable} to the user.
 * <p>
 * If the user wants to ensure that the table is completely populated with an initial state of the remote table prior to
 * using the result they must either set {@code blockUntilComplete} to {@code true} or call
 * {@link #blockUntilComplete()} to ensure that the {@code BarrageTable} is respecting the requested subscription.
 * <p>
 * It is not an error to create derived tables from the {@code BarrageTable} prior to the subscription being complete,
 * as all changes are propagated to downstream tables.
 * <p>
 * For Snapshots (static result tables):
 * <p>
 * Users may call {@link #snapshotEntireTable} or {@link #snapshotPartialTable} to initiate the gRPC call to the server.
 * These methods return the eventually populated {@code BarrageTable} to the user. The user must either set
 * {@code blockUntilComplete} to {@code true} during initiation or call {@link #blockUntilComplete()} to ensure that the
 * {@code BarrageTable} is fully populated before using it. Noting that snapshots are static tables that do not
 * propagate changes on any update graph.
 */
public class BarrageSubscriptionImpl extends ReferenceCountedLivenessNode implements BarrageSubscription {
    private static final Logger log = LoggerFactory.getLogger(BarrageSubscriptionImpl.class);

    private final String logName;
    private final TableHandle tableHandle;
    private final BarrageSubscriptionOptions options;
    private final ClientCallStreamObserver<FlightData> observer;
    private final CheckForCompletion checkForCompletion;
    private final BarrageTable resultTable;

    private boolean subscribed;
    private boolean isSnapshot;

    private volatile Condition completedCondition;
    private volatile int completed;
    private static final AtomicIntegerFieldUpdater<BarrageSubscriptionImpl> COMPLETED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BarrageSubscriptionImpl.class, "completed");
    private volatile boolean completedSuccessfully;
    private volatile Throwable exceptionWhileCompleting;

    private volatile int connected = 1;
    private static final AtomicIntegerFieldUpdater<BarrageSubscriptionImpl> CONNECTED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BarrageSubscriptionImpl.class, "connected");

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
        this.tableHandle = tableHandle;
        this.options = options;

        final BarrageUtil.ConvertedArrowSchema schema = BarrageUtil.convertArrowSchema(tableHandle.response());
        final TableDefinition tableDefinition = schema.tableDef;
        checkForCompletion = new CheckForCompletion();
        resultTable = BarrageTable.make(executorService, tableDefinition, schema.attributes, checkForCompletion);

        final MethodDescriptor<FlightData, BarrageMessage> subscribeDescriptor =
                getClientDoExchangeDescriptor(options, schema.computeWireChunkTypes(), schema.computeWireTypes(),
                        schema.computeWireComponentTypes(),
                        new BarrageStreamReader(resultTable.getDeserializationTmConsumer()));

        // We need to ensure that the DoExchange RPC does not get attached to the server RPC when this is being called
        // from a Deephaven server RPC thread. If we need to generalize this in the future, we may wrap this logic in a
        // Channel or interceptor; inject the appropriate Context to use; or have the server RPC set a more appropriate
        // Context along the stack.
        final ClientCall<FlightData, BarrageMessage> call;
        final Context previous = Context.ROOT.attach();
        try {
            call = session.channel().channel().newCall(subscribeDescriptor, CallOptions.DEFAULT);
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
                if (!isConnected()) {
                    GrpcUtil.safelyCancel(observer, "Barrage subscription is closed", null);
                    return;
                }

                resultTable.handleBarrageMessage(barrageMessage);
            }
        }

        @Override
        public void onError(final Throwable t) {
            if (!tryRecordDisconnect()) {
                return;
            }

            log.error().append(BarrageSubscriptionImpl.this)
                    .append(": Error detected in subscription: ")
                    .append(t).endl();

            resultTable.handleBarrageError(t);
            cleanup();
        }

        @Override
        public void onCompleted() {
            if (!tryRecordDisconnect()) {
                return;
            }

            log.error().append(BarrageSubscriptionImpl.this).append(": unexpectedly closed by other host").endl();
            resultTable.handleBarrageError(new RequestCancelledException("Barrage subscription closed by server"));
            cleanup();
        }
    }

    @Override
    public boolean isCompleted() {
        return completedSuccessfully || exceptionWhileCompleting != null;
    }

    @Override
    public Table entireTable() throws InterruptedException {
        return entireTable(true);
    }

    @Override
    public Table entireTable(boolean blockUntilComplete) throws InterruptedException {
        return partialTable(null, null, false, blockUntilComplete);
    }

    @Override
    public Table partialTable(RowSet viewport, BitSet columns) throws InterruptedException {
        return partialTable(viewport, columns, false, true);
    }

    @Override
    public Table partialTable(RowSet viewport, BitSet columns, boolean reverseViewport)
            throws InterruptedException {
        return partialTable(viewport, columns, reverseViewport, true);
    }


    @Override
    public Table snapshotEntireTable() throws InterruptedException {
        return snapshotEntireTable(true);
    }

    @Override
    public Table snapshotEntireTable(boolean blockUntilComplete) throws InterruptedException {
        return snapshotPartialTable(null, null, false, blockUntilComplete);
    }

    @Override
    public Table snapshotPartialTable(RowSet viewport, BitSet columns) throws InterruptedException {
        return snapshotPartialTable(viewport, columns, false, true);
    }

    @Override
    public Table snapshotPartialTable(RowSet viewport, BitSet columns, boolean reverseViewport)
            throws InterruptedException {
        return snapshotPartialTable(viewport, columns, reverseViewport, true);
    }

    @Override
    public Table snapshotPartialTable(RowSet viewport, BitSet columns, boolean reverseViewport,
            boolean blockUntilComplete) throws InterruptedException {
        isSnapshot = true;
        return partialTable(viewport, columns, reverseViewport, blockUntilComplete);
    }

    @Override
    public Table partialTable(RowSet viewport, BitSet columns, boolean reverseViewport,
            boolean blockUntilComplete) throws InterruptedException {
        synchronized (this) {
            if (!isConnected()) {
                throw new UncheckedDeephavenException(
                        this + " is no longer an active subscription and cannot be retained further");
            }
            if (subscribed) {
                throw new UncheckedDeephavenException(
                        "Barrage subscription objects cannot be reused.");
            }
            subscribed = true;
        }

        checkForCompletion.setExpected(
                viewport == null ? null : viewport.copy(),
                columns == null ? null : (BitSet) (columns.clone()),
                reverseViewport);

        if (!isSnapshot) {
            resultTable.addSourceToRegistrar();
            resultTable.addParentReference(this);
        }

        // Send the initial subscription:
        observer.onNext(FlightData.newBuilder()
                .setAppMetadata(ByteStringAccess.wrap(makeRequestInternal(
                        viewport, columns, reverseViewport, options, tableHandle.ticketId().bytes())))
                .build());

        if (blockUntilComplete) {
            return blockUntilComplete();
        }

        return resultTable;
    }

    private boolean checkIfCompleteOrThrow() {
        if (exceptionWhileCompleting != null) {
            throw new UncheckedDeephavenException("Error while handling subscription", exceptionWhileCompleting);
        }
        return completedSuccessfully;
    }

    @Override
    public Table blockUntilComplete() throws InterruptedException {
        if (checkIfCompleteOrThrow()) {
            return resultTable;
        }

        // test lock conditions
        if (resultTable.getUpdateGraph().sharedLock().isHeldByCurrentThread()) {
            throw new UnsupportedOperationException(
                    "Cannot wait for subscription to complete while holding the UpdateGraph shared lock");
        }

        final boolean holdingUpdateGraphLock = resultTable.getUpdateGraph().exclusiveLock().isHeldByCurrentThread();
        if (completedCondition == null && holdingUpdateGraphLock) {
            synchronized (this) {
                if (completedCondition == null) {
                    completedCondition = resultTable.getUpdateGraph().exclusiveLock().newCondition();
                }
            }
        }

        if (holdingUpdateGraphLock) {
            while (!checkIfCompleteOrThrow()) {
                completedCondition.await();
            }
        } else {
            synchronized (this) {
                while (!checkIfCompleteOrThrow()) {
                    wait(); // BarrageSubscriptionImpl lock
                }
            }
        }

        return resultTable;
    }

    private boolean isConnected() {
        return connected == 1;
    }

    private boolean tryRecordDisconnect() {
        return CONNECTED_UPDATER.compareAndSet(this, 1, 0);
    }

    private void trySignalCompletion() {
        trySignalCompletion(null);
    }

    private void trySignalCompletion(@Nullable final Throwable t) {
        if (!COMPLETED_UPDATER.compareAndSet(BarrageSubscriptionImpl.this, 0, 1)) {
            return;
        }

        if (t != null) {
            exceptionWhileCompleting = t;
        } else {
            completedSuccessfully = true;
        }

        // if we are building a snapshot via a growing viewport subscription, then cancel our subscription
        if (isSnapshot) {
            tryRecordDisconnect();
            GrpcUtil.safelyCancel(observer, "Barrage snapshot is complete", null);
        }

        final Condition localCondition = completedCondition;
        if (localCondition != null) {
            resultTable.getUpdateGraph().requestSignal(localCondition);
        }
        synchronized (this) {
            notifyAll();
        }
    }

    @Override
    protected void destroy() {
        super.destroy();
        cancel();
    }

    @Override
    public void cancel() {
        if (!tryRecordDisconnect()) {
            return;
        }

        resultTable.handleBarrageError(new RequestCancelledException("Barrage subscription cancelled by client"));
        GrpcUtil.safelyCancel(observer, "Barrage subscription is cancelled", null);
        cleanup();
    }

    private void cleanup() {
        tableHandle.close();
    }

    @Override
    public LogOutput append(final LogOutput logOutput) {
        return logOutput.append("Barrage/ClientSubscription/").append(logName).append("/")
                .append(System.identityHashCode(this)).append("/");
    }

    @VisibleForTesting
    static public ByteBuffer makeRequestInternal(
            @Nullable final RowSet viewport,
            @Nullable final BitSet columns,
            boolean reverseViewport,
            @Nullable BarrageSubscriptionOptions options,
            byte @NotNull [] ticketId) {

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

        final int ticOffset = BarrageSubscriptionRequest.createTicketVector(metadata, ticketId);
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
        final MethodDescriptor.Marshaller<FlightData> requestMarshaller =
                ProtoUtils.marshaller(FlightData.getDefaultInstance());
        final MethodDescriptor<?, ?> descriptor = FlightServiceGrpc.getDoExchangeMethod();

        return MethodDescriptor.<FlightData, BarrageMessage>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                .setFullMethodName(descriptor.getFullMethodName())
                .setSampledToLocalTracing(false)
                .setRequestMarshaller(requestMarshaller)
                .setResponseMarshaller(
                        new BarrageDataMarshaller(options, columnChunkTypes, columnTypes, componentTypes, streamReader))
                .setSchemaDescriptor(descriptor.getSchemaDescriptor())
                .build();
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

    private class CheckForCompletion implements BarrageTable.ViewportChangedCallback {
        private RowSet expectedViewport;
        private BitSet expectedColumns;
        private boolean expectedReverseViewport;

        private synchronized void setExpected(RowSet viewport, BitSet columns, boolean reverseViewport) {
            expectedViewport = viewport == null ? null : viewport.copy();
            expectedColumns = columns == null ? null : (BitSet) (columns.clone());
            expectedReverseViewport = reverseViewport;
        }

        @Override
        public synchronized boolean viewportChanged(
                @Nullable final RowSet serverViewport,
                @Nullable final BitSet serverColumns,
                final boolean serverReverseViewport) {
            if (isCompleted()) {
                return false;
            }

            // @formatter:off
            final boolean correctColumns =
                    // all columns are expected
                    (expectedColumns == null
                        && (serverColumns == null || serverColumns.cardinality() == resultTable.numColumns()))
                    // only specific set of columns are expected
                    || (expectedColumns != null && expectedColumns.equals(serverColumns));

            final boolean isComplete = exceptionWhileCompleting != null
                    // Full subscription is completed
                    || (correctColumns && expectedViewport == null && serverViewport == null)
                    // Viewport subscription is completed
                    || (correctColumns && expectedViewport != null
                        && expectedReverseViewport == resultTable.getServerReverseViewport()
                        && expectedViewport.equals(serverViewport));
            // @formatter:on

            if (isComplete) {
                // remove all unpopulated rows from viewport snapshots
                if (isSnapshot && serverViewport != null) {
                    // noinspection resource
                    WritableRowSet currentRowSet = resultTable.getRowSet().writableCast();
                    try (final RowSet populated =
                            currentRowSet.subSetForPositions(serverViewport, serverReverseViewport)) {
                        currentRowSet.retain(populated);
                    }
                }

                trySignalCompletion();
            }

            return !isComplete;
        }

        @Override
        public void onError(Throwable t) {
            trySignalCompletion(t);
        }
    }
}
