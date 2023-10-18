/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.flatbuffers.FlatBufferBuilder;
import com.google.protobuf.ByteStringAccess;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.barrage.flatbuf.*;
import io.deephaven.base.log.LogOutput;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.liveness.ReferenceCountedLivenessNode;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.table.impl.util.BarrageMessage.Listener;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.table.BarrageTable;
import io.deephaven.extensions.barrage.util.*;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.locks.Condition;

public class BarrageSnapshotImpl extends ReferenceCountedLivenessNode implements BarrageSnapshot {
    private static final Logger log = LoggerFactory.getLogger(BarrageSnapshotImpl.class);

    private final String logName;
    private final TableHandle tableHandle;
    private final BarrageSnapshotOptions options;
    private final ClientCallStreamObserver<FlightData> observer;

    private final BarrageTable resultTable;

    private volatile BitSet expectedColumns;

    private volatile Condition completedCondition;
    private volatile boolean completed = false;
    private volatile Throwable exceptionWhileCompleting = null;

    private volatile boolean connected = true;

    private boolean prevUsed = false;

    /**
     * Represents a BarrageSnapshot.
     *
     * @param session the Deephaven session that this export belongs to
     * @param executorService an executor service used to flush metrics when enabled
     * @param tableHandle the tableHandle to snapshot (ownership is transferred to the snapshot)
     * @param options the transport level options for this snapshot
     */
    public BarrageSnapshotImpl(
            final BarrageSession session, @Nullable final ScheduledExecutorService executorService,
            final TableHandle tableHandle, final BarrageSnapshotOptions options) {
        super(false);

        this.logName = tableHandle.exportId().toString();
        this.options = options;
        this.tableHandle = tableHandle;

        final BarrageUtil.ConvertedArrowSchema schema = BarrageUtil.convertArrowSchema(tableHandle.response());
        final TableDefinition tableDefinition = schema.tableDef;
        resultTable = BarrageTable.make(executorService, tableDefinition, schema.attributes, new CheckForCompletion());
        resultTable.addParentReference(this);

        final MethodDescriptor<FlightData, BarrageMessage> snapshotDescriptor =
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
            call = session.channel().channel().newCall(snapshotDescriptor, CallOptions.DEFAULT);
        } finally {
            Context.ROOT.detach(previous);
        }
        observer = (ClientCallStreamObserver<FlightData>) ClientCalls
                .asyncBidiStreamingCall(call, new DoExchangeObserver());

        // Allow the server to send us all commands when there is sufficient bandwidth:
        observer.request(Integer.MAX_VALUE);
    }

    private class DoExchangeObserver implements ClientResponseObserver<FlightData, BarrageMessage> {
        private long rowsReceived = 0L;

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
                final Listener localResultTable = resultTable;
                if (!connected || localResultTable == null) {
                    return;
                }

                final long resultSize = barrageMessage.rowsIncluded.size();

                // override server-supplied data and regenerate flattened rowsets
                barrageMessage.rowsAdded.close();
                barrageMessage.rowsIncluded.close();
                barrageMessage.rowsAdded = RowSetFactory.fromRange(rowsReceived, rowsReceived + resultSize - 1);
                barrageMessage.rowsIncluded = barrageMessage.rowsAdded.copy();
                try (final RowSet ignored = barrageMessage.snapshotRowSet) {
                    barrageMessage.snapshotRowSet = null;
                }

                rowsReceived += resultSize;

                localResultTable.handleBarrageMessage(barrageMessage);
            }
        }

        @Override
        public void onError(final Throwable t) {
            log.error().append(BarrageSnapshotImpl.this)
                    .append(": Error detected in snapshot: ")
                    .append(t).endl();

            final Listener localResultTable = resultTable;
            if (!connected || localResultTable == null) {
                return;
            }
            localResultTable.handleBarrageError(t);
            handleDisconnect();
        }

        @Override
        public void onCompleted() {
            handleDisconnect();
        }
    }

    @Override
    public BarrageTable entireTable() throws InterruptedException {
        return partialTable(null, null, false, true);
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
    public BarrageTable partialTable(
            RowSet viewport, BitSet columns, boolean reverseViewport) throws InterruptedException {
        return partialTable(viewport, columns, reverseViewport, true);
    }

    @Override
    public BarrageTable partialTable(
            RowSet viewport, BitSet columns, boolean reverseViewport, boolean blockUntilComplete)
            throws InterruptedException {
        synchronized (this) {
            if (!connected) {
                throw new UncheckedDeephavenException(this + " is not connected");
            }

            // notify user when connection has already been used and closed
            if (prevUsed) {
                throw new UnsupportedOperationException("Snapshot object already used");
            }
            prevUsed = true;
        }

        // store this for streamreader parser
        expectedColumns = columns;

        // Send the snapshot request:
        observer.onNext(FlightData.newBuilder()
                .setAppMetadata(ByteStringAccess.wrap(makeRequestInternal(viewport, columns, reverseViewport, options)))
                .build());

        observer.onCompleted();

        if (blockUntilComplete) {
            return blockUntilComplete();
        }

        return resultTable;
    }

    private boolean checkIfCompleteOrThrow() {
        if (exceptionWhileCompleting != null) {
            throw new UncheckedDeephavenException("Error while handling subscription:", exceptionWhileCompleting);
        }
        return completed;
    }

    @Override
    public BarrageTable blockUntilComplete() throws InterruptedException {
        if (checkIfCompleteOrThrow()) {
            return resultTable;
        }

        // test lock conditions
        if (resultTable.getUpdateGraph().sharedLock().isHeldByCurrentThread()) {
            throw new UnsupportedOperationException(
                    "Cannot wait for snapshot to complete while holding the UpdateGraph shared lock");
        }

        final boolean holdingUpdateGraphLock = resultTable.getUpdateGraph().exclusiveLock().isHeldByCurrentThread();
        if (completedCondition == null && holdingUpdateGraphLock) {
            synchronized (this) {
                if (checkIfCompleteOrThrow()) {
                    return resultTable;
                }
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
                    wait(); // BarrageSnapshotImpl lock
                }
            }
        }

        return resultTable;
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

        completed = true;
        signalCompletion();
        cleanup();
    }

    private synchronized void signalCompletion() {
        if (completedCondition != null) {
            resultTable.getUpdateGraph().requestSignal(completedCondition);
        }

        notifyAll();
    }

    @Override
    public void close() {
        if (!connected) {
            return;
        }
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
            boolean reverseViewport,
            @Nullable BarrageSnapshotOptions options) {
        final FlatBufferBuilder metadata = new FlatBufferBuilder();

        int colOffset = 0;
        if (columns != null) {
            colOffset = BarrageSnapshotRequest.createColumnsVector(metadata, columns.toByteArray());
        }
        int vpOffset = 0;
        if (viewport != null) {
            vpOffset = BarrageSnapshotRequest.createViewportVector(
                    metadata, BarrageProtoUtil.toByteBuffer(viewport));
        }
        int optOffset = 0;
        if (options != null) {
            optOffset = options.appendTo(metadata);
        }

        final int ticOffset = BarrageSnapshotRequest.createTicketVector(metadata, tableHandle.ticketId().bytes());
        BarrageSnapshotRequest.startBarrageSnapshotRequest(metadata);
        BarrageSnapshotRequest.addColumns(metadata, colOffset);
        BarrageSnapshotRequest.addViewport(metadata, vpOffset);
        BarrageSnapshotRequest.addSnapshotOptions(metadata, optOffset);
        BarrageSnapshotRequest.addTicket(metadata, ticOffset);
        BarrageSnapshotRequest.addReverseViewport(metadata, reverseViewport);
        metadata.finish(BarrageSnapshotRequest.endBarrageSnapshotRequest(metadata));

        final FlatBufferBuilder wrapper = new FlatBufferBuilder();
        final int innerOffset = wrapper.createByteVector(metadata.dataBuffer());
        wrapper.finish(BarrageMessageWrapper.createBarrageMessageWrapper(
                wrapper,
                BarrageUtil.FLATBUFFER_MAGIC,
                BarrageMessageType.BarrageSnapshotRequest,
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
    public MethodDescriptor<FlightData, BarrageMessage> getClientDoExchangeDescriptor(
            final BarrageSnapshotOptions options,
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

    private class BarrageDataMarshaller implements MethodDescriptor.Marshaller<BarrageMessage> {
        private final BarrageSnapshotOptions options;
        private final ChunkType[] columnChunkTypes;
        private final Class<?>[] columnTypes;
        private final Class<?>[] componentTypes;
        private final StreamReader streamReader;

        public BarrageDataMarshaller(
                final BarrageSnapshotOptions options,
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
            return streamReader.safelyParseFrom(options, expectedColumns, columnChunkTypes, columnTypes, componentTypes,
                    stream);
        }
    }

    private class CheckForCompletion implements BarrageTable.ViewportChangedCallback {
        @Override
        public boolean viewportChanged(@Nullable RowSet rowSet, @Nullable BitSet columns, boolean reverse) {
            return true;
        }

        @Override
        public void onError(Throwable t) {
            exceptionWhileCompleting = t;
            signalCompletion();
        }

        @Override
        public void onClose() {
            signalCompletion();
        }
    }
}
