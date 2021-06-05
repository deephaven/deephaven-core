package io.deephaven.grpc_api.example;

import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.logger.Logger;
import com.google.protobuf.ByteString;
import io.deephaven.db.backplane.barrage.BarrageMessage;
import io.deephaven.db.backplane.util.BarrageProtoUtil;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.grpc_api.barrage.BarrageClientSubscription;
import io.deephaven.grpc_api.barrage.BarrageStreamReader;
import io.deephaven.grpc_api.runner.DeephavenApiServerModule;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.proto.backplane.grpc.BarrageServiceGrpc;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportNotificationRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateBatchMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.OutOfBandSubscriptionResponse;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SubscriptionRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
import io.deephaven.barrage.flatbuf.Field;
import io.deephaven.barrage.flatbuf.KeyValue;
import io.deephaven.barrage.flatbuf.Schema;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.stub.StreamObserver;

import java.lang.ref.WeakReference;
import java.util.BitSet;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

public class SimpleDeephavenClient {
    private static final Logger log = LoggerFactory.getLogger(SimpleDeephavenClient.class);

    public static void main(final String[] args) throws Exception {
        // Assign properties that need to be set to even turn on
        System.setProperty("Configuration.rootFile", "grpc-api.prop");
        System.setProperty("io.deephaven.configuration.PropertyInputStreamLoader.override", "io.deephaven.configuration.PropertyInputStreamLoaderTraditional");

        final String target = args.length == 0 ? "localhost:8080" : args[0];
        final ManagedChannel channel = ManagedChannelBuilder.forTarget(target)
                .usePlaintext()
                .build();

        final Scheduler scheduler = DeephavenApiServerModule.provideScheduler(2);
        final SimpleDeephavenClient client = new SimpleDeephavenClient(scheduler, channel);
        Runtime.getRuntime().addShutdownHook(new Thread(client::stop));

        client.start();
        client.blockUntilShutdown();
    }

    private final Scheduler scheduler;
    private final Channel serverChannel;
    private final CountDownLatch shutdownRequested = new CountDownLatch(1);

    private final SessionServiceGrpc.SessionServiceStub sessionService;
    private final TableServiceGrpc.TableServiceStub tableService;
    private final BarrageServiceGrpc.BarrageServiceStub barrageService;

    private volatile UUID session;
    private volatile String sessionHeader;
    private Metadata.Key<String> sessionHeaderKey;

    private SimpleDeephavenClient(final Scheduler scheduler, final ManagedChannel managedChannel) {
        this.scheduler = scheduler;
        this.serverChannel = ClientInterceptors.intercept(managedChannel, new AuthInterceptor());
        this.sessionService = SessionServiceGrpc.newStub(serverChannel);
        this.tableService = TableServiceGrpc.newStub(serverChannel);
        this.barrageService = BarrageServiceGrpc.newStub(serverChannel);
    }

    private void start() {
        LiveTableMonitor.DEFAULT.start();

        // no payload in this simple server auth
        sessionService.newSession(HandshakeRequest.newBuilder().setAuthProtocol(1).build(),
                new ResponseBuilder<HandshakeResponse>()
                        .onError(this::onError)
                        .onComplete(this::runScript)
                        .onNext(this::onNewHandshakeResponse)
                        .build());
    }

    private void stop() {
        shutdownRequested.countDown();
    }

    private void blockUntilShutdown() throws InterruptedException {
        shutdownRequested.await();
    }

    private long nextTableId = 0;
    private long nextExportId() {
        return ++nextTableId;
    }

    private void runScript() {
        log.info().append("Script Running: ").endl();

        sessionService.exportNotifications(ExportNotificationRequest.getDefaultInstance(),
                new ResponseBuilder<ExportNotification>()
                        .onNext(m -> onExportNotificationMessage("global: ", m))
                        .onError((err) -> log.error().append("export notification listener error: ").append(err).endl())
                        .onComplete(() -> log.error().append("export notification listener completed").endl())
                        .build());

        tableService.exportedTableUpdates(ExportedTableUpdatesRequest.getDefaultInstance(),
                new ResponseBuilder<ExportedTableUpdateBatchMessage>()
                        .onNext(this::onTableUpdateBatch)
                        .onError((err) -> log.error().append("table update listener error: ").append(err).endl())
                        .onComplete(() -> log.error().append("table update listener completed").endl())
                        .build());

        final Ticket exportTable = SessionState.exportIdToTicket(nextExportId());
        final Ticket t2 = SessionState.exportIdToTicket(nextExportId());

        tableService.batch(BatchTableRequest.newBuilder()
                .addOps(BatchTableRequest.Operation.newBuilder().setTimeTable(TimeTableRequest.newBuilder()
                        .setPeriodNanos(1_000_000_000)))
                .addOps(BatchTableRequest.Operation.newBuilder().setTimeTable(TimeTableRequest.newBuilder()
                        .setPeriodNanos(12_000_000)
                        .setResultId(t2)))
                .addOps(BatchTableRequest.Operation.newBuilder().setUpdate(SelectOrUpdateRequest.newBuilder()
                        .setResultId(exportTable)
                        .setSourceId(TableReference.newBuilder().setBatchOffset(0))
                        .addColumnSpecs("key = i")))
                .build(), new ResponseBuilder<ExportedTableCreationResponse>()
                .onError(this::onError)
                .onNext(this::onExportedTableCreationResponse)
                .onComplete(() -> log.info().append("Batch Complete"))
                .build());

        final BarrageMessage.Listener listener = message -> log.info().append("Received BarrageMessage: {")
                .append("FirstSeq: ").append(message.firstSeq)
                .append(", LastSeq: ").append(message.lastSeq)
                .append(", IsSnapshot: ").append(message.isSnapshot)
                .append(", SnapIndex: ").append(message.snapshotIndex)
                .append(", SnapColumns: ").append(str(message.snapshotColumns))
                .append(", RowsAdded: ").append(message.rowsAdded)
                .append(", RowsRemoved: ").append(message.rowsRemoved)
                .append(", RowsIncluded: ").append(message.rowsIncluded)
                .append(", IndexShiftData: ").append(message.shifted.toString())
                .append("}").endl();

        final BitSet columns = new BitSet(1);
        columns.set(0);
        final ChunkType[] chunkTypes = new ChunkType[1];
        chunkTypes[0] = ChunkType.Long;
        final Class<?>[] columnTypes = new Class[1];
        columnTypes[0] = Long.class;

        final BarrageStreamReader reader = new BarrageStreamReader();
        final BarrageClientSubscription s1 = new BarrageClientSubscription(
                serverChannel, exportTable, SubscriptionRequest.newBuilder()
                .setTicket(exportTable)
                .setColumns(BarrageProtoUtil.toByteString(columns))
                .setUseDeephavenNulls(true)
                .build(), reader,  chunkTypes, columnTypes, new WeakReference<>(listener));

        final Ticket s2id = SessionState.exportIdToTicket(nextExportId());
        final BarrageClientSubscription s2 = new BarrageClientSubscription(
                serverChannel, exportTable, SubscriptionRequest.newBuilder()
                .setSequence(1)
                .setTicket(exportTable)
                .setExportId(s2id)
                .setViewport(BarrageProtoUtil.toByteString(Index.FACTORY.getIndexByRange(20, 40)))
                .setColumns(BarrageProtoUtil.toByteString(columns))
                .setUseDeephavenNulls(true)
                .build(), reader, chunkTypes, columnTypes, new WeakReference<>(listener));

        safeSleep(5_000);
        barrageService.doUpdateSubscription(
                SubscriptionRequest.newBuilder()
                        .setSequence(2)
                        .setExportId(s2id)
                        .setViewport(BarrageProtoUtil.toByteString(Index.FACTORY.getIndexByRange(40, 60)))
                        .setColumns(BarrageProtoUtil.toByteString(columns))
                        .build(),
                new ResponseBuilder<OutOfBandSubscriptionResponse>()
                        .onError(this::onError)
                        .onNext(r -> log.info().append("update s2 response ").append(r.toString()).endl())
                        .build());

        sessionService.exportNotifications(ExportNotificationRequest.getDefaultInstance(),
                new ResponseBuilder<ExportNotification>()
                        .onNext(m -> onExportNotificationMessage2("refresh1: ", m))
                        .build());

        safeSleep(5_000);
        sessionService.release(s2id, new ResponseBuilder<ReleaseResponse>()
                .onError(this::onError)
                .onNext(r -> log.info().append("release s2 response ").append(r.toString()).endl())
                .build());
        s2.close();

        safeSleep(5_000);
        sessionService.release(t2, new ResponseBuilder<ReleaseResponse>()
                .onError(this::onError)
                .onNext((response) -> log.info().append("t2 has been released: ").append(response.toString()).endl())
                .build());

        sessionService.exportNotifications(ExportNotificationRequest.getDefaultInstance(),
                new ResponseBuilder<ExportNotification>()
                        .onNext(m -> onExportNotificationMessage2("refresh2: ", m))
                        .build());

        safeSleep(5_000);
        s1.close();

        log.info().append("Script Complete: shutting down.").endl();
        sessionService.closeSession(HandshakeRequest.newBuilder()
                        .setAuthProtocol(0)
                        .setPayload(ByteString.copyFromUtf8(session.toString())).build(),
                new ResponseBuilder<ReleaseResponse>()
                        .onNext(r -> log.info().append("release session response ").append(r.toString()).endl())
                        .onError(e -> stop())
                        .onComplete(this::stop)
                        .build());
    }

    private void onExportedTableCreationResponse(final ExportedTableCreationResponse result) {
        final LogEntry entry = log.info().append("Received ExportedTableCreationResponse for {");

        if (result.getResultId().hasTicket()) {
            entry.append("exportId: ").append(SessionState.ticketToExportId(result.getResultId().getTicket()));
        } else {
            entry.append("batchOffset: ").append(result.getResultId().getBatchOffset());
        }

        entry.append(", columns: {\n");
        Schema schema = Schema.getRootAsSchema(result.getSchemaHeader().asReadOnlyByteBuffer());
        for (int i = 0; i < schema.fieldsLength(); i++) {
            Field field = schema.fields(i);
            entry.append("\t");
            entry.append(field.name()).append(" : ").append(field.typeType());
            for (int j = 0; j < field.customMetadataLength(); j++) {
                KeyValue keyValue = field.customMetadata(j);
                entry.append("\t\t").append(keyValue.key()).append(" = ").append(keyValue.value());
                entry.append("\n");
            }
            entry.append("\n");
        }
        entry.append("}, attributes: {\n");
        for (int i = 0; i < schema.customMetadataLength(); i++) {
            entry.append("\t");
            KeyValue keyValue = schema.customMetadata(i);
            entry.append(keyValue.key()).append(" = ").append(keyValue.value());
            entry.append("\n");
        }
        entry.append("}\n");

        entry.append("}").endl();
    }

    private void onExportNotificationMessage(final String prefix, final ExportNotification notification) {
        final LogEntry entry = log.info().append(prefix).append("Received ExportNotification: {id: ")
                .append(SessionState.ticketToExportId(notification.getTicket()))
                .append(", state: ").append(notification.getExportState().toString());

        if (!notification.getContext().isEmpty()) {
            entry.append(", context: '").append(notification.getContext()).append("'");
        }
        if (!notification.getDependentHandle().isEmpty()) {
            entry.append(", dependent: '").append(notification.getDependentHandle()).append("'");
        }
        entry.append("}").endl();
    }

    private void onExportNotificationMessage2(final String prefix, final ExportNotification notification) {
        onExportNotificationMessage(prefix, notification);
        if (SessionState.ticketToExportId(notification.getTicket()) == SessionState.NON_EXPORT_ID) {
            log.info().append("Export refresh complete; unsubscribing.").endl();
            throw new RuntimeException("unsubscribe to notifications");
        }
    }

    private void onTableUpdateBatch(final ExportedTableUpdateBatchMessage batch) {
        log.info().append("Received ExportedTableUpdatedBatchMessage:").endl();
        for (final ExportedTableUpdateMessage msg : batch.getUpdatesList()) {
            final LogEntry entry = log.info().append("\tid=").append(SessionState.ticketToExportId(msg.getExportId()))
                    .append(" size=").append(msg.getSize());

            if (!msg.getUpdateFailureMessage().isEmpty()) {
                entry.append(" error='").append(msg.getUpdateFailureMessage()).append("'");
            }

            entry.endl();
        }
    }

    private void onNewHandshakeResponse(final HandshakeResponse result) {
        final String responseHeader = result.getMetadataHeader().toStringUtf8();
        if (sessionHeader == null || !sessionHeader.equals(responseHeader)) {
            sessionHeader = responseHeader;
            sessionHeaderKey = Metadata.Key.of(sessionHeader, Metadata.ASCII_STRING_MARSHALLER);
        }
        session = UUID.fromString(result.getSessionToken().toStringUtf8());
        log.info().append("Session Details: {header: '")
                .append(this.sessionHeader).append("', token: '")
                .append(this.session.toString()).append("}").endl();

        // Guess a good time to do the next refresh.
        final long refreshDelayMs = Math.min(
                scheduler.currentTime().getMillis() + result.getTokenExpirationDelayMillis() / 3,
                result.getTokenDeadlineTimeMillis() - result.getTokenExpirationDelayMillis() / 10);

        scheduler.runAtTime(DBTimeUtils.millisToTime(refreshDelayMs), this::refreshToken);
    }

    private void refreshToken() {
        sessionService.refreshSessionToken(HandshakeRequest.newBuilder()
                        .setAuthProtocol(0)
                        .setPayload(ByteString.copyFromUtf8(session.toString())).build(),
                new ResponseBuilder<HandshakeResponse>()
                        .onError(this::onError)
                        .onNext(this::onNewHandshakeResponse)
                        .build());
    }

    private void onError(final Throwable t) {
        log.error().append("Unexpected error ").append(t).endl();
        stop();
    }

    private String str(final BitSet bs) {
        if (bs == null) {
            return "null";
        }
        return FormatBitSet.formatBitSetAsString(bs);
    }

    private void safeSleep(final long millis) {
        try {
            Thread.sleep(millis);
        } catch (final InterruptedException ignore) {}
    }

    private static class ResponseBuilder<T> {
        private Consumer<T> _onNext;
        private Consumer<Throwable> _onError;
        private Runnable _onComplete;

        public ResponseBuilder<T> onNext(final Consumer<T> _onNext) {
            this._onNext = _onNext;
            return this;
        }

        public ResponseBuilder<T> onError(final Consumer<Throwable> _onError) {
            this._onError = _onError;
            return this;
        }

        public ResponseBuilder<T> onComplete(final Runnable _onComplete) {
            this._onComplete = _onComplete;
            return this;
        }

        public StreamObserver<T> build() {
            return new StreamObserver<T>() {
                @Override
                public void onNext(final T value) {
                    if (_onNext != null) _onNext.accept(value);
                }

                @Override
                public void onError(final Throwable t) {
                    if (_onError != null) _onError.accept(t);
                }

                @Override
                public void onCompleted() {
                    if (_onComplete != null) _onComplete.run();
                }
            };
        }
    }

    private class AuthInterceptor implements ClientInterceptor {
        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                final MethodDescriptor<ReqT, RespT> methodDescriptor, final CallOptions callOptions, final Channel channel) {
            return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(channel.newCall(methodDescriptor, callOptions)) {
                @Override
                public void start(final Listener<RespT> responseListener, final Metadata headers) {
                    final UUID currSession = session;
                    if (currSession != null) {
                        headers.put(sessionHeaderKey, currSession.toString());
                    }
                    super.start(responseListener, headers);
                }
            };
        }
    }
}
