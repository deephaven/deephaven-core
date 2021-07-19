/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.example;

import com.google.protobuf.ByteString;
import io.deephaven.barrage.flatbuf.Field;
import io.deephaven.barrage.flatbuf.KeyValue;
import io.deephaven.barrage.flatbuf.Schema;
import io.deephaven.base.formatters.FormatBitSet;
import io.deephaven.db.tables.TableDefinition;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.v2.InstrumentedShiftAwareListener;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.grpc_api.barrage.BarrageClientSubscription;
import io.deephaven.grpc_api.barrage.BarrageStreamReader;
import io.deephaven.grpc_api.barrage.util.BarrageSchemaUtil;
import io.deephaven.grpc_api.runner.DeephavenApiServerModule;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.Scheduler;
import io.deephaven.grpc_api_client.table.BarrageSourcedTable;
import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.BarrageServiceGrpc;
import io.deephaven.proto.backplane.grpc.BatchTableRequest;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportNotificationRequest;
import io.deephaven.proto.backplane.grpc.ExportedTableCreationResponse;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdatesRequest;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SelectOrUpdateRequest;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SubscriptionRequest;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc;
import io.deephaven.proto.backplane.grpc.TimeTableRequest;
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
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;

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
    private final FlightServiceGrpc.FlightServiceStub flightService;

    private volatile UUID session;
    private volatile String sessionHeader;
    private Metadata.Key<String> sessionHeaderKey;

    private SimpleDeephavenClient(final Scheduler scheduler, final ManagedChannel managedChannel) {
        this.scheduler = scheduler;
        this.serverChannel = ClientInterceptors.intercept(managedChannel, new AuthInterceptor());
        this.sessionService = SessionServiceGrpc.newStub(serverChannel);
        this.tableService = TableServiceGrpc.newStub(serverChannel);
        this.barrageService = BarrageServiceGrpc.newStub(serverChannel);
        this.flightService = FlightServiceGrpc.newStub(serverChannel);
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

    private int nextTableId = 0;
    private int nextExportId() {
        return ++nextTableId;
    }

    final Flight.Ticket exportTable = ExportTicketHelper.exportIdToTicket(nextExportId());
    final Flight.Ticket putResultTicket = ExportTicketHelper.exportIdToTicket(nextExportId());

    BarrageSourcedTable resultTable;
    BarrageClientSubscription resultSub;
    final BarrageStreamReader reader = new BarrageStreamReader();

    private void runScript() {
        log.info().append("Script Running: ").endl();

        sessionService.exportNotifications(ExportNotificationRequest.getDefaultInstance(),
                new ResponseBuilder<ExportNotification>()
                        .onNext(m -> onExportNotificationMessage("global: ", m))
                        .onError((err) -> log.error().append("export notification listener error: ").append(err).endl())
                        .onComplete(() -> log.info().append("export notification listener completed").endl())
                        .build());

        tableService.exportedTableUpdates(ExportedTableUpdatesRequest.getDefaultInstance(),
                new ResponseBuilder<ExportedTableUpdateMessage>()
                        .onNext(this::onTableUpdate)
                        .onError((err) -> log.error().append("table update listener error: ").append(err).endl())
                        .onComplete(() -> log.info().append("table update listener completed").endl())
                        .build());

        tableService.batch(BatchTableRequest.newBuilder()
                .addOps(BatchTableRequest.Operation.newBuilder().setTimeTable(TimeTableRequest.newBuilder()
                        .setPeriodNanos(1_000_000_000)))
                .addOps(BatchTableRequest.Operation.newBuilder().setTimeTable(TimeTableRequest.newBuilder()
                        .setPeriodNanos(12_000_000)))
                .addOps(BatchTableRequest.Operation.newBuilder().setUpdate(SelectOrUpdateRequest.newBuilder()
                        .addColumnSpecs("I = i")
                        .addColumnSpecs("II = ii")
                        .addColumnSpecs("S = `` + i")
                        .setResultId(exportTable)
                        .setSourceId(TableReference.newBuilder().setBatchOffset(1).build())
                        .build()))
                .build(), new ResponseBuilder<ExportedTableCreationResponse>()
                .onError(this::onError)
                .onNext(this::onExportedTableCreationResponse)
                .onComplete(() -> log.info().append("Batch Complete"))
                .build());

        flightService.getSchema(
                ExportTicketHelper.ticketToDescriptor(exportTable),
                new ResponseBuilder<Flight.SchemaResult>()
                        .onError(this::onError)
                        .onNext(this::onSchemaResult)
                        .build());

        final StreamObserver<Flight.FlightData> putObserver = flightService.doPut(new ResponseBuilder<Flight.PutResult>()
                .onError(this::onError)
                .onComplete(() -> log.info().append("Flight PUT Complete").endl())
                .build());
        flightService.doGet(exportTable, new ResponseBuilder<Flight.FlightData>()
                .onError(this::onError)
                .onNext(data -> {
                    log.info().append("DoGet Recv Payload").endl();
                    putObserver.onNext(data);
                })
                .onComplete(putObserver::onCompleted)
                .build());
    }

    private void onSchemaResult(final Flight.SchemaResult schemaResult) {
        final Schema schema = Schema.getRootAsSchema(schemaResult.getSchema().asReadOnlyByteBuffer());
        final TableDefinition definition = BarrageSchemaUtil.schemaToTableDefinition(schema);

        // Note: until subscriptions move to flatbuffer, we cannot distinguish between the all-inclusive non-existing-bitset and an empty bitset.
        final BitSet columns = new BitSet();
        columns.set(0, definition.getColumns().length);

        resultTable = BarrageSourcedTable.make(definition, false);
        final InstrumentedShiftAwareListener listener = new InstrumentedShiftAwareListener("test") {
            @Override
            protected void onFailureInternal(final Throwable originalException, final UpdatePerformanceTracker.Entry sourceEntry) {
                SimpleDeephavenClient.this.onError(originalException);
            }

            @Override
            public void onUpdate(final Update update) {
                log.info().append("received update: ").append(update).endl();
            }
        };
        resultTable.listenForUpdates(listener);

        resultSub = new BarrageClientSubscription(
                ExportTicketHelper.toReadableString(exportTable),
                serverChannel, exportTable, SubscriptionRequest.newBuilder()
                .setTicket(exportTable)
                .setColumns(BarrageProtoUtil.toByteString(columns))
                .setUseDeephavenNulls(true)
                .build(), reader, resultTable);
    }

    private void onSchemaResultNoLocalLTM(final Flight.SchemaResult schemaResult) {
        final Schema schema = Schema.getRootAsSchema(schemaResult.getSchema().asReadOnlyByteBuffer());
        final TableDefinition definition = BarrageSchemaUtil.schemaToTableDefinition(schema);

        // Note: until subscriptions move to flatbuffer, we cannot distinguish between the all-inclusive non-existing-bitset and an empty bitset.
        final BitSet columns = new BitSet();
        columns.set(0, definition.getColumns().length);

        BarrageSourcedTable dummy = BarrageSourcedTable.make(definition, false);

        resultSub = new BarrageClientSubscription(
                ExportTicketHelper.toReadableString(exportTable),
                serverChannel, exportTable, SubscriptionRequest.newBuilder()
                .setTicket(exportTable)
                .setColumns(BarrageProtoUtil.toByteString(columns))
                .setUseDeephavenNulls(true)
                .build(), reader,
                dummy.getWireChunkTypes(),
                dummy.getWireTypes(),
                dummy.getWireComponentTypes(),
                new WeakReference<>(new BarrageMessage.Listener() {
                    @Override
                    public void handleBarrageMessage(final BarrageMessage update) {
                        final Index mods = Index.CURRENT_FACTORY.getEmptyIndex();
                        for (int ci = 0; ci < update.modColumnData.length; ++ci) {
                            mods.insert(update.modColumnData[ci].rowsModified);
                        }
                        final ShiftAwareListener.Update up = new ShiftAwareListener.Update(
                                update.rowsAdded, update.rowsRemoved, mods, update.shifted, ModifiedColumnSet.ALL);

                        log.info().append("recv update: ").append(up).endl();
                    }

                    @Override
                    public void handleBarrageError(Throwable t) {
                        log.error().append("upstream client failed: " + t.getMessage());
                    }
                }));
    }

    private void onScriptComplete() {
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
            entry.append("exportId: ").append(ExportTicketHelper.ticketToExportId(result.getResultId().getTicket()));
        } else {
            entry.append("batchOffset: ").append(result.getResultId().getBatchOffset());
        }

        entry.append(", columns: {\n");
        Schema schema = Schema.getRootAsSchema(result.getSchemaHeader().asReadOnlyByteBuffer());
        for (int i = 0; i < schema.fieldsLength(); i++) {
            Field field = schema.fields(i);
            entry.append("\t");
            entry.append(field.name()).append(" : ").append(field.typeType()).append("\n");
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
                .append(ExportTicketHelper.ticketToExportId(notification.getTicket()))
                .append(", state: ").append(notification.getExportState().toString());

        if (!notification.getContext().isEmpty()) {
            entry.append(", context: '").append(notification.getContext()).append("'");
        }
        if (!notification.getDependentHandle().isEmpty()) {
            entry.append(", dependent: '").append(notification.getDependentHandle()).append("'");
        }
        entry.append("}").endl();
    }

    private void onTableUpdate(final ExportedTableUpdateMessage msg) {
        log.info().append("Received ExportedTableUpdatedMessage:").endl();

        final LogEntry entry = log.info().append("\tid=")
                .append(ExportTicketHelper.ticketToExportId(msg.getExportId()))
                .append(" size=").append(msg.getSize());

        if (!msg.getUpdateFailureMessage().isEmpty()) {
            entry.append(" error='").append(msg.getUpdateFailureMessage()).append("'");
        }

        entry.endl();
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
