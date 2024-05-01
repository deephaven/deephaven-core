//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.client.impl.script.Changes;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.backplane.grpc.AddTableRequest;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsRequest;
import io.deephaven.proto.backplane.grpc.AuthenticationConstantsResponse;
import io.deephaven.proto.backplane.grpc.ConfigValue;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.deephaven.proto.backplane.grpc.ConnectRequest;
import io.deephaven.proto.backplane.grpc.DeleteTableRequest;
import io.deephaven.proto.backplane.grpc.ExportRequest;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.PublishRequest;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.StreamRequest;
import io.deephaven.proto.backplane.grpc.StreamResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.qst.table.TableSpec;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.SourceVersion;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * A {@link Session} implementation that uses {@link io.deephaven.proto.backplane.grpc.BatchTableRequest batch requests}
 * and memoizes based on {@link io.deephaven.qst.table.TableSpec} equality.
 *
 * <p>
 * {@inheritDoc}
 */
public final class SessionImpl extends SessionBase {
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

    /**
     * Creates a session. Closing the session does <b>not</b> close the underlying channel.
     *
     * @param config the config
     * @return the session
     * @throws InterruptedException if the thread is interrupted
     */
    public static SessionImpl create(SessionImplConfig config) throws InterruptedException {
        final Authentication authentication =
                Authentication.authenticate(config.channel(), config.authenticationTypeAndValue());
        authentication.awaitOrCancel();
        return create(config, authentication);
    }

    public static SessionImpl create(SessionImplConfig config, Authentication authentication) {
        authentication.throwOnError();
        final DeephavenChannel bearerChannel = authentication.bearerChannel().orElseThrow(IllegalStateException::new);
        final ConfigurationConstantsResponse response =
                authentication.configurationConstants().orElseThrow(IllegalStateException::new);
        final Optional<Duration> httpSessionDuration = parseHttpSessionDuration(response);
        if (!httpSessionDuration.isPresent()) {
            log.warn(
                    "Server did not return an 'http.session.durationMs', defaulting to pinging the server every minute.");
        }
        final Duration pingFrequency = httpSessionDuration.map(d -> d.dividedBy(3)).orElse(Duration.ofMinutes(1));
        return new SessionImpl(config, bearerChannel, pingFrequency, authentication.bearerHandler());
    }

    private static Optional<Duration> parseHttpSessionDuration(ConfigurationConstantsResponse response) {
        return getHttpSessionDurationMs(response).map(SessionImpl::stringValue).flatMap(SessionImpl::parseMillis);
    }

    private static String stringValue(ConfigValue value) {
        if (!value.hasStringValue()) {
            throw new IllegalArgumentException("Expected string value");
        }
        return value.getStringValue();
    }

    private static Optional<ConfigValue> getHttpSessionDurationMs(ConfigurationConstantsResponse response) {
        return Optional.ofNullable(response.getConfigValuesMap().get("http.session.durationMs"));
    }

    private static Optional<Duration> parseMillis(String x) {
        try {
            return Optional.of(Duration.ofMillis(Long.parseLong(x)));
        } catch (NumberFormatException e) {
            return Optional.empty();
        }
    }

    private final SessionImplConfig config;
    private final DeephavenChannel bearerChannel;
    // Needed for downstream flight workarounds
    private final BearerHandler bearerHandler;
    private final ExportTicketCreator exportTicketCreator;
    private final ScheduledFuture<?> pingJob;

    /** Cache the close future, so we only close once. */
    private CompletableFuture<Void> closeFuture;

    private SessionImpl(SessionImplConfig config, DeephavenChannel bearerChannel, Duration pingFrequency,
            BearerHandler bearerHandler) {
        this.config = Objects.requireNonNull(config);
        this.bearerChannel = Objects.requireNonNull(bearerChannel);
        this.bearerHandler = Objects.requireNonNull(bearerHandler);
        this.exportTicketCreator = new ExportTicketCreator();
        this.pingJob = config.executor().scheduleAtFixedRate(
                () -> bearerChannel.config().getConfigurationConstants(
                        ConfigurationConstantsRequest.getDefaultInstance(), PingObserverNoOp.INSTANCE),
                pingFrequency.toNanos(), pingFrequency.toNanos(), TimeUnit.NANOSECONDS);
    }

    // exposed for Flight and testing
    public BearerHandler _hackBearerHandler() {
        return bearerHandler;
    }

    private ExportStates newExportStates() {
        return new ExportStates(this, bearerChannel.session(), bearerChannel.table(), exportTicketCreator);
    }

    @Override
    public TableService newStatefulTableService() {
        return new TableServiceImpl(newExportStates());
    }

    @Override
    public TableHandleManager batch() {
        return batch(config.mixinStacktrace());
    }

    @Override
    public TableHandleManager batch(boolean mixinStacktraces) {
        return new TableHandleManagerBatch(mixinStacktraces) {
            @Override
            protected ExportService exportService() {
                return newExportStates();
            }
        };
    }

    @Override
    public TableHandleManager serial() {
        return new TableHandleManagerSerial() {
            @Override
            protected ExportService exportService() {
                return newExportStates();
            }

            @Override
            protected TableHandle handle(TableSpec table) {
                return io.deephaven.client.impl.TableServiceImpl.executeUnchecked(exportService(), table, null);
            }
        };
    }

    @Override
    public CompletableFuture<? extends ConsoleSession> console(String type) {
        final ExportId consoleId = new ExportId("Console", exportTicketCreator.createExportId());
        final StartConsoleRequest request = StartConsoleRequest.newBuilder().setSessionType(type)
                .setResultId(consoleId.ticketId().proto()).build();
        return UnaryGrpcFuture.of(request, channel().console()::startConsole,
                response -> new ConsoleSessionImpl(request));
    }

    @Override
    public CompletableFuture<Void> publish(String name, HasTicketId ticketId) {
        if (!SourceVersion.isName(name)) {
            throw new IllegalArgumentException("Invalid name");
        }
        BindTableToVariableRequest request = BindTableToVariableRequest.newBuilder()
                .setVariableName(name)
                .setTableId(ticketId.ticketId().proto())
                .build();
        return UnaryGrpcFuture.ignoreResponse(request, channel().console()::bindTableToVariable);
    }

    @Override
    public CompletableFuture<Void> publish(HasTicketId resultId, HasTicketId sourceId) {
        final PublishRequest request = PublishRequest.newBuilder()
                .setSourceId(sourceId.ticketId().proto())
                .setResultId(resultId.ticketId().proto())
                .build();
        return UnaryGrpcFuture.ignoreResponse(request, channel().session()::publishFromTicket);
    }

    @Override
    public CompletableFuture<ServerData> fetch(HasTypedTicket typedTicket) {
        final TypedTicket tt = typedTicket.typedTicket();
        if (!tt.type().isPresent()) {
            throw new IllegalArgumentException("Type must be present to fetch an object");
        }
        final StreamRequest connectRequest = StreamRequest.newBuilder()
                .setConnect(ConnectRequest.newBuilder().setSourceId(tt.proto()))
                .build();
        return UnaryGrpcFuture.of(connectRequest, this::messageStreamConnectOnly, this::toDataAndExports);
    }

    private void messageStreamConnectOnly(
            io.deephaven.proto.backplane.grpc.StreamRequest request,
            io.grpc.stub.StreamObserver<io.deephaven.proto.backplane.grpc.StreamResponse> responseObserver) {
        final StreamObserver<StreamRequest> observer = channel().object().messageStream(responseObserver);
        observer.onNext(request);
        observer.onCompleted();
    }

    private ServerData toDataAndExports(StreamResponse value) {
        // noinspection SwitchStatementWithTooFewBranches
        switch (value.getMessageCase()) {
            case DATA:
                return ServerData.of(this, value.getData());
            default:
                throw new IllegalStateException(
                        String.format("Unexpected stream response message type, %s", value.getMessageCase()));
        }
    }

    @Override
    public MessageStream<ClientData> connect(HasTypedTicket typedTicket,
            MessageStream<ServerData> receiveStream) {
        final TypedTicket tt = typedTicket.typedTicket();
        if (!tt.type().isPresent()) {
            throw new IllegalArgumentException("Type must be present to open messageStream with an object");
        }
        final StreamRequest connectRequest = StreamRequest.newBuilder()
                .setConnect(ConnectRequest.newBuilder()
                        .setSourceId(tt.proto())
                        .build())
                .build();
        final StreamObserver<StreamRequest> serverObserver =
                channel().object().messageStream(new MessageStreamObserver(receiveStream));
        serverObserver.onNext(connectRequest);
        return new MessageStreamImpl(serverObserver);
    }

    @Override
    public CompletableFuture<? extends ServerObject> export(HasTypedTicket typedTicket) {
        return export(typedTicket, this::toServerObject);
    }

    @Override
    public CompletableFuture<? extends Fetchable> fetchable(HasTypedTicket typedTicket) {
        return export(typedTicket, this::toFetchableObject);
    }

    @Override
    public CompletableFuture<? extends Bidirectional> bidirectional(HasTypedTicket typedTicket) {
        return export(typedTicket, this::toBidirectionalObject);
    }

    private <T extends ServerObject> CompletableFuture<T> export(HasTypedTicket typedTicket, Function<ExportId, T> f) {
        final TypedTicket tt = typedTicket.typedTicket();
        final ExportId exportId = newExportId(tt.type().orElse(null));
        // Doesn't need to be closed unless it makes it back to the user
        // noinspection resource
        final T serverObject = f.apply(exportId);
        final ExportRequest exportRequest = ExportRequest.newBuilder()
                .setSourceId(tt.ticketId().proto())
                .setResultId(exportId.ticketId().proto())
                .build();
        return UnaryGrpcFuture.ignoreResponse(exportRequest, channel().session()::exportFromTicket)
                .thenApply(x -> serverObject);
    }

    private ServerObject toServerObject(ExportId exportId) {
        return exportId.toServerObject(this);
    }

    private Fetchable toFetchableObject(ExportId exportId) {
        // all fetchable objects are custom objects atm
        return new CustomObject(this, exportId);
    }

    private Bidirectional toBidirectionalObject(ExportId exportId) {
        // all bidirectional objects are custom objects atm
        return new CustomObject(this, exportId);
    }

    @Override
    public void close() {
        try {
            closeFuture().get(config.closeTimeout().toNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted waiting for session close");
        } catch (TimeoutException e) {
            log.warn("Timed out waiting for session close");
        } catch (ExecutionException e) {
            log.error("Exception waiting for session close", e);
        } catch (CancellationException e) {
            log.warn("Close cancelled", e);
        }
    }

    @Override
    public synchronized CompletableFuture<Void> closeFuture() {
        if (closeFuture == null) {
            pingJob.cancel(false);
            HandshakeRequest handshakeRequest = HandshakeRequest.getDefaultInstance();
            closeFuture = UnaryGrpcFuture.ignoreResponse(handshakeRequest, channel().session()::closeSession);
        }
        return closeFuture;
    }

    @Override
    protected TableService delegate() {
        // This allows Session to implement an un-cached TableService.
        // Each respective execution (Session.execute(), Session.executeAsync(), Session.serial().execute(), etc)
        // will create new states for that specific execution.
        return newStatefulTableService();
    }

    @Override
    public ExportId newExportId() {
        return newExportId(TableObject.TYPE);
    }

    private ExportId newExportId(String type) {
        return new ExportId(type, exportTicketCreator.createExportId());
    }

    @Override
    public CompletableFuture<Void> release(ExportId exportId) {
        ReleaseRequest request = ReleaseRequest.newBuilder()
                .setId(exportId.ticketId().proto())
                .build();
        return UnaryGrpcFuture.ignoreResponse(request, channel().session()::release);
    }

    @Override
    public DeephavenChannel channel() {
        return bearerChannel;
    }

    @Override
    public CompletableFuture<Void> addToInputTable(HasTicketId destination, HasTicketId source) {
        final AddTableRequest request = AddTableRequest.newBuilder()
                .setInputTable(destination.ticketId().proto())
                .setTableToAdd(source.ticketId().proto())
                .build();
        return UnaryGrpcFuture.ignoreResponse(request, channel().inputTable()::addTableToInputTable);
    }

    @Override
    public CompletableFuture<Void> deleteFromInputTable(HasTicketId destination, HasTicketId source) {
        final DeleteTableRequest request = DeleteTableRequest.newBuilder()
                .setInputTable(destination.ticketId().proto())
                .setTableToRemove(source.ticketId().proto())
                .build();
        return UnaryGrpcFuture.ignoreResponse(request,
                channel().inputTable()::deleteTableFromInputTable);
    }

    @Override
    public Cancel subscribeToFields(Listener listener) {
        final ListFieldsRequest request = ListFieldsRequest.newBuilder().build();
        final ListFieldsObserver observer = new ListFieldsObserver(listener);
        bearerChannel.application().listFields(request, observer);
        return observer;
    }

    ScheduledExecutorService executor() {
        return config.executor();
    }

    @Override
    public CompletableFuture<Map<String, ConfigValue>> getAuthenticationConstants() {
        return UnaryGrpcFuture.of(AuthenticationConstantsRequest.getDefaultInstance(),
                channel().config()::getAuthenticationConstants, AuthenticationConstantsResponse::getConfigValuesMap);
    }

    @Override
    public CompletableFuture<Map<String, ConfigValue>> getConfigurationConstants() {
        return UnaryGrpcFuture.of(ConfigurationConstantsRequest.getDefaultInstance(),
                channel().config()::getConfigurationConstants, ConfigurationConstantsResponse::getConfigValuesMap);
    }

    private class ConsoleSessionImpl implements ConsoleSession {

        private final StartConsoleRequest request;

        public ConsoleSessionImpl(StartConsoleRequest request) {
            this.request = Objects.requireNonNull(request);
        }

        @Override
        public String type() {
            return request.getSessionType();
        }

        @Override
        public Ticket ticket() {
            return request.getResultId();
        }

        @Override
        public Changes executeCode(String code) throws InterruptedException, ExecutionException, TimeoutException {
            return executeCodeFuture(code).get(config.executeTimeout().toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public Changes executeScript(Path path)
                throws IOException, InterruptedException, ExecutionException, TimeoutException {
            return executeScriptFuture(path).get(config.executeTimeout().toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public CompletableFuture<Changes> executeCodeFuture(String code) {
            final ExecuteCommandRequest request =
                    ExecuteCommandRequest.newBuilder().setConsoleId(ticket()).setCode(code).build();
            return UnaryGrpcFuture.of(request, channel().console()::executeCommand,
                    response -> {
                        Changes.Builder builder = Changes.builder().changes(new FieldChanges(response.getChanges()));
                        if (!response.getErrorMessage().isEmpty()) {
                            builder.errorMessage(response.getErrorMessage());
                        }
                        return builder.build();
                    });
        }

        @Override
        public CompletableFuture<Changes> executeScriptFuture(Path path) throws IOException {
            final String code = String.join(System.lineSeparator(), Files.readAllLines(path, StandardCharsets.UTF_8));
            return executeCodeFuture(code);
        }

        @Override
        public CompletableFuture<Void> closeFuture() {
            ReleaseRequest request = ReleaseRequest.newBuilder()
                    .setId(this.request.getResultId())
                    .build();
            return UnaryGrpcFuture.ignoreResponse(request, channel().session()::release);
        }

        @Override
        public void close() {
            try {
                closeFuture().get(config.closeTimeout().toNanos(), TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("Interrupted waiting for console close");
            } catch (TimeoutException e) {
                log.warn("Timed out waiting for console close");
            } catch (ExecutionException e) {
                log.error("Exception waiting for console close", e);
            }
        }
    }

    private static class ListFieldsObserver
            implements Cancel, ClientResponseObserver<ListFieldsRequest, FieldsChangeUpdate> {

        private final Listener listener;
        private ClientCallStreamObserver<?> stream;

        public ListFieldsObserver(Listener listener) {
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void cancel() {
            stream.cancel("User cancelled", null);
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<ListFieldsRequest> requestStream) {
            stream = requestStream;
        }

        @Override
        public void onNext(FieldsChangeUpdate value) {
            listener.onNext(new FieldChanges(value));
        }

        @Override
        public void onError(Throwable t) {
            listener.onError(t);
        }

        @Override
        public void onCompleted() {
            listener.onCompleted();
        }
    }

    private enum PingObserverNoOp implements StreamObserver<ConfigurationConstantsResponse> {
        INSTANCE;

        @Override
        public void onNext(ConfigurationConstantsResponse value) {

        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }

    private class MessageStreamObserver implements StreamObserver<StreamResponse> {
        private final MessageStream<ServerData> clientStream;

        public MessageStreamObserver(MessageStream<ServerData> clientStream) {
            this.clientStream = Objects.requireNonNull(clientStream);
        }

        @Override
        public void onNext(StreamResponse value) {
            clientStream.onData(toDataAndExports(value));
        }

        @Override
        public void onError(Throwable t) {
            clientStream.onClose();
        }

        @Override
        public void onCompleted() {
            clientStream.onClose();
        }
    }

    private static class MessageStreamImpl implements MessageStream<ClientData> {
        private final StreamObserver<StreamRequest> serverObserver;

        public MessageStreamImpl(StreamObserver<StreamRequest> serverObserver) {
            this.serverObserver = Objects.requireNonNull(serverObserver);
        }

        @Override
        public void onData(ClientData message) {
            serverObserver.onNext(StreamRequest.newBuilder().setData(message.proto()).build());
        }

        @Override
        public void onClose() {
            serverObserver.onCompleted();
        }
    }

    private class TableServiceImpl extends TableHandleManagerDelegate implements TableService {

        private final ExportStates exportStates;

        TableServiceImpl(ExportStates exportStates) {
            this.exportStates = Objects.requireNonNull(exportStates);
        }

        // ---------------------------------------------------

        @Override
        public TableHandleFuture executeAsync(TableSpec table) {
            return TableServiceAsyncImpl.executeAsync(exportStates, table);
        }

        @Override
        public List<? extends TableHandleFuture> executeAsync(Iterable<? extends TableSpec> tables) {
            return TableServiceAsyncImpl.executeAsync(exportStates, tables);
        }

        // ---------------------------------------------------

        @Override
        protected TableHandleManager delegate() {
            return config.delegateToBatch() ? batch() : serial();
        }

        @Override
        public TableHandleManager batch() {
            return batch(config.mixinStacktrace());
        }

        @Override
        public TableHandleManager batch(boolean mixinStacktrace) {
            return new TableHandleManagerBatch(mixinStacktrace) {
                @Override
                protected ExportService exportService() {
                    return exportStates;
                }
            };
        }

        @Override
        public TableHandleManager serial() {
            return new TableHandleManagerSerial() {
                @Override
                protected ExportService exportService() {
                    return exportStates;
                }

                @Override
                protected TableHandle handle(TableSpec table) {
                    return io.deephaven.client.impl.TableServiceImpl.executeUnchecked(exportService(), table, null);
                }
            };
        }
    }
}
