/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.proto.DeephavenChannel;
import io.deephaven.proto.backplane.grpc.AddTableRequest;
import io.deephaven.proto.backplane.grpc.AddTableResponse;
import io.deephaven.proto.backplane.grpc.CloseSessionResponse;
import io.deephaven.proto.backplane.grpc.ConfigValue;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsRequest;
import io.deephaven.proto.backplane.grpc.ConfigurationConstantsResponse;
import io.deephaven.proto.backplane.grpc.DeleteTableRequest;
import io.deephaven.proto.backplane.grpc.DeleteTableResponse;
import io.deephaven.proto.backplane.grpc.FetchObjectRequest;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableResponse;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandResponse;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleResponse;
import io.deephaven.proto.util.ExportTicketHelper;
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
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * A {@link Session} implementation that uses {@link io.deephaven.proto.backplane.grpc.BatchTableRequest batch requests}
 * and memoizes based on {@link io.deephaven.qst.table.TableSpec} equality.
 *
 * <p>
 * {@inheritDoc}
 */
public final class SessionImpl extends SessionBase {
    private static final Logger log = LoggerFactory.getLogger(SessionImpl.class);

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
    private final ExportStates states;
    private final TableHandleManagerSerial serialManager;
    private final TableHandleManagerBatch batchManager;
    private final ScheduledFuture<?> pingJob;

    private SessionImpl(SessionImplConfig config, DeephavenChannel bearerChannel, Duration pingFrequency,
            BearerHandler bearerHandler) {
        this.config = Objects.requireNonNull(config);
        this.bearerChannel = Objects.requireNonNull(bearerChannel);
        this.bearerHandler = Objects.requireNonNull(bearerHandler);
        this.exportTicketCreator = new ExportTicketCreator();
        this.states = new ExportStates(this, bearerChannel.session(), bearerChannel.table(), exportTicketCreator);
        this.serialManager = TableHandleManagerSerial.of(this);
        this.batchManager = TableHandleManagerBatch.of(this, config.mixinStacktrace());
        this.pingJob = config.executor().scheduleAtFixedRate(
                () -> bearerChannel.config().getConfigurationConstants(
                        ConfigurationConstantsRequest.getDefaultInstance(), PingObserverNoOp.INSTANCE),
                pingFrequency.toNanos(), pingFrequency.toNanos(), TimeUnit.NANOSECONDS);
    }

    BearerHandler _hackBearerHandler() {
        return bearerHandler;
    }

    @Override
    public List<Export> export(ExportsRequest request) {
        return states.export(request);
    }

    @Override
    public CompletableFuture<? extends ConsoleSession> console(String type) {
        final ExportId consoleId = new ExportId("Console", exportTicketCreator.createExportId());
        final StartConsoleRequest request = StartConsoleRequest.newBuilder().setSessionType(type)
                .setResultId(consoleId.ticketId().ticket()).build();
        final ConsoleHandler handler = new ConsoleHandler(request);
        bearerChannel.console().startConsole(request, handler);
        return handler.future();
    }

    @Override
    public CompletableFuture<Void> publish(String name, HasTicketId ticketId) {
        if (!SourceVersion.isName(name)) {
            throw new IllegalArgumentException("Invalid name");
        }
        PublishObserver observer = new PublishObserver();
        bearerChannel.console().bindTableToVariable(BindTableToVariableRequest.newBuilder()
                .setVariableName(name).setTableId(ticketId.ticketId().ticket()).build(), observer);
        return observer.future;
    }

    @Override
    public CompletableFuture<FetchedObject> fetchObject(String type, HasTicketId ticketId) {
        final FetchObjectRequest request = FetchObjectRequest.newBuilder()
                .setSourceId(TypedTicket.newBuilder()
                        .setType(type)
                        .setTicket(ticketId.ticketId().ticket())
                        .build())
                .build();
        final FetchObserver observer = new FetchObserver();
        bearerChannel.object().fetchObject(request, observer);
        return observer.future;
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
        }
    }

    @Override
    public CompletableFuture<Void> closeFuture() {
        pingJob.cancel(false);
        HandshakeRequest handshakeRequest = HandshakeRequest.getDefaultInstance();
        CloseSessionHandler handler = new CloseSessionHandler();
        bearerChannel.session().closeSession(handshakeRequest, handler);
        return handler.future;
    }

    @Override
    protected TableHandleManager delegate() {
        return config.delegateToBatch() ? batchManager : serialManager;
    }

    @Override
    public TableHandleManager batch() {
        return batchManager;
    }

    @Override
    public TableHandleManager batch(boolean mixinStacktrace) {
        if (this.config.mixinStacktrace() == mixinStacktrace) {
            return batchManager;
        }
        return TableHandleManagerBatch.of(this, mixinStacktrace);
    }

    @Override
    public TableHandleManager serial() {
        return serialManager;
    }

    @Override
    public ExportId newExportId() {
        return new ExportId("Table", exportTicketCreator.createExportId());
    }

    @Override
    public CompletableFuture<Void> release(ExportId exportId) {
        final ReleaseTicketObserver observer = new ReleaseTicketObserver();
        bearerChannel.session().release(
                ReleaseRequest.newBuilder().setId(exportId.ticketId().ticket()).build(), observer);
        return observer.future;
    }

    @Override
    public DeephavenChannel channel() {
        return bearerChannel;
    }

    @Override
    public CompletableFuture<Void> addToInputTable(HasTicketId destination, HasTicketId source) {
        final AddTableRequest request = AddTableRequest.newBuilder()
                .setInputTable(destination.ticketId().ticket())
                .setTableToAdd(source.ticketId().ticket())
                .build();
        final AddToInputTableObserver observer = new AddToInputTableObserver();
        bearerChannel.inputTable().addTableToInputTable(request, observer);
        return observer.future;
    }

    @Override
    public CompletableFuture<Void> deleteFromInputTable(HasTicketId destination, HasTicketId source) {
        final DeleteTableRequest request = DeleteTableRequest.newBuilder()
                .setInputTable(destination.ticketId().ticket())
                .setTableToRemove(source.ticketId().ticket())
                .build();
        final DeleteFromInputTableObserver observer = new DeleteFromInputTableObserver();
        bearerChannel.inputTable().deleteTableFromInputTable(request, observer);
        return observer.future;
    }

    @Override
    public Cancel subscribeToFields(Listener listener) {
        final ListFieldsRequest request = ListFieldsRequest.newBuilder().build();
        final ListFieldsObserver observer = new ListFieldsObserver(listener);
        bearerChannel.application().listFields(request, observer);
        return observer;
    }

    public ScheduledExecutorService executor() {
        return config.executor();
    }

    public long batchCount() {
        return states.batchCount();
    }

    public long releaseCount() {
        return states.releaseCount();
    }

    private static class PublishObserver
            implements ClientResponseObserver<BindTableToVariableRequest, BindTableToVariableResponse> {
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void beforeStart(
                ClientCallStreamObserver<BindTableToVariableRequest> requestStream) {
            future.whenComplete((session, throwable) -> {
                if (future.isCancelled()) {
                    requestStream.cancel("User cancelled", null);
                }
            });
        }

        @Override
        public void onNext(BindTableToVariableResponse value) {
            future.complete(null);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(
                        new IllegalStateException("Observer completed without response"));
            }
        }
    }

    private static final class FetchObserver
            implements ClientResponseObserver<FetchObjectRequest, FetchObjectResponse> {
        private final CompletableFuture<FetchedObject> future = new CompletableFuture<>();

        @Override
        public void beforeStart(ClientCallStreamObserver<FetchObjectRequest> requestStream) {
            future.whenComplete((session, throwable) -> {
                if (future.isCancelled()) {
                    requestStream.cancel("User cancelled", null);
                }
            });
        }

        @Override
        public void onNext(FetchObjectResponse value) {
            final String type = value.getType();
            final ByteString data = value.getData();
            final List<ExportId> exportIds = value.getTypedExportIdList().stream()
                    .map(FetchObserver::toExportId)
                    .collect(Collectors.toList());
            future.complete(new FetchedObject(type, data, exportIds));
        }

        private static ExportId toExportId(TypedTicket e) {
            final String type;
            if (e.getType().isEmpty()) {
                type = null;
            } else {
                type = e.getType();
            }
            final int exportId = ExportTicketHelper.ticketToExportId(e.getTicket(), "exportId");
            return new ExportId(type, exportId);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(
                        new IllegalStateException("Observer completed without response"));
            }
        }
    }

    private static class CloseSessionHandler implements StreamObserver<CloseSessionResponse> {

        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void onNext(CloseSessionResponse value) {

        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            future.complete(null);
        }
    }

    private static class ExecuteCommandHandler implements StreamObserver<ExecuteCommandResponse> {

        private final CompletableFuture<Changes> future = new CompletableFuture<>();

        private static Changes of(ExecuteCommandResponse value) {
            Changes.Builder builder = Changes.builder().changes(new FieldChanges(value.getChanges()));
            if (!value.getErrorMessage().isEmpty()) {
                builder.errorMessage(value.getErrorMessage());
            }
            return builder.build();
        }

        @Override
        public void onNext(ExecuteCommandResponse value) {
            future.complete(of(value));
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(new IllegalStateException("ExecuteCommandHandler.onNext not called"));
            }
        }
    }

    private class ConsoleHandler implements StreamObserver<StartConsoleResponse> {
        private final StartConsoleRequest request;
        private final CompletableFuture<ConsoleSession> future;

        public ConsoleHandler(StartConsoleRequest request) {
            this.request = Objects.requireNonNull(request);
            this.future = new CompletableFuture<>();
        }

        CompletableFuture<ConsoleSession> future() {
            return future;
        }

        @Override
        public void onNext(StartConsoleResponse response) {
            future.complete(new ConsoleSessionImpl(request, response));
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(new IllegalStateException("ConsoleHandler.onNext not called"));
            }
        }
    }

    private class ConsoleSessionImpl implements ConsoleSession {

        private final StartConsoleRequest request;
        private final StartConsoleResponse response;

        public ConsoleSessionImpl(StartConsoleRequest request, StartConsoleResponse response) {
            this.request = Objects.requireNonNull(request);
            this.response = Objects.requireNonNull(response);
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
            final ExecuteCommandHandler handler = new ExecuteCommandHandler();
            bearerChannel.console().executeCommand(request, handler);
            return handler.future;
        }

        @Override
        public CompletableFuture<Changes> executeScriptFuture(Path path) throws IOException {
            final String code = String.join(System.lineSeparator(), Files.readAllLines(path, StandardCharsets.UTF_8));
            return executeCodeFuture(code);
        }

        @Override
        public CompletableFuture<Void> closeFuture() {
            final ConsoleCloseHandler handler = new ConsoleCloseHandler();
            bearerChannel.session().release(ReleaseRequest.newBuilder().setId(request.getResultId()).build(), handler);
            return handler.future();
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

    private static class ConsoleCloseHandler implements StreamObserver<ReleaseResponse> {
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        CompletableFuture<Void> future() {
            return future;
        }

        @Override
        public void onNext(ReleaseResponse value) {
            future.complete(null);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(new IllegalStateException("ConsoleCloseHandler.onNext not called"));
            }
        }
    }

    private static class ReleaseTicketObserver
            implements ClientResponseObserver<ReleaseRequest, ReleaseResponse> {
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void beforeStart(
                ClientCallStreamObserver<ReleaseRequest> requestStream) {
            future.whenComplete((session, throwable) -> {
                if (future.isCancelled()) {
                    requestStream.cancel("User cancelled", null);
                }
            });
        }

        @Override
        public void onNext(ReleaseResponse value) {
            future.complete(null);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(
                        new IllegalStateException("Observer completed without response"));
            }
        }
    }

    private static class AddToInputTableObserver
            implements ClientResponseObserver<AddTableRequest, AddTableResponse> {
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void beforeStart(
                ClientCallStreamObserver<AddTableRequest> requestStream) {
            future.whenComplete((session, throwable) -> {
                if (future.isCancelled()) {
                    requestStream.cancel("User cancelled", null);
                }
            });
        }

        @Override
        public void onNext(AddTableResponse value) {
            future.complete(null);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(
                        new IllegalStateException("Observer completed without response"));
            }
        }
    }

    private static class DeleteFromInputTableObserver
            implements ClientResponseObserver<DeleteTableRequest, DeleteTableResponse> {
        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void beforeStart(
                ClientCallStreamObserver<DeleteTableRequest> requestStream) {
            future.whenComplete((session, throwable) -> {
                if (future.isCancelled()) {
                    requestStream.cancel("User cancelled", null);
                }
            });
        }

        @Override
        public void onNext(DeleteTableResponse value) {
            future.complete(null);
        }

        @Override
        public void onError(Throwable t) {
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(
                        new IllegalStateException("Observer completed without response"));
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
}
