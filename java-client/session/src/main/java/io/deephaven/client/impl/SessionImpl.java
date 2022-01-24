package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.proto.backplane.grpc.AddTableRequest;
import io.deephaven.proto.backplane.grpc.AddTableResponse;
import io.deephaven.proto.backplane.grpc.ApplicationServiceGrpc.ApplicationServiceStub;
import io.deephaven.proto.backplane.grpc.CloseSessionResponse;
import io.deephaven.proto.backplane.grpc.DeleteTableRequest;
import io.deephaven.proto.backplane.grpc.DeleteTableResponse;
import io.deephaven.proto.backplane.grpc.FetchObjectRequest;
import io.deephaven.proto.backplane.grpc.FetchObjectResponse;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.InputTableServiceGrpc.InputTableServiceStub;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.proto.backplane.grpc.ObjectServiceGrpc.ObjectServiceStub;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableResponse;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandResponse;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleResponse;
import io.deephaven.proto.util.ExportTicketHelper;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
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

    private static final int REFRESH_RETRIES = 5;

    public interface Handler {
        void onRefreshSuccess();

        void onRefreshTokenError(Throwable t, Runnable invokeForRetry);

        void onCloseSessionError(Throwable t);

        void onClosed();
    }

    private static class Retrying implements Handler {
        private static final Logger log = LoggerFactory.getLogger(Retrying.class);

        private final int maxRefreshes;
        private int remainingRefreshes;

        Retrying(int maxRefreshes) {
            this.maxRefreshes = maxRefreshes;
        }

        @Override
        public void onRefreshSuccess() {
            remainingRefreshes = maxRefreshes;
        }

        @Override
        public void onRefreshTokenError(Throwable t, Runnable invokeForRetry) {
            if (remainingRefreshes > 0) {
                remainingRefreshes--;
                log.warn("Error refreshing token, trying again", t);
                invokeForRetry.run();
                return;
            }
            log.error("Error refreshing token, giving up", t);
        }

        @Override
        public void onCloseSessionError(Throwable t) {
            log.error("onCloseSessionError", t);
        }

        @Override
        public void onClosed() {

        }
    }

    public static SessionImpl create(SessionImplConfig config) {
        final HandshakeRequest request = initialHandshake();
        final HandshakeResponse response = config.channel().sessionBlocking().newSession(request);
        final AuthenticationInfo initialAuth = AuthenticationInfo.of(response);
        final SessionImpl session =
                new SessionImpl(config, new Retrying(REFRESH_RETRIES), initialAuth);


        session.scheduleRefreshSessionToken(response);
        return session;
    }

    public static CompletableFuture<SessionImpl> createFuture(SessionImplConfig config) {
        final HandshakeRequest request = initialHandshake();
        final SessionObserver sessionObserver = new SessionObserver(config);
        config.channel().session().newSession(request, sessionObserver);
        return sessionObserver.future;
    }

    private static HandshakeRequest initialHandshake() {
        return HandshakeRequest.newBuilder().setAuthProtocol(1).build();
    }

    private static class SessionObserver
            implements ClientResponseObserver<HandshakeRequest, HandshakeResponse> {

        private final SessionImplConfig config;
        private final CompletableFuture<SessionImpl> future = new CompletableFuture<>();

        SessionObserver(SessionImplConfig config) {
            this.config = Objects.requireNonNull(config);
        }

        @Override
        public void beforeStart(ClientCallStreamObserver<HandshakeRequest> requestStream) {
            future.whenComplete((session, throwable) -> {
                if (future.isCancelled()) {
                    requestStream.cancel("User cancelled", null);
                }
            });
        }

        @Override
        public void onNext(HandshakeResponse response) {
            AuthenticationInfo initialAuth = AuthenticationInfo.of(response);
            SessionImpl session =
                    new SessionImpl(config, new Retrying(REFRESH_RETRIES), initialAuth);
            if (future.complete(session)) {
                session.scheduleRefreshSessionToken(response);
            } else {
                // Make sure we don't leak a session if we aren't able to pass it off to the user
                session.close();
            }
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

    private final ScheduledExecutorService executor;
    private final SessionServiceStub sessionService;
    private final ConsoleServiceStub consoleService;
    private final ObjectServiceStub objectService;
    private final InputTableServiceStub inputTableService;
    private final ApplicationServiceStub applicationServiceStub;
    private final Handler handler;
    private final ExportTicketCreator exportTicketCreator;
    private final ExportStates states;

    private volatile AuthenticationInfo auth;

    private final boolean delegateToBatch;
    private final boolean mixinStacktrace;
    private final Duration executeTimeout;
    private final Duration closeTimeout;
    private final TableHandleManagerSerial serialManager;
    private final TableHandleManagerBatch batchManager;

    private SessionImpl(SessionImplConfig config, Handler handler, AuthenticationInfo auth) {

        CallCredentials credentials = new SessionCallCredentials();
        this.auth = Objects.requireNonNull(auth);
        this.handler = Objects.requireNonNull(handler);
        this.executor = config.executor();
        this.sessionService = config.channel().session().withCallCredentials(credentials);
        this.consoleService = config.channel().console().withCallCredentials(credentials);
        this.objectService = config.channel().object().withCallCredentials(credentials);
        this.inputTableService = config.channel().inputTable().withCallCredentials(credentials);
        this.applicationServiceStub = config.channel().application().withCallCredentials(credentials);
        this.exportTicketCreator = new ExportTicketCreator();
        this.states = new ExportStates(this, sessionService, config.channel().table().withCallCredentials(credentials),
                exportTicketCreator);
        this.delegateToBatch = config.delegateToBatch();
        this.mixinStacktrace = config.mixinStacktrace();
        this.executeTimeout = config.executeTimeout();
        this.closeTimeout = config.closeTimeout();
        this.serialManager = TableHandleManagerSerial.of(this);
        this.batchManager = TableHandleManagerBatch.of(this, mixinStacktrace);
    }

    public AuthenticationInfo auth() {
        return auth;
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
        consoleService.startConsole(request, handler);
        return handler.future();
    }

    @Override
    public CompletableFuture<Void> publish(String name, HasTicketId ticketId) {
        if (!SourceVersion.isName(name)) {
            throw new IllegalArgumentException("Invalid name");
        }
        PublishObserver observer = new PublishObserver();
        consoleService.bindTableToVariable(BindTableToVariableRequest.newBuilder()
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
        objectService.fetchObject(request, observer);
        return observer.future;
    }

    @Override
    public void close() {
        try {
            closeFuture().get(closeTimeout.toNanos(), TimeUnit.NANOSECONDS);
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
        HandshakeRequest handshakeRequest = HandshakeRequest.newBuilder().setAuthProtocol(0)
                .setPayload(ByteString.copyFromUtf8(auth.session())).build();
        CloseSessionHandler handler = new CloseSessionHandler();
        sessionService.closeSession(handshakeRequest, handler);
        return handler.future;
    }

    @Override
    protected TableHandleManager delegate() {
        return delegateToBatch ? batchManager : serialManager;
    }

    @Override
    public TableHandleManager batch() {
        return batchManager;
    }

    @Override
    public TableHandleManager batch(boolean mixinStacktrace) {
        if (this.mixinStacktrace == mixinStacktrace) {
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
        sessionService.release(
                ReleaseRequest.newBuilder().setId(exportId.ticketId().ticket()).build(), observer);
        return observer.future;
    }

    @Override
    public CompletableFuture<Void> addToInputTable(HasTicketId destination, HasTicketId source) {
        final AddTableRequest request = AddTableRequest.newBuilder()
                .setInputTable(destination.ticketId().ticket())
                .setTableToAdd(source.ticketId().ticket())
                .build();
        final AddToInputTableObserver observer = new AddToInputTableObserver();
        inputTableService.addTableToInputTable(request, observer);
        return observer.future;
    }

    @Override
    public CompletableFuture<Void> deleteFromInputTable(HasTicketId destination, HasTicketId source) {
        final DeleteTableRequest request = DeleteTableRequest.newBuilder()
                .setInputTable(destination.ticketId().ticket())
                .setTableToRemove(source.ticketId().ticket())
                .build();
        final DeleteFromInputTableObserver observer = new DeleteFromInputTableObserver();
        inputTableService.deleteTableFromInputTable(request, observer);
        return observer.future;
    }

    @Override
    public Cancel subscribeToFields(Listener listener) {
        final ListFieldsRequest request = ListFieldsRequest.newBuilder().build();
        final ListFieldsObserver observer = new ListFieldsObserver(listener);
        applicationServiceStub.listFields(request, observer);
        return observer;
    }

    public long batchCount() {
        return states.batchCount();
    }

    public long releaseCount() {
        return states.releaseCount();
    }

    private void scheduleRefreshSessionToken(HandshakeResponse response) {
        final long now = System.currentTimeMillis();
        final long targetRefreshTime = Math.min(
                now + response.getTokenExpirationDelayMillis() / 3,
                response.getTokenDeadlineTimeMillis() - response.getTokenExpirationDelayMillis() / 10);
        final long refreshDelayMs = Math.max(targetRefreshTime - now, 0);
        executor.schedule(SessionImpl.this::refreshSessionToken, refreshDelayMs, TimeUnit.MILLISECONDS);
    }

    private void scheduleRefreshSessionTokenNow() {
        executor.schedule(SessionImpl.this::refreshSessionToken, 0, TimeUnit.MILLISECONDS);
    }

    private void refreshSessionToken() {
        HandshakeRequest handshakeRequest = HandshakeRequest.newBuilder().setAuthProtocol(0)
                .setPayload(ByteString.copyFromUtf8(auth.session())).build();
        HandshakeHandler handler = new HandshakeHandler();
        sessionService.refreshSessionToken(handshakeRequest, handler);
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

    private class SessionCallCredentials extends CallCredentials {

        @Override
        public void applyRequestMetadata(RequestInfo requestInfo, Executor appExecutor,
                MetadataApplier applier) {
            AuthenticationInfo localAuth = auth;
            Metadata metadata = new Metadata();
            metadata.put(Key.of(localAuth.sessionHeaderKey(), Metadata.ASCII_STRING_MARSHALLER),
                    localAuth.session());
            applier.apply(metadata);
        }

        @Override
        public void thisUsesUnstableApi() {

        }
    }

    private class HandshakeHandler implements StreamObserver<HandshakeResponse> {

        @Override
        public void onNext(HandshakeResponse value) {
            auth = AuthenticationInfo.of(value);
            scheduleRefreshSessionToken(value);
            handler.onRefreshSuccess();
        }

        @Override
        public void onError(Throwable t) {
            handler.onRefreshTokenError(t, SessionImpl.this::scheduleRefreshSessionTokenNow);
        }

        @Override
        public void onCompleted() {
            // ignore
        }
    }

    private class CloseSessionHandler implements StreamObserver<CloseSessionResponse> {

        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void onNext(CloseSessionResponse value) {
            handler.onClosed();
        }

        @Override
        public void onError(Throwable t) {
            handler.onCloseSessionError(t);
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
            return executeCodeFuture(code).get(executeTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public Changes executeScript(Path path)
                throws IOException, InterruptedException, ExecutionException, TimeoutException {
            return executeScriptFuture(path).get(executeTimeout.toNanos(), TimeUnit.NANOSECONDS);
        }

        @Override
        public CompletableFuture<Changes> executeCodeFuture(String code) {
            final ExecuteCommandRequest request =
                    ExecuteCommandRequest.newBuilder().setConsoleId(ticket()).setCode(code).build();
            final ExecuteCommandHandler handler = new ExecuteCommandHandler();
            consoleService.executeCommand(request, handler);
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
            sessionService.release(ReleaseRequest.newBuilder().setId(request.getResultId()).build(), handler);
            return handler.future();
        }

        @Override
        public void close() {
            try {
                closeFuture().get(closeTimeout.toNanos(), TimeUnit.NANOSECONDS);
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
}
