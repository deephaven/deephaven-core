package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.script.Changes;
import io.deephaven.client.impl.script.VariableDefinition;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.CloseSessionResponse;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceBlockingStub;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableResponse;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandRequest;
import io.deephaven.proto.backplane.script.grpc.ExecuteCommandResponse;
import io.deephaven.proto.backplane.script.grpc.StartConsoleRequest;
import io.deephaven.proto.backplane.script.grpc.StartConsoleResponse;
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
import java.util.concurrent.atomic.AtomicReference;

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

    private static final int CONSOLE_EXPORT_ID = 127;

    private static final int TABLE_EXPORT_ID_START = CONSOLE_EXPORT_ID + 1;

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

    public static SessionImpl create(SessionImplConfig config,
            SessionServiceBlockingStub stubBlocking) {
        final HandshakeRequest request = initialHandshake();
        final HandshakeResponse response = stubBlocking.newSession(request);
        final AuthenticationInfo initialAuth = AuthenticationInfo.of(response);
        final SessionImpl session =
                new SessionImpl(config, new Retrying(REFRESH_RETRIES), initialAuth);
        session.scheduleRefreshSessionToken(response);
        return session;
    }

    public static CompletableFuture<SessionImpl> createFuture(SessionImplConfig config) {
        final HandshakeRequest request = initialHandshake();
        final SessionObserver sessionObserver = new SessionObserver(config);
        config.sessionService().newSession(request, sessionObserver);
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
    private final Handler handler;
    private final ExportStates states;

    private volatile AuthenticationInfo auth;

    private final String consoleType;
    private final boolean delegateToBatch;
    private final boolean mixinStacktrace;
    private final Duration executeTimeout;
    private final Duration closeTimeout;
    private final TableHandleManagerSerial serialManager;
    private final TableHandleManagerBatch batchManager;

    private final AtomicReference<ConsoleHandler> consoleHandler;

    private SessionImpl(SessionImplConfig config, Handler handler, AuthenticationInfo auth) {

        CallCredentials credentials = new SessionCallCredentials();
        this.auth = Objects.requireNonNull(auth);
        this.handler = Objects.requireNonNull(handler);
        this.executor = config.executor();
        this.sessionService = config.sessionService().withCallCredentials(credentials);
        this.consoleService = config.consoleService().withCallCredentials(credentials);
        this.states = new ExportStates(this, sessionService, config.tableService().withCallCredentials(credentials),
                TABLE_EXPORT_ID_START);
        this.consoleType = config.consoleType().orElse(null);
        this.delegateToBatch = config.delegateToBatch();
        this.mixinStacktrace = config.mixinStacktrace();
        this.executeTimeout = config.executeTimeout();
        this.closeTimeout = config.closeTimeout();
        this.serialManager = TableHandleManagerSerial.of(this);
        this.batchManager = TableHandleManagerBatch.of(this, mixinStacktrace);
        this.consoleHandler = new AtomicReference<>();
    }

    public AuthenticationInfo auth() {
        return auth;
    }

    private Ticket consoleId() {
        return ExportTicketHelper.exportIdToTicket(CONSOLE_EXPORT_ID);
    }

    @Override
    public List<Export> export(ExportsRequest request) {
        return states.export(request);
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
    public CompletableFuture<Changes> executeScriptFuture(Path path) throws IOException {
        final String code = String.join(System.lineSeparator(), Files.readAllLines(path, StandardCharsets.UTF_8));
        return executeCodeFuture(code);
    }

    @Override
    public CompletableFuture<Changes> executeCodeFuture(String code) {
        return startConsole().thenCompose(response -> {
            final ExecuteCommandRequest request =
                    ExecuteCommandRequest.newBuilder().setConsoleId(response.getResultId()).setCode(code).build();
            final ExecuteCommandHandler handler = new ExecuteCommandHandler();
            consoleService.executeCommand(request, handler);
            return handler.future;
        });
    }

    @Override
    public CompletableFuture<Void> publish(String name, Export export) {
        if (!SourceVersion.isName(name)) {
            throw new IllegalArgumentException("Invalid name");
        }
        PublishObserver observer = new PublishObserver();
        consoleService.bindTableToVariable(BindTableToVariableRequest.newBuilder()
                .setVariableName(name).setTableId(export.ticket()).build(), observer);
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

    public long batchCount() {
        return states.batchCount();
    }

    public long releaseCount() {
        return states.releaseCount();
    }

    private void scheduleRefreshSessionToken(HandshakeResponse response) {
        final long refreshDelayMs = Math.min(
                System.currentTimeMillis() + response.getTokenExpirationDelayMillis() / 3,
                response.getTokenDeadlineTimeMillis() - response.getTokenExpirationDelayMillis() / 10);
        executor.schedule(SessionImpl.this::refreshSessionToken, refreshDelayMs,
                TimeUnit.MILLISECONDS);
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

    private CompletableFuture<StartConsoleResponse> startConsole() {
        if (consoleType == null) {
            throw new IllegalArgumentException("Unable to start a console unless an expected console type is set.");
        }
        ConsoleHandler handler;
        while ((handler = consoleHandler.get()) == null) {
            handler = new ConsoleHandler();
            if (consoleHandler.compareAndSet(null, handler)) {
                final StartConsoleRequest request =
                        StartConsoleRequest.newBuilder().setSessionType(consoleType).setResultId(consoleId()).build();
                consoleService.startConsole(request, handler);
                break;
            }
        }
        return handler.future;
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

        private static VariableDefinition of(io.deephaven.proto.backplane.script.grpc.VariableDefinition d) {
            return VariableDefinition.of(d.getType(), d.getTitle());
        }

        private static Changes of(ExecuteCommandResponse value) {
            Changes.Builder builder = Changes.builder();
            if (!value.getErrorMessage().isEmpty()) {
                builder.errorMessage(value.getErrorMessage());
            }
            for (io.deephaven.proto.backplane.script.grpc.VariableDefinition d : value.getCreatedList()) {
                builder.addCreated(of(d));
            }
            for (io.deephaven.proto.backplane.script.grpc.VariableDefinition d : value.getUpdatedList()) {
                builder.addUpdated(of(d));
            }
            for (io.deephaven.proto.backplane.script.grpc.VariableDefinition d : value.getRemovedList()) {
                builder.addRemoved(of(d));
            }
            return builder.build();
        }

        @Override
        public void onNext(ExecuteCommandResponse value) {
            future.complete(of(value));
        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(new IllegalStateException("onNext not called"));
            }
        }
    }

    private static class ConsoleHandler implements StreamObserver<StartConsoleResponse> {
        private final CompletableFuture<StartConsoleResponse> future = new CompletableFuture<>();

        @Override
        public void onNext(StartConsoleResponse value) {
            future.complete(value);
        }

        @Override
        public void onError(Throwable t) {
            t.printStackTrace();
            future.completeExceptionally(t);
        }

        @Override
        public void onCompleted() {
            if (!future.isDone()) {
                future.completeExceptionally(new IllegalStateException("onNext not called"));
            }
        }
    }
}
