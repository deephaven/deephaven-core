package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc.SessionServiceStub;
import io.deephaven.proto.backplane.grpc.TableServiceGrpc.TableServiceStub;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableRequest;
import io.deephaven.proto.backplane.script.grpc.BindTableToVariableResponse;
import io.deephaven.proto.backplane.script.grpc.ConsoleServiceGrpc.ConsoleServiceStub;
import io.grpc.CallCredentials;
import io.grpc.Metadata;
import io.grpc.Metadata.Key;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.lang.model.SourceVersion;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Session} implementation that uses
 * {@link io.deephaven.proto.backplane.grpc.BatchTableRequest batch requests} and memoizes based on
 * {@link io.deephaven.qst.table.TableSpec} equality.
 *
 * <p>
 * {@inheritDoc}
 */
public final class SessionImpl extends SessionBase {

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

    public static SessionImpl create(SessionServiceGrpc.SessionServiceBlockingStub stubBlocking,
        SessionServiceGrpc.SessionServiceStub stub, TableServiceStub tableServiceStub,
        ConsoleServiceStub consoleServiceStub, ScheduledExecutorService executor) {
        HandshakeRequest request = initialHandshake();
        HandshakeResponse response = stubBlocking.newSession(request);
        AuthenticationInfo initialAuth = AuthenticationInfo.of(response);
        SessionImpl session = new SessionImpl(executor, stub, tableServiceStub, consoleServiceStub,
            new Retrying(REFRESH_RETRIES), initialAuth);
        session.scheduleRefreshSessionToken(response);
        return session;
    }

    public static CompletableFuture<SessionImpl> create(SessionServiceGrpc.SessionServiceStub stub,
        TableServiceStub tableServiceStub, ConsoleServiceStub consoleServiceStub,
        ScheduledExecutorService executor) {
        HandshakeRequest request = initialHandshake();
        SessionObserver sessionObserver =
            new SessionObserver(executor, stub, tableServiceStub, consoleServiceStub);
        stub.newSession(request, sessionObserver);
        return sessionObserver.future;
    }

    private static HandshakeRequest initialHandshake() {
        return HandshakeRequest.newBuilder().setAuthProtocol(1).build();
    }

    private static class SessionObserver
        implements ClientResponseObserver<HandshakeRequest, HandshakeResponse> {

        private final ScheduledExecutorService executor;
        private final SessionServiceStub sessionService;
        private final TableServiceStub tableServiceStub;
        private final ConsoleServiceStub consoleServiceStub;

        private final CompletableFuture<SessionImpl> future = new CompletableFuture<>();

        SessionObserver(ScheduledExecutorService executor, SessionServiceStub sessionService,
            TableServiceStub tableServiceStub, ConsoleServiceStub consoleServiceStub) {
            this.executor = Objects.requireNonNull(executor);
            this.sessionService = Objects.requireNonNull(sessionService);
            this.tableServiceStub = Objects.requireNonNull(tableServiceStub);
            this.consoleServiceStub = Objects.requireNonNull(consoleServiceStub);
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
            SessionImpl session = new SessionImpl(executor, sessionService, tableServiceStub,
                consoleServiceStub, new Retrying(REFRESH_RETRIES), initialAuth);
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

    private SessionImpl(ScheduledExecutorService executor, SessionServiceStub sessionService,
        TableServiceStub tableServiceStub, ConsoleServiceStub consoleService, Handler handler,
        AuthenticationInfo auth) {

        CallCredentials credentials = new SessionCallCredentials();
        sessionService = sessionService.withCallCredentials(credentials);
        tableServiceStub = tableServiceStub.withCallCredentials(credentials);
        consoleService = consoleService.withCallCredentials(credentials);

        this.executor = Objects.requireNonNull(executor);
        this.sessionService = Objects.requireNonNull(sessionService);
        this.consoleService = Objects.requireNonNull(consoleService);
        this.handler = Objects.requireNonNull(handler);
        this.auth = Objects.requireNonNull(auth);
        this.states = new ExportStates(sessionService, tableServiceStub);
    }

    public AuthenticationInfo auth() {
        return auth;
    }

    @Override
    public List<Export> export(ExportsRequest request) {
        return states.export(request);
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
        closeFuture();
    }

    @Override
    public CompletableFuture<Void> closeFuture() {
        HandshakeRequest handshakeRequest = HandshakeRequest.newBuilder().setAuthProtocol(0)
            .setPayload(ByteString.copyFromUtf8(auth.session())).build();
        CloseSessionHandler handler = new CloseSessionHandler();
        sessionService.closeSession(handshakeRequest, handler);
        return handler.future;
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

    private class CloseSessionHandler implements StreamObserver<ReleaseResponse> {

        private final CompletableFuture<Void> future = new CompletableFuture<>();

        @Override
        public void onNext(ReleaseResponse value) {
            if (value.getSuccess()) {
                handler.onClosed();
            }
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
}
