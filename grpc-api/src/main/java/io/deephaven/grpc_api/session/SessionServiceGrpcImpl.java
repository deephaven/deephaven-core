/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.session;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.grpc_api.auth.AuthContextProvider;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.util.auth.AuthContext;
import io.grpc.*;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Optional;
import java.util.UUID;

public class SessionServiceGrpcImpl extends SessionServiceGrpc.SessionServiceImplBase {
    // TODO (#997): use flight AuthConstants
    public static final String DEEPHAVEN_SESSION_ID = "deephaven_session_id";
    public static final Metadata.Key<String> SESSION_HEADER_KEY =
            Metadata.Key.of(DEEPHAVEN_SESSION_ID, Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<SessionState> SESSION_CONTEXT_KEY = Context.key(DEEPHAVEN_SESSION_ID);

    private static final Logger log = LoggerFactory.getLogger(SessionServiceGrpcImpl.class);

    private final SessionService service;
    private final AuthContextProvider authProvider;
    private final TicketRouter ticketRouter;

    @Inject()
    public SessionServiceGrpcImpl(final SessionService service,
            final AuthContextProvider authProvider,
            final TicketRouter ticketRouter) {
        this.service = service;
        this.authProvider = authProvider;
        this.ticketRouter = ticketRouter;
    }

    @Override
    public void newSession(final HandshakeRequest request, final StreamObserver<HandshakeResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            if (!authProvider.supportsProtocol(request.getAuthProtocol())) {
                responseObserver.onError(
                        GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Protocol version not allowed."));
                return;
            }

            final AuthContext authContext = authProvider.authenticate(request.getAuthProtocol(), request.getPayload());
            if (authContext == null) {
                responseObserver
                        .onError(GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "Authentication failed."));
                return;
            }

            final SessionState session = service.newSession(authContext);
            responseObserver.onNext(HandshakeResponse.newBuilder()
                    .setMetadataHeader(ByteString.copyFromUtf8(DEEPHAVEN_SESSION_ID))
                    .setSessionToken(session.getExpiration().getTokenAsByteString())
                    .setTokenDeadlineTimeMillis(session.getExpiration().deadline.getMillis())
                    .setTokenExpirationDelayMillis(service.getExpirationDelayMs())
                    .build());

            responseObserver.onCompleted();
        });
    }

    @Override
    public void refreshSessionToken(final HandshakeRequest request,
            final StreamObserver<HandshakeResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            if (request.getAuthProtocol() != 0) {
                responseObserver.onError(
                        GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Protocol version not allowed."));
                return;
            }

            final SessionState session = service.getCurrentSession();
            if (session != service.getSessionForToken(UUID.fromString(request.getPayload().toStringUtf8()))) {
                responseObserver.onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Refresh request's session ID does not match metadata header provided ID."));
                return;
            }

            final SessionService.TokenExpiration expiration = service.refreshToken(session);

            responseObserver.onNext(HandshakeResponse.newBuilder()
                    .setMetadataHeader(ByteString.copyFromUtf8(DEEPHAVEN_SESSION_ID))
                    .setSessionToken(expiration.getTokenAsByteString())
                    .setTokenDeadlineTimeMillis(expiration.deadline.getMillis())
                    .setTokenExpirationDelayMillis(service.getExpirationDelayMs())
                    .build());

            responseObserver.onCompleted();
        });
    }

    @Override
    public void closeSession(final HandshakeRequest request,
            final StreamObserver<CloseSessionResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            if (request.getAuthProtocol() != 0) {
                responseObserver.onError(
                        GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Protocol version not allowed."));
                return;
            }

            final SessionState session = service.getCurrentSession();
            if (session != service.getSessionForToken(UUID.fromString(request.getPayload().toStringUtf8()))) {
                responseObserver.onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT,
                        "Refresh request's session ID does not match metadata header provided ID."));
                return;
            }

            service.closeSession(session);
            responseObserver.onNext(CloseSessionResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void release(final ReleaseRequest request, final StreamObserver<ReleaseResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = service.getCurrentSession();
            if (!request.hasId()) {
                responseObserver
                        .onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Release ticket not supplied"));
                return;
            }
            final SessionState.ExportObject<?> export = session.getExportIfExists(request.getId(), "id");
            if (export == null) {
                responseObserver.onError(GrpcUtil.statusRuntimeException(Code.UNAVAILABLE, "Export not yet defined"));
                return;
            }

            // noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (export) {
                final ExportNotification.State currState = export.getState();
                if (currState != ExportNotification.State.PENDING
                        && currState != ExportNotification.State.QUEUED
                        && currState != ExportNotification.State.EXPORTED) {
                    responseObserver.onError(
                            GrpcUtil.statusRuntimeException(Code.NOT_FOUND, "Ticket already in state: " + currState));
                    return;
                }
            }

            export.cancel();
            responseObserver.onNext(ReleaseResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }

    @Override
    public void exportFromTicket(ExportRequest request, StreamObserver<ExportResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = service.getCurrentSession();
            if (!request.hasSourceId()) {
                responseObserver
                        .onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Source ticket not supplied"));
                return;
            }
            if (!request.hasResultId()) {
                responseObserver
                        .onError(GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Result ticket not supplied"));
                return;
            }

            final SessionState.ExportObject<Object> source = ticketRouter.resolve(
                    session, request.getSourceId(), "sourceId");
            session.newExport(request.getResultId(), "resultId")
                    .require(source)
                    .onError(responseObserver)
                    .submit(() -> {
                        GrpcUtil.safelyExecute(() -> responseObserver.onNext(ExportResponse.getDefaultInstance()));
                        GrpcUtil.safelyExecute(responseObserver::onCompleted);
                        return source.get();
                    });
        });
    }

    @Override
    public void exportNotifications(final ExportNotificationRequest request,
            final StreamObserver<ExportNotification> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = service.getCurrentSession();

            session.addExportListener(responseObserver);
            ((ServerCallStreamObserver<ExportNotification>) responseObserver).setOnCancelHandler(() -> {
                session.removeExportListener(responseObserver);
            });
        });
    }

    @Singleton
    public static class AuthServerInterceptor implements ServerInterceptor {
        private final SessionService service;

        @Inject()
        public AuthServerInterceptor(final SessionService service) {
            this.service = service;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
                final Metadata metadata,
                final ServerCallHandler<ReqT, RespT> serverCallHandler) {
            SessionState session = null;
            final Optional<String> tokenBytes = Optional.ofNullable(metadata.get(SESSION_HEADER_KEY));
            if (tokenBytes.isPresent()) {
                UUID token = UuidCreator.fromString(tokenBytes.get());
                session = service.getSessionForToken(token);
            }
            final Context newContext = Context.current().withValue(SESSION_CONTEXT_KEY, session);
            return Contexts.interceptCall(newContext, serverCall, metadata, serverCallHandler);
        }
    }
}
