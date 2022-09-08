/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.CloseSessionResponse;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.ExportNotificationRequest;
import io.deephaven.proto.backplane.grpc.ExportRequest;
import io.deephaven.proto.backplane.grpc.ExportResponse;
import io.deephaven.proto.backplane.grpc.HandshakeRequest;
import io.deephaven.proto.backplane.grpc.HandshakeResponse;
import io.deephaven.proto.backplane.grpc.ReleaseRequest;
import io.deephaven.proto.backplane.grpc.ReleaseResponse;
import io.deephaven.proto.backplane.grpc.SessionServiceGrpc;
import io.deephaven.proto.backplane.grpc.TerminationNotificationRequest;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.deephaven.auth.AuthContext;
import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.auth.AuthConstants;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class SessionServiceGrpcImpl extends SessionServiceGrpc.SessionServiceImplBase {
    public static final String DEEPHAVEN_SESSION_ID = Auth2Constants.AUTHORIZATION_HEADER;
    public static final Metadata.Key<String> SESSION_HEADER_KEY =
            Metadata.Key.of(DEEPHAVEN_SESSION_ID, Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<SessionState> SESSION_CONTEXT_KEY = Context.key(DEEPHAVEN_SESSION_ID);

    private static final String SERVER_CALL_ID = "SessionServiceGrpcImpl.ServerCall";
    private static final Context.Key<InterceptedCall<?, ?>> SESSION_CALL_KEY = Context.key(SERVER_CALL_ID);

    private static final Logger log = LoggerFactory.getLogger(SessionServiceGrpcImpl.class);

    private final SessionService service;
    private final TicketRouter ticketRouter;

    @Inject()
    public SessionServiceGrpcImpl(final SessionService service, final TicketRouter ticketRouter) {
        this.service = service;
        this.ticketRouter = ticketRouter;
    }

    @Override
    public void newSession(final HandshakeRequest request, final StreamObserver<HandshakeResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            // TODO: once jsapi is updated to use flight auth, then newSession can be deprecated or removed
            final AuthContext authContext = new AuthContext.SuperUser();

            final SessionState session = service.newSession(authContext);
            responseObserver.onNext(HandshakeResponse.newBuilder()
                    .setMetadataHeader(ByteString.copyFromUtf8(DEEPHAVEN_SESSION_ID))
                    .setSessionToken(session.getExpiration().getBearerTokenAsByteString())
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
            // TODO: once jsapi is updated to use flight auth, then newSession can be deprecated or removed
            if (request.getAuthProtocol() != 0) {
                responseObserver.onError(
                        GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "Protocol version not allowed."));
                return;
            }

            final SessionState session = service.getCurrentSession();
            final SessionService.TokenExpiration expiration = service.refreshToken(session);

            responseObserver.onNext(HandshakeResponse.newBuilder()
                    .setMetadataHeader(ByteString.copyFromUtf8(DEEPHAVEN_SESSION_ID))
                    .setSessionToken(expiration.getBearerTokenAsByteString())
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
                if (SessionState.isExportStateTerminal(currState)) {
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

    @Override
    public void terminationNotification(TerminationNotificationRequest request,
            StreamObserver<TerminationNotificationResponse> responseObserver) {
        GrpcUtil.rpcWrapper(log, responseObserver, () -> {
            final SessionState session = service.getCurrentSession();
            service.addTerminationListener(session, responseObserver);
        });
    }

    public static void insertCallHeader(String key, String value) {
        final Metadata.Key<String> metaKey = Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER);
        final InterceptedCall<?, ?> call = SESSION_CALL_KEY.get();
        if (call == null) {
            throw new IllegalStateException("Cannot insert call header; there is no grpc call in the context");
        }
        if (call.sentHeaders) {
            throw new IllegalStateException("Cannot insert call header; headers already sent");
        }
        if (call.extraHeaders.put(metaKey, value) != null) {
            log.warn().append("Overwrote gRPC call header with key: ").append(metaKey.toString()).endl();
        }
    }

    public static class InterceptedCall<ReqT, RespT> extends SimpleForwardingServerCall<ReqT, RespT> {
        private boolean sentHeaders = false;
        private final SessionService service;
        private final SessionState session;
        private final Map<Metadata.Key<String>, String> extraHeaders = new LinkedHashMap<>();

        private InterceptedCall(final SessionService service, final ServerCall<ReqT, RespT> call,
                @Nullable final SessionState session) {
            super(call);
            this.service = service;
            this.session = session;
        }

        @Override
        public void sendHeaders(final Metadata headers) {
            sentHeaders = true;
            try {
                addHeaders(headers);
            } finally {
                // Make sure to always call the gRPC callback to avoid interrupting the gRPC request cycle
                super.sendHeaders(headers);
            }
        }

        @Override
        public void close(final Status status, final Metadata trailers) {
            try {
                if (!sentHeaders) {
                    // gRPC doesn't always send response headers if the call errors or completes immediately
                    addHeaders(trailers);
                }
            } finally {
                // Make sure to always call the gRPC callback to avoid interrupting the gRPC request cycle
                super.close(status, trailers);
            }
        }

        private void addHeaders(final Metadata md) {
            // add any headers that were have been accumulated
            extraHeaders.forEach(md::put);

            // add the bearer header if applicable
            if (session != null) {
                final SessionService.TokenExpiration exp = service.refreshToken(session);
                if (exp != null) {
                    md.put(SESSION_HEADER_KEY, Auth2Constants.BEARER_PREFIX + exp.token.toString());
                }
            }
        }
    }

    @Singleton
    public static class AuthServerInterceptor implements ServerInterceptor {
        private final SessionService service;

        @Inject()
        public AuthServerInterceptor(final SessionService service) {
            this.service = service;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
                final Metadata metadata,
                final ServerCallHandler<ReqT, RespT> serverCallHandler) {
            SessionState session = null;
            final byte[] altToken = metadata.get(AuthConstants.TOKEN_KEY);
            if (altToken != null) {
                try {
                    session = service.getSessionForToken(UUID.fromString(new String(altToken)));
                } catch (IllegalArgumentException ignored) {
                }
            }

            final String token = metadata.get(SESSION_HEADER_KEY);
            if (session == null && token != null) {
                try {
                    session = service.getSessionForAuthToken(token);
                } catch (AuthenticationException e) {
                    log.error().append("Failed to authenticate: ").append(e).endl();
                    throw Status.UNAUTHENTICATED.asRuntimeException();
                }
            }
            final InterceptedCall<ReqT, RespT> serverCall = new InterceptedCall<>(service, call, session);
            final Context newContext = Context.current().withValues(
                    SESSION_CONTEXT_KEY, session, SESSION_CALL_KEY, serverCall);
            return Contexts.interceptCall(newContext, serverCall, metadata, serverCallHandler);
        }
    }
}
