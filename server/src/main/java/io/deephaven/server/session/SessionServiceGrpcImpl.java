/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.auth.AuthContext;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.csv.util.MutableObject;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.*;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.function.ThrowingRunnable;
import io.grpc.Context;
import io.grpc.ForwardingServerCall.SimpleForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.auth.AuthConstants;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.lang.Object;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class SessionServiceGrpcImpl extends SessionServiceGrpc.SessionServiceImplBase {
    /**
     * Deprecated, use {@link Auth2Constants#AUTHORIZATION_HEADER} instead.
     */
    @Deprecated
    public static final String DEEPHAVEN_SESSION_ID = Auth2Constants.AUTHORIZATION_HEADER;
    public static final Metadata.Key<String> SESSION_HEADER_KEY =
            Metadata.Key.of(Auth2Constants.AUTHORIZATION_HEADER, Metadata.ASCII_STRING_MARSHALLER);
    public static final Context.Key<SessionState> SESSION_CONTEXT_KEY =
            Context.key(Auth2Constants.AUTHORIZATION_HEADER);

    private static final String SERVER_CALL_ID = "SessionServiceGrpcImpl.ServerCall";
    private static final Context.Key<InterceptedCall<?, ?>> SESSION_CALL_KEY = Context.key(SERVER_CALL_ID);

    private static final Logger log = LoggerFactory.getLogger(SessionServiceGrpcImpl.class);

    private final SessionService service;
    private final TicketRouter ticketRouter;

    @Inject
    public SessionServiceGrpcImpl(
            final SessionService service,
            final TicketRouter ticketRouter) {
        this.service = service;
        this.ticketRouter = ticketRouter;
    }

    @Override
    public void newSession(
            @NotNull final HandshakeRequest request,
            @NotNull final StreamObserver<HandshakeResponse> responseObserver) {
        // TODO: once jsapi is updated to use flight auth, then newSession can be deprecated or removed
        final AuthContext authContext = new AuthContext.SuperUser();

        final SessionState session = service.newSession(authContext);
        responseObserver.onNext(HandshakeResponse.newBuilder()
                .setMetadataHeader(ByteString.copyFromUtf8(Auth2Constants.AUTHORIZATION_HEADER))
                .setSessionToken(session.getExpiration().getBearerTokenAsByteString())
                .setTokenDeadlineTimeMillis(session.getExpiration().deadlineMillis)
                .setTokenExpirationDelayMillis(service.getExpirationDelayMs())
                .build());

        responseObserver.onCompleted();
    }

    @Override
    public void refreshSessionToken(
            @NotNull final HandshakeRequest request,
            @NotNull final StreamObserver<HandshakeResponse> responseObserver) {
        // TODO: once jsapi is updated to use flight auth, then newSession can be deprecated or removed
        if (request.getAuthProtocol() != 0) {
            responseObserver.onError(
                    Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Protocol version not allowed."));
            return;
        }

        final SessionState session = service.getCurrentSession();
        final SessionService.TokenExpiration expiration = service.refreshToken(session);

        responseObserver.onNext(HandshakeResponse.newBuilder()
                .setMetadataHeader(ByteString.copyFromUtf8(Auth2Constants.AUTHORIZATION_HEADER))
                .setSessionToken(expiration.getBearerTokenAsByteString())
                .setTokenDeadlineTimeMillis(expiration.deadlineMillis)
                .setTokenExpirationDelayMillis(service.getExpirationDelayMs())
                .build());

        responseObserver.onCompleted();
    }

    @Override
    public void closeSession(
            @NotNull final HandshakeRequest request,
            @NotNull final StreamObserver<CloseSessionResponse> responseObserver) {
        if (request.getAuthProtocol() != 0) {
            responseObserver.onError(
                    Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Protocol version not allowed."));
            return;
        }

        final SessionState session = service.getCurrentSession();
        service.closeSession(session);
        responseObserver.onNext(CloseSessionResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void release(
            @NotNull final ReleaseRequest request,
            @NotNull final StreamObserver<ReleaseResponse> responseObserver) {
        final SessionState session = service.getCurrentSession();

        if (!request.hasId()) {
            responseObserver
                    .onError(Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Release ticket not supplied"));
            return;
        }
        final SessionState.ExportObject<?> export = session.getExportIfExists(request.getId(), "id");
        if (export == null) {
            responseObserver.onError(Exceptions.statusRuntimeException(Code.UNAVAILABLE, "Export not yet defined"));
            return;
        }

        // If the export is already in a terminal state, the implementation quietly ignores the request as there
        // are no additional resources to release.
        export.cancel();
        responseObserver.onNext(ReleaseResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void exportFromTicket(
            @NotNull final ExportRequest request,
            @NotNull final StreamObserver<ExportResponse> responseObserver) {
        final SessionState session = service.getCurrentSession();

        if (!request.hasSourceId()) {
            responseObserver
                    .onError(Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Source ticket not supplied"));
            return;
        }
        if (!request.hasResultId()) {
            responseObserver
                    .onError(Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Result ticket not supplied"));
            return;
        }

        final SessionState.ExportObject<Object> source = ticketRouter.resolve(
                session, request.getSourceId(), "sourceId");
        session.newExport(request.getResultId(), "resultId")
                .require(source)
                .onError(responseObserver)
                .submit(() -> {
                    final Object o = source.get();
                    GrpcUtil.safelyComplete(responseObserver, ExportResponse.getDefaultInstance());
                    return o;
                });
    }

    @Override
    public void publishFromTicket(
            @NotNull final PublishRequest request,
            @NotNull final StreamObserver<PublishResponse> responseObserver) {
        final SessionState session = service.getCurrentSession();

        if (!request.hasSourceId()) {
            responseObserver
                    .onError(Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Source ticket not supplied"));
            return;
        }
        if (!request.hasResultId()) {
            responseObserver
                    .onError(Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "Result ticket not supplied"));
            return;
        }

        final SessionState.ExportObject<Object> source = ticketRouter.resolve(
                session, request.getSourceId(), "sourceId");
        Ticket resultId = request.getResultId();

        final SessionState.ExportBuilder<Object> publisher = ticketRouter.publish(
                session, resultId, "resultId", () -> {
                    // when publish is complete, complete the gRPC request
                    GrpcUtil.safelyComplete(responseObserver, PublishResponse.getDefaultInstance());
                });
        publisher.require(source)
                .onError(responseObserver)
                .submit(source::get);
    }

    @Override
    public void exportNotifications(
            @NotNull final ExportNotificationRequest request,
            @NotNull final StreamObserver<ExportNotification> responseObserver) {
        final SessionState session = service.getCurrentSession();

        session.addExportListener(responseObserver);
        ((ServerCallStreamObserver<ExportNotification>) responseObserver).setOnCancelHandler(() -> {
            session.removeExportListener(responseObserver);
        });
    }

    @Override
    public void terminationNotification(
            @NotNull final TerminationNotificationRequest request,
            @NotNull final StreamObserver<TerminationNotificationResponse> responseObserver) {
        final SessionState session = service.getCurrentSession();
        service.addTerminationListener(session, responseObserver);
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
    public static class SessionServiceInterceptor implements ServerInterceptor {
        private final SessionService service;
        private final SessionService.ErrorTransformer errorTransformer;
        private static final Status authenticationDetailsInvalid =
                Status.UNAUTHENTICATED.withDescription("Authentication details invalid");

        @Inject
        public SessionServiceInterceptor(
                final SessionService service,
                final SessionService.ErrorTransformer errorTransformer) {
            this.service = service;
            this.errorTransformer = errorTransformer;
        }

        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
                final Metadata metadata,
                final ServerCallHandler<ReqT, RespT> serverCallHandler) {
            SessionState session = null;

            // Lookup the session using Flight Auth 1.0 token.
            final byte[] altToken = metadata.get(AuthConstants.TOKEN_KEY);
            if (altToken != null) {
                try {
                    session = service.getSessionForToken(UUID.fromString(new String(altToken)));
                } catch (IllegalArgumentException ignored) {
                }
            }

            // Lookup the session using Flight Auth 2.0 token.
            final String token = metadata.get(SESSION_HEADER_KEY);
            if (session == null && token != null) {
                try {
                    session = service.getSessionForAuthToken(token);
                } catch (AuthenticationException e) {
                    try {
                        call.close(authenticationDetailsInvalid, new Metadata());
                    } catch (IllegalStateException ignored) {
                        // could be thrown if the call was already closed. As an interceptor, we can't throw,
                        // so ignoring this and just returning the no-op listener.
                    }
                    return new ServerCall.Listener<>() {};
                }
            }

            // On the outer half of the call we'll install the context that includes our session.
            final InterceptedCall<ReqT, RespT> serverCall = new InterceptedCall<>(service, call, session);
            final Context context = Context.current().withValues(
                    SESSION_CONTEXT_KEY, session, SESSION_CALL_KEY, serverCall);

            final SessionState finalSession = session;

            final MutableObject<SessionServiceCallListener<ReqT, RespT>> listener = new MutableObject<>();
            rpcWrapper(serverCall, context, finalSession, errorTransformer, () -> listener.setValue(
                    new SessionServiceCallListener<>(serverCallHandler.startCall(serverCall, metadata), serverCall,
                            context, finalSession, errorTransformer)));
            if (listener.getValue() == null) {
                return new ServerCall.Listener<>() {};
            }
            return listener.getValue();
        }
    }

    private static class SessionServiceCallListener<ReqT, RespT> extends
            ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {
        private final ServerCall<ReqT, RespT> call;
        private final Context context;
        private final SessionState session;
        private final SessionService.ErrorTransformer errorTransformer;

        public SessionServiceCallListener(
                ServerCall.Listener<ReqT> delegate,
                ServerCall<ReqT, RespT> call,
                Context context,
                SessionState session,
                SessionService.ErrorTransformer errorTransformer) {
            super(delegate);
            this.call = call;
            this.context = context;
            this.session = session;
            this.errorTransformer = errorTransformer;
        }

        @Override
        public void onMessage(ReqT message) {
            rpcWrapper(call, context, session, errorTransformer, () -> super.onMessage(message));
        }

        @Override
        public void onHalfClose() {
            rpcWrapper(call, context, session, errorTransformer, super::onHalfClose);
        }

        @Override
        public void onCancel() {
            rpcWrapper(call, context, session, errorTransformer, super::onCancel);
        }

        @Override
        public void onComplete() {
            rpcWrapper(call, context, session, errorTransformer, super::onComplete);
        }

        @Override
        public void onReady() {
            rpcWrapper(call, context, session, errorTransformer, super::onReady);
        }
    }

    /**
     * Utility to avoid errors escaping to the stream, to make sure the server log and client both see the message if
     * there is an error, and if the error was not meant to propagate to a gRPC client, obfuscates it.
     *
     * @param call the gRPC call
     * @param context the gRPC context to attach
     * @param session the session that this gRPC call is associated with
     * @param lambda the code to safely execute
     */
    private static <ReqT, RespT> void rpcWrapper(
            @NotNull final ServerCall<ReqT, RespT> call,
            @NotNull final Context context,
            @Nullable final SessionState session,
            @NotNull final SessionService.ErrorTransformer errorTransformer,
            @NotNull final ThrowingRunnable<InterruptedException> lambda) {
        Context previous = context.attach();
        // note: we'll open the execution context here so that it may be used by the error transformer
        try (final SafeCloseable ignored1 = session == null ? null : session.getExecutionContext().open()) {
            try (final SafeCloseable ignored2 = LivenessScopeStack.open()) {
                lambda.run();
            } catch (final InterruptedException err) {
                Thread.currentThread().interrupt();
                closeWithError(call, errorTransformer.transform(err));
            } catch (final Throwable err) {
                closeWithError(call, errorTransformer.transform(err));
            } finally {
                context.detach(previous);
            }
        }
    }

    private static <ReqT, RespT> void closeWithError(
            @NotNull final ServerCall<ReqT, RespT> call,
            @NotNull final StatusRuntimeException err) {
        try {
            Metadata metadata = Status.trailersFromThrowable(err);
            if (metadata == null) {
                metadata = new Metadata();
            }
            call.close(Status.fromThrowable(err), metadata);
        } catch (final Exception unexpectedErr) {
            log.debug().append("Unanticipated gRPC Error: ").append(unexpectedErr).endl();
        }
    }
}
