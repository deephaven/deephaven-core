//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.github.f4b6a3.uuid.UuidCreator;
import com.github.f4b6a3.uuid.exception.InvalidUuidException;
import com.google.protobuf.ByteString;
import com.google.protobuf.ByteStringAccess;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.rpc.Code;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.auth.BasicAuthMarshaller;
import io.deephaven.engine.table.impl.perf.QueryPerformanceNugget;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.util.EngineMetrics;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.WrappedAuthenticationRequest;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.auth.AuthContext;
import io.deephaven.util.SafeCloseable;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.FlightServiceGrpc;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;

@Singleton
public class FlightServiceGrpcImpl extends FlightServiceGrpc.FlightServiceImplBase {
    private static final Logger log = LoggerFactory.getLogger(FlightServiceGrpcImpl.class);

    private final ScheduledExecutorService executorService;
    private final BarrageMessageWriter.Factory streamGeneratorFactory;
    private final SessionService sessionService;
    private final SessionService.ErrorTransformer errorTransformer;
    private final TicketRouter ticketRouter;
    private final ArrowFlightUtil.DoExchangeMarshaller.Factory doExchangeFactory;

    private final Map<String, AuthenticationRequestHandler> authRequestHandlers;

    @Inject
    public FlightServiceGrpcImpl(
            @Nullable final ScheduledExecutorService executorService,
            final BarrageMessageWriter.Factory streamGeneratorFactory,
            final SessionService sessionService,
            final SessionService.ErrorTransformer errorTransformer,
            final TicketRouter ticketRouter,
            final ArrowFlightUtil.DoExchangeMarshaller.Factory doExchangeFactory,
            Map<String, AuthenticationRequestHandler> authRequestHandlers) {
        this.executorService = executorService;
        this.streamGeneratorFactory = streamGeneratorFactory;
        this.sessionService = sessionService;
        this.errorTransformer = errorTransformer;
        this.ticketRouter = ticketRouter;
        this.doExchangeFactory = doExchangeFactory;
        this.authRequestHandlers = authRequestHandlers;
    }

    @Override
    public StreamObserver<Flight.HandshakeRequest> handshake(
            @NotNull final StreamObserver<Flight.HandshakeResponse> responseObserver) {
        // handle the scenario where authentication headers initialized a session
        SessionState session = sessionService.getOptionalSession();
        if (session != null) {
            // Do not reply over the stream, some clients will break if they receive a message here - but since the
            // session was already created, our "200 OK" will include the Bearer response already. Do not close
            // yet to avoid hitting https://github.com/envoyproxy/envoy/issues/30149.
            return new StreamObserver<>() {
                @Override
                public void onNext(Flight.HandshakeRequest value) {
                    // noop, already sent response
                }

                @Override
                public void onError(Throwable t) {
                    // ignore, already closed
                }

                @Override
                public void onCompleted() {
                    GrpcUtil.safelyComplete(responseObserver);
                }
            };
        }

        return new HandshakeObserver(responseObserver);
    }

    private final class HandshakeObserver implements StreamObserver<Flight.HandshakeRequest> {

        private boolean isComplete = false;
        private final StreamObserver<Flight.HandshakeResponse> responseObserver;

        private HandshakeObserver(StreamObserver<Flight.HandshakeResponse> responseObserver) {
            this.responseObserver = responseObserver;
        }

        @Override
        public void onNext(final Flight.HandshakeRequest value) {
            final AuthenticationRequestHandler.HandshakeResponseListener handshakeResponseListener =
                    (protocol, response) -> {
                        GrpcUtil.safelyComplete(responseObserver, Flight.HandshakeResponse.newBuilder()
                                .setProtocolVersion(protocol)
                                .setPayload(ByteStringAccess.wrap(response))
                                .build());
                    };

            final ByteString payload = value.getPayload();
            final long protocolVersion = value.getProtocolVersion();
            Optional<AuthContext> auth;
            try {
                auth = login(BasicAuthMarshaller.AUTH_TYPE, protocolVersion, payload, handshakeResponseListener);
                if (auth.isEmpty()) {
                    final WrappedAuthenticationRequest req = WrappedAuthenticationRequest.parseFrom(payload);
                    // If the auth request is bearer, the v1 auth might be trying to renew an existing session
                    if (req.getType().equals(Auth2Constants.BEARER_PREFIX.trim())) {
                        try {
                            UUID uuid = UuidCreator.fromString(req.getPayload().toString(StandardCharsets.US_ASCII));
                            SessionState session = sessionService.getSessionForToken(uuid);
                            if (session != null) {
                                SessionService.TokenExpiration expiration = session.getExpiration();
                                if (expiration != null) {
                                    respondWithAuthTokenBin(expiration);
                                }
                            }
                            return;
                        } catch (IllegalArgumentException | InvalidUuidException ignored) {
                        }
                    }

                    // Attempt to log in with the given type and token
                    auth = login(req.getType(), protocolVersion, req.getPayload(), handshakeResponseListener);
                }
            } catch (final AuthenticationException | InvalidProtocolBufferException err) {
                log.error().append("Authentication failed: ").append(err).endl();
                auth = Optional.empty();
            }

            if (auth.isEmpty()) {
                responseObserver.onError(
                        Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "Authentication details invalid"));
                return;
            }

            SessionState session = sessionService.newSession(auth.get());
            respondWithAuthTokenBin(session.getExpiration());
        }

        private Optional<AuthContext> login(String type, long version, ByteString payload,
                AuthenticationRequestHandler.HandshakeResponseListener listener) throws AuthenticationException {
            AuthenticationRequestHandler handler = authRequestHandlers.get(type);
            if (handler == null) {
                log.info().append("No AuthenticationRequestHandler registered for type ").append(type).endl();
                return Optional.empty();
            }
            return handler.login(version, payload.asReadOnlyByteBuffer(), listener);
        }

        /** send the bearer token as an AuthTokenBin, as headers might have already been sent */
        private void respondWithAuthTokenBin(SessionService.TokenExpiration expiration) {
            isComplete = true;
            responseObserver.onNext(Flight.HandshakeResponse.newBuilder()
                    .setPayload(expiration.getTokenAsByteString())
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public void onError(final Throwable t) {
            // ignore
        }

        @Override
        public void onCompleted() {
            if (isComplete) {
                return;
            }
            responseObserver.onError(
                    Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, "no authentication details provided"));
        }
    }

    @Override
    public void listFlights(
            @NotNull final Flight.Criteria request,
            @NotNull final StreamObserver<Flight.FlightInfo> responseObserver) {
        ticketRouter.visitFlightInfo(sessionService.getOptionalSession(), responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void getFlightInfo(
            @NotNull final Flight.FlightDescriptor request,
            @NotNull final StreamObserver<Flight.FlightInfo> responseObserver) {
        final SessionState session = sessionService.getOptionalSession();

        final String description = "FlightService#getFlightInfo(request=" + request + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session == null ? null : session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Flight.FlightInfo> export =
                    ticketRouter.flightInfoFor(session, request, "request");

            if (session != null) {
                session.nonExport()
                        .queryPerformanceRecorder(queryPerformanceRecorder)
                        .require(export)
                        .onError(responseObserver)
                        .submit(() -> {
                            responseObserver.onNext(export.get());
                            responseObserver.onCompleted();
                        });
                return;
            }

            StatusRuntimeException exception = null;
            if (export.tryRetainReference()) {
                try {
                    if (export.getState() == ExportNotification.State.EXPORTED) {
                        GrpcUtil.safelyOnNext(responseObserver, export.get());
                        GrpcUtil.safelyComplete(responseObserver);
                    }
                } finally {
                    export.dropReference();
                }
            } else {
                exception = Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not find flight info");
                GrpcUtil.safelyError(responseObserver, exception);
            }

            if (queryPerformanceRecorder.endQuery() || exception != null) {
                EngineMetrics.getInstance().logQueryProcessingResults(queryPerformanceRecorder, exception);
            }
        }
    }

    @Override
    public void getSchema(
            @NotNull final Flight.FlightDescriptor request,
            @NotNull final StreamObserver<Flight.SchemaResult> responseObserver) {
        final SessionState session = sessionService.getOptionalSession();

        final String description = "FlightService#getSchema(request=" + request + ")";
        final QueryPerformanceRecorder queryPerformanceRecorder = QueryPerformanceRecorder.newQuery(
                description, session == null ? null : session.getSessionId(), QueryPerformanceNugget.DEFAULT_FACTORY);

        try (final SafeCloseable ignored = queryPerformanceRecorder.startQuery()) {
            final SessionState.ExportObject<Flight.FlightInfo> export =
                    ticketRouter.flightInfoFor(session, request, "request");

            if (session != null) {
                session.nonExport()
                        .queryPerformanceRecorder(queryPerformanceRecorder)
                        .require(export)
                        .onError(responseObserver)
                        .submit(() -> {
                            responseObserver.onNext(Flight.SchemaResult.newBuilder()
                                    .setSchema(export.get().getSchema())
                                    .build());
                            responseObserver.onCompleted();
                        });
                return;
            }

            StatusRuntimeException exception = null;
            if (export.tryRetainReference()) {
                try {
                    if (export.getState() == ExportNotification.State.EXPORTED) {
                        GrpcUtil.safelyOnNext(responseObserver, Flight.SchemaResult.newBuilder()
                                .setSchema(export.get().getSchema())
                                .build());
                        GrpcUtil.safelyComplete(responseObserver);
                    }
                } finally {
                    export.dropReference();
                }
            } else {
                exception = Exceptions.statusRuntimeException(Code.FAILED_PRECONDITION, "Could not find flight info");
                responseObserver.onError(exception);
            }

            if (queryPerformanceRecorder.endQuery() || exception != null) {
                EngineMetrics.getInstance().logQueryProcessingResults(queryPerformanceRecorder, exception);
            }
        }
    }

    public void doGetCustom(
            final Flight.Ticket request,
            final StreamObserver<InputStream> responseObserver) {
        ArrowFlightUtil.DoGetCustom(
                streamGeneratorFactory, sessionService.getCurrentSession(), ticketRouter, request, responseObserver);
    }

    /**
     * Establish a new DoPut bi-directional stream.
     *
     * @param responseObserver the observer to reply to
     * @return the observer that grpc can delegate received messages to
     */
    public StreamObserver<InputStream> doPutCustom(final StreamObserver<Flight.PutResult> responseObserver) {
        return new ArrowFlightUtil.DoPutObserver(
                sessionService.getCurrentSession(), ticketRouter, errorTransformer, responseObserver);
    }

    /**
     * Establish a new DoExchange bi-directional stream.
     *
     * @param responseObserver the observer to reply to
     * @return the observer that grpc can delegate received messages to
     */
    public StreamObserver<InputStream> doExchangeCustom(final StreamObserver<InputStream> responseObserver) {
        return doExchangeFactory.openExchange(sessionService.getCurrentSession(), responseObserver);
    }
}
