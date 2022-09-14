/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.ByteString;
import io.deephaven.auth.AuthenticationException;
import io.deephaven.auth.AuthenticationRequestHandler;
import io.deephaven.auth.BasicAuthMarshaller;
import io.deephaven.configuration.Configuration;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.deephaven.server.util.Scheduler;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.auth.AuthContext;
import io.deephaven.util.process.ProcessEnvironment;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.auth2.Auth2Constants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Singleton
public class SessionService {

    static final long MIN_COOKIE_EXPIRE_MS = 10_000; // 10 seconds
    private static final int MAX_STACK_TRACE_CAUSAL_DEPTH =
            Configuration.getInstance().getIntegerForClassWithDefault(SessionService.class,
                    "maxStackTraceCausedByDepth", 20);
    private static final int MAX_STACK_TRACE_DEPTH =
            Configuration.getInstance().getIntegerForClassWithDefault(SessionService.class,
                    "maxStackTraceDepth", 50);

    private final Scheduler scheduler;
    private final SessionState.Factory sessionFactory;

    private final long tokenExpireMs;
    private final long tokenRotateMs;

    private final Map<UUID, TokenExpiration> tokenToSession = new ConcurrentHashMap<>();
    private final Deque<TokenExpiration> outstandingCookies = new ConcurrentLinkedDeque<>();
    private boolean cleanupJobInstalled = false;
    private final SessionCleanupJob sessionCleanupJob = new SessionCleanupJob();

    private final List<TerminationNotificationListener> terminationListeners = new CopyOnWriteArrayList<>();

    private final BasicAuthMarshaller basicAuthMarshaller;
    private final Map<String, AuthenticationRequestHandler> authRequestHandlers;

    @Inject()
    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    public SessionService(final Scheduler scheduler, final SessionState.Factory sessionFactory,
            @Named("session.tokenExpireMs") final long tokenExpireMs,
            final Optional<BasicAuthMarshaller> basicAuthMarshaller,
            Map<String, AuthenticationRequestHandler> authRequestHandlers) {
        this.scheduler = scheduler;
        this.sessionFactory = sessionFactory;
        this.tokenExpireMs = tokenExpireMs;
        this.basicAuthMarshaller = basicAuthMarshaller.orElse(null);
        this.authRequestHandlers = authRequestHandlers;

        if (tokenExpireMs < MIN_COOKIE_EXPIRE_MS) {
            throw new IllegalArgumentException("session.tokenExpireMs is set too low. It is configured to "
                    + tokenExpireMs + "ms (minimum is " + MIN_COOKIE_EXPIRE_MS + "ms). At low levels it is difficult "
                    + "to guarantee smooth operability given a distributed system and potential clock drift");
        }
        final long tokenExpireNanos = TimeUnit.MILLISECONDS.toNanos(tokenExpireMs);
        if (tokenExpireNanos == Long.MIN_VALUE || tokenExpireNanos == Long.MAX_VALUE) {
            throw new IllegalArgumentException("session.tokenExpireMs is set too high.");
        }

        // Protect ourselves from rotation spam, but be loose enough that any reasonable run strategy works.
        this.tokenRotateMs = tokenExpireMs / 5;

        if (ProcessEnvironment.tryGet() != null) {
            ProcessEnvironment.getGlobalFatalErrorReporter().addInterceptor(this::onFatalError);
        }
    }

    private synchronized void onFatalError(
            @NotNull String message,
            @NotNull Throwable throwable,
            boolean isFromUncaught) {
        final TerminationNotificationResponse.Builder builder =
                TerminationNotificationResponse.newBuilder()
                        .setAbnormalTermination(true)
                        .setIsFromUncaughtException(isFromUncaught)
                        .setReason(message);

        // TODO (core#801): revisit this error communication to properly match the API Error mode
        for (int depth = 0; throwable != null && depth < MAX_STACK_TRACE_CAUSAL_DEPTH; ++depth) {
            builder.addStackTraces(transformToProtoBuf(throwable));
            throwable = throwable.getCause();
        }

        final TerminationNotificationResponse notification = builder.build();
        terminationListeners.forEach(listener -> listener.sendMessage(notification));
        terminationListeners.clear();
    }

    private static TerminationNotificationResponse.StackTrace transformToProtoBuf(@NotNull final Throwable throwable) {
        return TerminationNotificationResponse.StackTrace.newBuilder()
                .setType(throwable.getClass().getName())
                .setMessage(Objects.toString(throwable.getMessage()))
                .addAllElements(Arrays.stream(throwable.getStackTrace())
                        .limit(MAX_STACK_TRACE_DEPTH)
                        .map(StackTraceElement::toString)
                        .collect(Collectors.toList()))
                .build();
    }

    public synchronized void onShutdown() {
        final TerminationNotificationResponse notification = TerminationNotificationResponse.newBuilder()
                .setAbnormalTermination(false)
                .build();
        terminationListeners.forEach(listener -> listener.sendMessage(notification));
        terminationListeners.clear();

        closeAllSessions();
    }

    /**
     * Add a listener who receives a single notification when this process is exiting and yet able to communicate with
     * the observer.
     *
     * @param session the session the observer belongs to
     * @param responseObserver the observer to notify
     */
    public void addTerminationListener(
            final SessionState session,
            final StreamObserver<TerminationNotificationResponse> responseObserver) {
        terminationListeners.add(new TerminationNotificationListener(session, responseObserver));
    }

    /**
     * Create a new session object for the provided auth context.
     *
     * @param authContext the auth context of the session
     * @return a new session independent of all other existing sessions
     */
    public SessionState newSession(final AuthContext authContext) {
        final SessionState session = sessionFactory.create(authContext);
        refreshToken(session, true);
        return session;
    }

    /**
     * If enough time has passed since the last token run, rotate to a new token and reset the expiration deadline.
     *
     * @param session the session to run
     * @return the most recent token expiration
     */
    public TokenExpiration refreshToken(final SessionState session) {
        return refreshToken(session, false);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private TokenExpiration refreshToken(final SessionState session, boolean initialToken) {
        UUID newUUID;
        TokenExpiration expiration;
        final DateTime now = scheduler.currentTime();

        synchronized (session) {
            expiration = session.getExpiration();
            if (!initialToken) {
                if (expiration == null) {
                    // current token is expired; we should not rotate
                    return null;
                }

                if (expiration.deadline.getMillis() - tokenExpireMs + tokenRotateMs > now.getMillis()) {
                    // current token is not old enough to rotate
                    return expiration;
                }
            }

            do {
                newUUID = UuidCreator.getRandomBased();
                final long tokenExpireNanos = TimeUnit.MILLISECONDS.toNanos(tokenExpireMs);
                expiration = new TokenExpiration(newUUID, DateTimeUtils.plus(now, tokenExpireNanos), session);
            } while (tokenToSession.putIfAbsent(newUUID, expiration) != null);

            if (initialToken) {
                session.initializeExpiration(expiration);
            } else {
                session.updateExpiration(expiration);
            }
        }
        outstandingCookies.addLast(expiration);

        synchronized (this) {
            if (!cleanupJobInstalled) {
                cleanupJobInstalled = true;
                scheduler.runAtTime(expiration.deadline, sessionCleanupJob);
            }
        }

        return expiration;
    }

    /**
     * @return the configured token duration in milliseconds
     */
    public long getExpirationDelayMs() {
        return tokenExpireMs;
    }

    /**
     * Lookup a session by token. Creates a new session if it's a basic auth and it passes.
     *
     * @param token the Authentication header to service
     * @return the session or null if the session is invalid
     */
    public SessionState getSessionForAuthToken(final String token) throws AuthenticationException {
        if (token.startsWith(Auth2Constants.BEARER_PREFIX)) {
            final String authToken = token.substring(Auth2Constants.BEARER_PREFIX.length());
            try {
                UUID uuid = UuidCreator.fromString(authToken);
                SessionState session = getSessionForToken(uuid);
                if (session != null) {
                    return session;
                }
            } catch (IllegalArgumentException ignored) {
            }
        }

        Optional<AuthContext> context = Optional.empty();
        if (token.startsWith(Auth2Constants.BASIC_PREFIX) && basicAuthMarshaller != null) {
            final String payload = token.substring(Auth2Constants.BASIC_PREFIX.length());
            context = basicAuthMarshaller.login(payload, SessionServiceGrpcImpl::insertCallHeader);
        } else {
            int offset = token.indexOf(' ');
            final String key = token.substring(0, offset < 0 ? token.length() : offset);
            final String payload = offset < 0 ? "" : token.substring(offset + 1);
            AuthenticationRequestHandler handler = authRequestHandlers.get(key);
            if (handler != null) {
                context = handler.login(payload, SessionServiceGrpcImpl::insertCallHeader);
            }
        }

        return context.map(this::newSession).orElse(null);
    }

    /**
     * Lookup a session by token.
     *
     * @param token the session secret to look for
     * @return the session or null if the session is invalid
     */
    public SessionState getSessionForToken(final UUID token) {
        final TokenExpiration expiration = tokenToSession.get(token);
        if (expiration == null || expiration.session.isExpired()
                || expiration.deadline.compareTo(scheduler.currentTime()) <= 0) {
            return null;
        }
        return expiration.session;
    }

    /**
     * Lookup a session via the SessionServiceGrpcImpl.SESSION_CONTEXT_KEY. This method is only valid in the context of
     * the original calling gRPC thread.
     *
     * @return the session attached to this gRPC request
     * @throws StatusRuntimeException if thread is not attached to a session or if the session is expired/closed
     */
    @NotNull
    public SessionState getCurrentSession() {
        final SessionState session = getOptionalSession();
        if (session == null) {
            throw Status.UNAUTHENTICATED.asRuntimeException();
        }
        return session;
    }

    /**
     * Lookup a session via the SessionServiceGrpcImpl.SESSION_CONTEXT_KEY. This method is only valid in the context of
     * the original calling gRPC thread.
     *
     * @return the session attached to this gRPC request; null if no session is established
     */
    @Nullable
    public SessionState getOptionalSession() {
        final SessionState session = SessionServiceGrpcImpl.SESSION_CONTEXT_KEY.get();
        if (session == null || session.isExpired()) {
            return null;
        }
        return session;
    }

    /**
     * Reduces the liveness of the session.
     *
     * @param session the session to close
     */
    public void closeSession(final SessionState session) {
        if (session.isExpired()) {
            return;
        }
        session.onExpired();
    }

    public void closeAllSessions() {
        for (final TokenExpiration token : outstandingCookies) {
            // close all exports/resources acquired by the session
            token.session.onExpired();
        }
    }

    public static final class TokenExpiration {
        public final UUID token;
        public final DateTime deadline;
        public final SessionState session;

        public TokenExpiration(final UUID cookie, final DateTime deadline, final SessionState session) {
            this.token = cookie;
            this.deadline = deadline;
            this.session = session;
        }

        /**
         * Returns the bearer token in byte[] friendly format.
         */
        public ByteString getBearerTokenAsByteString() {
            return ByteString.copyFromUtf8(Auth2Constants.BEARER_PREFIX + UuidCreator.toString(token));
        }

        public ByteString getTokenAsByteString() {
            return ByteString.copyFromUtf8(UuidCreator.toString(token));
        }
    }

    private final class SessionCleanupJob implements Runnable {
        @Override
        public void run() {
            final DateTime now = scheduler.currentTime();

            do {
                final TokenExpiration next = outstandingCookies.peek();
                if (next == null || next.deadline.getMillis() > now.getMillis()) {
                    break;
                }

                // Permanently remove the first token as it is officially expired, note that other tokens may exist for
                // this session, so the session itself does not expire. We allow multiple tokens to co-exist to best
                // support out of order requests and thus allow any reasonable client behavior that respects a given
                // token expiration time.
                outstandingCookies.poll();

                synchronized (next.session) {
                    final TokenExpiration tokenExpiration = next.session.getExpiration();
                    if (tokenExpiration != null && tokenExpiration.deadline.getMillis() <= now.getMillis()) {
                        next.session.onExpired();
                    }
                }
            } while (true);

            synchronized (SessionService.this) {
                final TokenExpiration next = outstandingCookies.peek();
                if (next == null) {
                    cleanupJobInstalled = false;
                } else {
                    scheduler.runAtTime(next.deadline, this);
                }
            }
        }
    }

    private final class TerminationNotificationListener
            extends SessionCloseableObserver<TerminationNotificationResponse> {
        public TerminationNotificationListener(
                final SessionState session,
                final StreamObserver<TerminationNotificationResponse> responseObserver) {
            super(session, responseObserver);
        }

        @Override
        void onClose() {
            terminationListeners.remove(this);
        }

        void sendMessage(final TerminationNotificationResponse response) {
            GrpcUtil.safelyExecuteLocked(responseObserver, () -> {
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            });
        }
    }
}
