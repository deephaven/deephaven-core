package io.deephaven.grpc_api.session;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.protobuf.ByteString;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.util.auth.AuthContext;
import io.deephaven.grpc_api.util.Scheduler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;
import java.util.Deque;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

@Singleton
public class SessionService {

    static final long MIN_COOKIE_EXPIRE_MS = 10_000; // 10 seconds

    private final Scheduler scheduler;
    private final SessionState.Factory sessionFactory;

    private final long tokenExpireMs;
    private final long tokenRotateMs;

    private final Map<UUID, TokenExpiration> tokenToSession = new ConcurrentHashMap<>();
    private final Deque<TokenExpiration> outstandingCookies = new ConcurrentLinkedDeque<>();
    private boolean cleanupJobInstalled = false;
    private final SessionCleanupJob sessionCleanupJob = new SessionCleanupJob();

    @Inject()
    public SessionService(final Scheduler scheduler, final SessionState.Factory sessionFactory,
        @Named("session.tokenExpireMs") final long tokenExpireMs) {
        this.scheduler = scheduler;
        this.sessionFactory = sessionFactory;
        this.tokenExpireMs = tokenExpireMs;

        if (tokenExpireMs < MIN_COOKIE_EXPIRE_MS) {
            throw new IllegalArgumentException(
                "session.tokenExpireMs is set too low. It is configured to "
                    + tokenExpireMs + "ms (minimum is " + MIN_COOKIE_EXPIRE_MS
                    + "ms). At low levels it is difficult "
                    + "to guarantee smooth operability given a distributed system and potential clock drift");
        }

        // Protect ourselves from rotation spam, but be loose enough that any reasonable refresh
        // strategy works.
        this.tokenRotateMs = tokenExpireMs / 5;
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
     * If enough time has passed since the last token refresh, rotate to a new token and reset the
     * expiration deadline.
     *
     * @param session the session to refresh
     * @return the most recent token expiration
     */
    public TokenExpiration refreshToken(final SessionState session) {
        return refreshToken(session, false);
    }

    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private TokenExpiration refreshToken(final SessionState session, boolean initialToken) {
        UUID newUUID;
        TokenExpiration expiration;
        final DBDateTime now = scheduler.currentTime();

        synchronized (session) {
            expiration = session.getExpiration();
            if (expiration != null && expiration.deadline.getMillis() - tokenExpireMs
                + tokenRotateMs > now.getMillis()) {
                // current token is not old enough to rotate
                return expiration;
            }

            do {
                newUUID = UuidCreator.getRandomBased();
                expiration = new TokenExpiration(newUUID,
                    DBTimeUtils.millisToTime(now.getMillis() + tokenExpireMs), session);
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
     * Lookup a session via the SessionServiceGrpcImpl.SESSION_CONTEXT_KEY. This method is only
     * valid in the context of the original calling gRPC thread.
     *
     * @return the session attached to this gRPC request
     * @throws StatusRuntimeException if thread is not attached to a session or if the session is
     *         expired/closed
     */
    public SessionState getCurrentSession() {
        final SessionState session = getOptionalSession();
        if (session == null) {
            throw new StatusRuntimeException(Status.UNAUTHENTICATED);
        }
        return session;
    }

    /**
     * Lookup a session via the SessionServiceGrpcImpl.SESSION_CONTEXT_KEY. This method is only
     * valid in the context of the original calling gRPC thread.
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
        public final DBDateTime deadline;
        public final SessionState session;

        public TokenExpiration(final UUID cookie, final DBDateTime deadline,
            final SessionState session) {
            this.token = cookie;
            this.deadline = deadline;
            this.session = session;
        }

        /**
         * Returns the UUID cookie in byte[] friendly format.
         */
        public ByteString getTokenAsByteString() {
            return ByteString.copyFromUtf8(UuidCreator.toString(token));
        }
    }

    private final class SessionCleanupJob implements Runnable {
        @Override
        public void run() {
            final DBDateTime now = scheduler.currentTime();

            do {
                final TokenExpiration next = outstandingCookies.peek();
                if (next == null || next.deadline.getMillis() > now.getMillis()) {
                    break;
                }

                // Permanently remove the first token as it is officially expired, note that other
                // tokens may exist for
                // this session, so the session itself does not expire. We allow multiple tokens to
                // co-exist to best
                // support out of order requests and thus allow any reasonable client behavior that
                // respects a given
                // token expiration time.
                outstandingCookies.poll();

                synchronized (next.session) {
                    if (next.session.getExpiration() != null
                        && next.session.getExpiration().deadline.getMillis() <= now.getMillis()) {
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
}
