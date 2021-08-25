package io.deephaven.grpc_api.session;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.grpc_api.util.TestControlledScheduler;
import io.deephaven.grpc_api.util.TestUtil;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.auth.AuthContext;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SessionServiceTest {

    private static final long TOKEN_EXPIRE_MS = 1_000_000;
    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private SafeCloseable livenessScope;
    private TestControlledScheduler scheduler;
    private SessionService sessionService;

    @Before
    public void setup() {
        livenessScope = LivenessScopeStack.open();
        scheduler = new TestControlledScheduler();
        sessionService = new SessionService(scheduler,
            authContext -> new SessionState(scheduler, authContext), TOKEN_EXPIRE_MS);
    }

    @After
    public void teardown() {
        livenessScope.close();

        scheduler = null;
        sessionService = null;
        livenessScope = null;
    }

    @Test
    public void testSessionExpiresOnClose() {
        final SessionState session;

        // Create a thrown away scope: (otherwise session is also managed by the unit test)
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session = sessionService.newSession(AUTH_CONTEXT);
        }
        final SessionService.TokenExpiration expiration = session.getExpiration();

        Assert.eqFalse(session.isExpired(), "session.isExpired()");
        Assert.neqNull(session.getExpiration(), "session.getExpiration()");
        sessionService.closeSession(session);
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        Assert.eqNull(session.getExpiration(), "session.getExpiration()");
        Assert.eqNull(sessionService.getSessionForToken(expiration.token),
            "sessionService.getSessionForToken(expiration.token)");
    }

    @Test
    public void testTokenRotationHasSpamProtection() {
        // because we need to keep some state per refresh token, we must protect slightly from
        // accidental DOS spam
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        final SessionService.TokenExpiration initialToken = session.getExpiration();
        Assert.eq(sessionService.refreshToken(session), "sessionService.refreshToken(session)",
            initialToken, "initialToken");
    }

    @Test
    public void testTokenRotation() {
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        final SessionService.TokenExpiration initialToken = session.getExpiration();

        // let's advance by some reasonable amount and ensure that the token now refreshes
        scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));
        final SessionService.TokenExpiration newToken = sessionService.refreshToken(session);
        final long timeToNewExpiration =
            newToken.deadline.getMillis() - scheduler.currentTime().getMillis();
        Assert.eq(timeToNewExpiration, "timeToNewExpiration", TOKEN_EXPIRE_MS);

        // ensure that the UUIDs are different so they may expire independently
        Assert.neq(newToken.token, "newToken.token", initialToken.token, "initialToken.token");
    }

    @Test
    public void testExpirationClosesSession() {
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        Assert.eqFalse(session.isExpired(), "session.isExpired()");
        scheduler.runThrough(session.getExpiration().deadline);
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
    }

    @Test
    public void testOldTokenExpirationDoesNotCloseSession() {
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        final SessionService.TokenExpiration initialToken = session.getExpiration();

        // advance so we can rotate token
        scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));
        Assert.eqFalse(session.isExpired(), "session.isExpired()");
        sessionService.refreshToken(session);

        // expire initial token
        scheduler.runThrough(initialToken.deadline);
        Assert.eqFalse(session.isExpired(), "session.isExpired()");

        // expire refreshed token
        scheduler.runThrough(session.getExpiration().deadline);
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
    }

    @Test
    public void testTokenLookup() {
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        final SessionService.TokenExpiration initialToken = session.getExpiration();
        Assert.eq(sessionService.getSessionForToken(initialToken.token),
            "sessionService.getSessionForToken(initialToken.token)", session, "session");

        // advance so we can rotate token
        scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));
        Assert.eqFalse(session.isExpired(), "session.isExpired()");
        final SessionService.TokenExpiration newToken = sessionService.refreshToken(session);

        // check both tokens are valid
        Assert.eq(sessionService.getSessionForToken(initialToken.token),
            "sessionService.getSessionForToken(initialToken.token)", session, "session");
        Assert.eq(sessionService.getSessionForToken(newToken.token),
            "sessionService.getSessionForToken(newToken.token)", session, "session");

        // expire original token; current token should be valid
        scheduler.runThrough(initialToken.deadline);
        Assert.eqNull(sessionService.getSessionForToken(initialToken.token),
            "sessionService.getSessionForToken(initialToken.token)");
        Assert.eq(sessionService.getSessionForToken(newToken.token),
            "sessionService.getSessionForToken(newToken.token)", session, "session");

        // let's expire the new token
        scheduler.runThrough(session.getExpiration().deadline);
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        Assert.eqNull(sessionService.getSessionForToken(newToken.token),
            "sessionService.getSessionForToken(newToken.token)");
    }

    @Test
    public void testSessionsAreIndependent() {
        final SessionState session1 = sessionService.newSession(AUTH_CONTEXT);
        final SessionState session2 = sessionService.newSession(AUTH_CONTEXT);
        Assert.neq(session1, "session1", session2, "session2");

        // advance so we can rotate token
        scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));
        final SessionService.TokenExpiration expiration1 = sessionService.refreshToken(session1);
        final SessionService.TokenExpiration expiration2 = session2.getExpiration();

        Assert.lt(expiration2.deadline.getNanos(), "expiration2.deadline",
            expiration1.deadline.getNanos(), "expiration1.deadline");
        scheduler.runThrough(expiration2.deadline);

        // first session is live
        Assert.eqFalse(session1.isExpired(), "session2.isExpired()");
        Assert.eq(sessionService.getSessionForToken(expiration1.token),
            "sessionService.getSessionForToken(expiration1.token)", session1, "session1");
        Assert.eqNull(sessionService.getSessionForToken(expiration2.token),
            "sessionService.getSessionForToken(initialToken.token)");

        // second session has expired
        Assert.eqTrue(session2.isExpired(), "session2.isExpired()");
        Assert.eqNull(sessionService.getSessionForToken(expiration2.token),
            "sessionService.getSessionForToken(initialToken.token)");
    }
}
