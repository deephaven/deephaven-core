//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.util.SafeCloseable;
import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.proto.backplane.grpc.TerminationNotificationResponse;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.StatusRuntimeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class SessionServiceTest {

    private static final long TOKEN_EXPIRE_MS = 1_000_000;
    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private SafeCloseable livenessScope;
    private TestControlledScheduler scheduler;
    private SessionService sessionService;
    private Consumer<SessionState> sessionStateCallable;

    @Before
    public void setup() {
        livenessScope = LivenessScopeStack.open();
        scheduler = new TestControlledScheduler();
        sessionService = new SessionService(scheduler,
                authContext -> new SessionState(scheduler, new SessionService.ObfuscatingErrorTransformer(),
                        TestExecutionContext::createForUnitTests, authContext),
                TOKEN_EXPIRE_MS, Collections.emptyMap(), Collections.singleton(this::sessionCreatedCallback));
    }

    private void sessionCreatedCallback(SessionState sessionState) {
        if (sessionStateCallable != null) {
            sessionStateCallable.accept(sessionState);
        }
    }

    @After
    public void teardown() {
        livenessScope.close();

        scheduler = null;
        sessionService = null;
        livenessScope = null;
    }

    @Test
    public void testSessionCreationCallback() {
        AtomicReference<SessionState> sessionReference = new AtomicReference<>(null);
        AtomicInteger count = new AtomicInteger(0);

        sessionStateCallable = newValue -> {
            sessionReference.set(newValue);
            count.incrementAndGet();
        };

        final SessionState session = sessionService.newSession(AUTH_CONTEXT);

        Assert.eq(sessionReference.get(), "sessionReference.get()", session, "session");
        Assert.eq(count.get(), "count.get()", 1);
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
        // because we need to keep some state per run token, we must protect slightly from accidental DOS spam
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        final SessionService.TokenExpiration initialToken = session.getExpiration();
        Assert.eq(sessionService.refreshToken(session), "sessionService.refreshToken(session)", initialToken,
                "initialToken");
    }

    @Test
    public void testTokenRotation() {
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        final SessionService.TokenExpiration initialToken = session.getExpiration();

        // let's advance by some reasonable amount and ensure that the token now refreshes
        scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));
        final SessionService.TokenExpiration newToken = sessionService.refreshToken(session);
        final long timeToNewExpiration = newToken.deadlineMillis - scheduler.currentTimeMillis();
        Assert.eq(timeToNewExpiration, "timeToNewExpiration", TOKEN_EXPIRE_MS);

        // ensure that the UUIDs are different so they may expire independently
        Assert.neq(newToken.token, "newToken.token", initialToken.token, "initialToken.token");
    }

    @Test
    public void testExpirationClosesSession() {
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        Assert.eqFalse(session.isExpired(), "session.isExpired()");
        scheduler.runThrough(session.getExpiration().deadlineMillis);
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
        scheduler.runThrough(initialToken.deadlineMillis);
        Assert.eqFalse(session.isExpired(), "session.isExpired()");

        // expire refreshed token
        scheduler.runThrough(session.getExpiration().deadlineMillis);
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
        scheduler.runThrough(initialToken.deadlineMillis);
        Assert.eqNull(sessionService.getSessionForToken(initialToken.token),
                "sessionService.getSessionForToken(initialToken.token)");
        Assert.eq(sessionService.getSessionForToken(newToken.token),
                "sessionService.getSessionForToken(newToken.token)", session, "session");

        // let's expire the new token
        scheduler.runThrough(session.getExpiration().deadlineMillis);
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

        Assert.lt(expiration2.deadlineMillis, "expiration2.deadline", expiration1.deadlineMillis,
                "expiration1.deadline");
        scheduler.runThrough(expiration2.deadlineMillis);

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

    @Test
    public void testErrorIdDeDupesIdentity() {
        final Exception e1 = new RuntimeException("e1");
        final SessionService.ObfuscatingErrorTransformer transformer = new SessionService.ObfuscatingErrorTransformer();

        final StatusRuntimeException t1 = transformer.transform(e1);
        final StatusRuntimeException t2 = transformer.transform(e1);
        Assert.neq(t1, "t1", t2, "t2");
        Assert.equals(t1.getMessage(), "t1.getMessage()", t2.getMessage(), "t2.getMessage()");
    }

    @Test
    public void testErrorIdDeDupesParentCause() {
        final Exception parent = new RuntimeException("parent");
        final Exception child = new RuntimeException("child", parent);
        final SessionService.ObfuscatingErrorTransformer transformer = new SessionService.ObfuscatingErrorTransformer();

        // important to transform parent then child for this test
        final StatusRuntimeException t1 = transformer.transform(parent);
        final StatusRuntimeException t2 = transformer.transform(child);
        Assert.neq(t1, "t1", t2, "t2");
        Assert.equals(t1.getMessage(), "t1.getMessage()", t2.getMessage(), "t2.getMessage()");
    }

    @Test
    public void testErrorIdDeDupesChildCause() {
        final Exception parent = new RuntimeException("parent");
        final Exception child = new RuntimeException("child", parent);
        final SessionService.ObfuscatingErrorTransformer transformer = new SessionService.ObfuscatingErrorTransformer();

        // important to transform child then parent for this test
        final StatusRuntimeException t1 = transformer.transform(child);
        final StatusRuntimeException t2 = transformer.transform(parent);
        Assert.neq(t1, "t1", t2, "t2");
        Assert.equals(t1.getMessage(), "t1.getMessage()", t2.getMessage(), "t2.getMessage()");
    }

    @Test
    public void testErrorIdDeDupesSharedAncestorCause() {
        final Exception parent = new RuntimeException("parent");
        final Exception child1 = new RuntimeException("child1", parent);
        final Exception child2 = new RuntimeException("child2", parent);
        final SessionService.ObfuscatingErrorTransformer transformer = new SessionService.ObfuscatingErrorTransformer();

        final StatusRuntimeException t1 = transformer.transform(child1);
        final StatusRuntimeException t2 = transformer.transform(child2);
        Assert.neq(t1, "t1", t2, "t2");
        Assert.equals(t1.getMessage(), "t1.getMessage()", t2.getMessage(), "t2.getMessage()");

        final StatusRuntimeException t3 = transformer.transform(parent);
        Assert.neq(t1, "t1", t3, "t3");
        Assert.equals(t1.getMessage(), "t1.getMessage()", t3.getMessage(), "t3.getMessage()");
    }

    @Test
    public void testErrorCausalLimit() {
        final Exception leaf = new RuntimeException("leaf");
        final Exception p1 = new RuntimeException("lastIncluded", leaf);
        Exception p0 = p1;
        for (int i = SessionService.ObfuscatingErrorTransformer.MAX_STACK_TRACE_CAUSAL_DEPTH - 1; i > 0; --i) {
            p0 = new RuntimeException("e" + i, p0);
        }

        final SessionService.ObfuscatingErrorTransformer transformer = new SessionService.ObfuscatingErrorTransformer();
        final StatusRuntimeException t0 = transformer.transform(p0);
        final StatusRuntimeException t1 = transformer.transform(p1);
        Assert.equals(t0.getMessage(), "t0.getMessage()", t1.getMessage(), "t1.getMessage()");

        // this one should not have made it
        final StatusRuntimeException tleaf = transformer.transform(leaf);
        Assert.notEquals(t0.getMessage(), "t0.getMessage()", tleaf.getMessage(), "tleaf.getMessage()");
    }

    /**
     * A shutdown or fatal-error notification sends to every termination listener while holding the SessionService
     * monitor; each send locks the listener's stream observer. A gRPC thread that is closing a call holds that same
     * observer monitor while it refreshes the session token, which locks the SessionService when the token rotates.
     * Both orders must be able to complete.
     */
    @Test
    public void testShutdownNotificationDoesNotDeadlockWithTokenRotation() throws InterruptedException {
        final SessionState session = sessionService.newSession(AUTH_CONTEXT);
        // age the token so that the next refresh rotates it, which is the path that locks the SessionService
        scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));

        final RecordingTerminationObserver observer = new RecordingTerminationObserver();
        sessionService.addTerminationListener(session, observer);

        final CountDownLatch observerLocked = new CountDownLatch(1);
        final CountDownLatch shutdownStarted = new CountDownLatch(1);
        final AtomicReference<Throwable> rotationFailure = new AtomicReference<>();
        final Thread shutdownThread = new Thread(() -> {
            shutdownStarted.countDown();
            sessionService.onShutdown();
        }, "SessionServiceTest-shutdown");
        shutdownThread.setDaemon(true);
        // mirrors a gRPC thread inside GrpcUtil.safelyError, which holds the observer's monitor while the call's close
        // path refreshes the session token
        final Thread rotationThread = new Thread(() -> {
            synchronized (observer) {
                observerLocked.countDown();
                try {
                    shutdownStarted.await();
                    awaitBlockedOnMonitorHeldByCurrentThreadOrFinished(shutdownThread);
                    sessionService.refreshToken(session);
                } catch (final Throwable t) {
                    rotationFailure.set(t);
                }
            }
        }, "SessionServiceTest-rotation");
        rotationThread.setDaemon(true);

        rotationThread.start();
        observerLocked.await();
        shutdownThread.start();

        shutdownThread.join(TimeUnit.SECONDS.toMillis(10));
        rotationThread.join(TimeUnit.SECONDS.toMillis(10));
        if (shutdownThread.isAlive() || rotationThread.isAlive()) {
            final long[] deadlocked = ManagementFactory.getThreadMXBean().findDeadlockedThreads();
            throw new AssertionFailure("shutdown notification and token rotation did not both complete; deadlocked "
                    + "threads: " + (deadlocked == null ? "none detected" : Arrays.toString(deadlocked)));
        }
        Assert.eqNull(rotationFailure.get(), "rotationFailure.get()");
        Assert.eqTrue(observer.completed, "observer.completed");
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
    }

    private static void awaitBlockedOnMonitorHeldByCurrentThreadOrFinished(final Thread thread) {
        final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        final long currentThreadId = Thread.currentThread().getId();
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (thread.isAlive() && System.nanoTime() < deadlineNanos) {
            final ThreadInfo info = threads.getThreadInfo(thread.getId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockOwnerId() == currentThreadId) {
                return;
            }
            Thread.onSpinWait();
        }
    }

    /**
     * Every token ever issued maps to its session for as long as the SessionService lives. Once the session has expired
     * and all of its tokens are past their deadline, nothing in the service may keep the session reachable.
     */
    @Test
    public void testExpiredSessionIsNotRetainedAfterItsTokensExpire() throws InterruptedException {
        final WeakReference<SessionState> sessionRef = createRotateAndExpireSession();
        for (int i = 0; i < 100 && sessionRef.get() != null; ++i) {
            System.gc();
            Thread.sleep(10);
        }
        Assert.eqNull(sessionRef.get(), "sessionRef.get()");
    }

    @Test
    public void testClosedSessionIsNotRetained() throws InterruptedException {
        final WeakReference<SessionState> sessionRef = createRotateAndCloseSession();
        for (int i = 0; i < 100 && sessionRef.get() != null; ++i) {
            System.gc();
            Thread.sleep(10);
        }
        Assert.eqNull(sessionRef.get(), "sessionRef.get()");
    }

    private WeakReference<SessionState> createRotateAndCloseSession() {
        final SessionState session;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            session = sessionService.newSession(AUTH_CONTEXT);
        }
        scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));
        Assert.neqNull(sessionService.refreshToken(session), "sessionService.refreshToken(session)");
        // an explicit close must not wait for the tokens to age out
        sessionService.closeSession(session);
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        return new WeakReference<>(session);
    }

    private WeakReference<SessionState> createRotateAndExpireSession() {
        final SessionState session;
        // a throw-away scope, so that this test's liveness scope does not keep anything of the session alive
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            session = sessionService.newSession(AUTH_CONTEXT);
        }
        // rotate a few times so that several tokens map to this session
        for (int i = 0; i < 3; ++i) {
            scheduler.runUntil(scheduler.timeAfterMs(TOKEN_EXPIRE_MS / 3));
            Assert.neqNull(sessionService.refreshToken(session), "sessionService.refreshToken(session)");
        }
        // let the newest token expire; the cleanup job expires the session and forgets its tokens
        scheduler.runThrough(session.getExpiration().deadlineMillis);
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        return new WeakReference<>(session);
    }

    private static final class RecordingTerminationObserver
            extends ServerCallStreamObserver<TerminationNotificationResponse> {
        volatile boolean completed;

        @Override
        public void onNext(final TerminationNotificationResponse value) {}

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {
            completed = true;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(final Runnable onCancelHandler) {}

        @Override
        public void setCompression(final String compression) {}

        @Override
        public boolean isReady() {
            return true;
        }

        @Override
        public void setOnReadyHandler(final Runnable onReadyHandler) {}

        @Override
        public void disableAutoInboundFlowControl() {}

        @Override
        public void request(final int count) {}

        @Override
        public void setMessageCompression(final boolean enable) {}
    }
}
