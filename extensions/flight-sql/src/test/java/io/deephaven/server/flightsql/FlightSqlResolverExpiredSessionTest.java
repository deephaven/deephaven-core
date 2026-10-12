//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.flightsql;

import com.google.protobuf.Any;
import io.deephaven.auth.AuthContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.test.TestAuthorizationProvider;
import io.deephaven.server.util.TestControlledScheduler;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.sql.impl.FlightSql.ActionCreatePreparedStatementRequest;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FlightSqlResolverExpiredSessionTest {

    /**
     * A session can expire between the handler's session check and the prepared statement registering its close
     * callback; the registration then throws, and nothing the statement published before it may be left behind.
     */
    @Test
    void createPreparedStatementOnExpiredSessionLeavesNothingBehind() {
        final TestControlledScheduler scheduler = new TestControlledScheduler();
        final FlightSqlResolver resolver = new FlightSqlResolver(new TestAuthorizationProvider(), scheduler);
        // a session whose expiration has been cleared, as onExpired() leaves it
        final SessionState session = new SessionState(scheduler, new SessionService.ObfuscatingErrorTransformer(),
                TestExecutionContext::createForUnitTests, new AuthContext.SuperUser());
        assertThat(session.isExpired()).isTrue();

        final Action action = new Action(FlightSqlActionHelper.CREATE_PREPARED_STATEMENT_ACTION_TYPE,
                Any.pack(ActionCreatePreparedStatementRequest.newBuilder().setQuery("SELECT 1").build())
                        .toByteArray());
        assertThatThrownBy(() -> resolver.doAction(session, action, new NoopObserver()))
                .isInstanceOf(StatusRuntimeException.class)
                .satisfies(e -> assertThat(((StatusRuntimeException) e).getStatus().getCode())
                        .isEqualTo(Code.UNAUTHENTICATED));
        assertThat(resolver.numPreparedStatements()).isZero();
    }

    /**
     * A session can also expire right after the prepared statement registers its close callback but before it is
     * published. The callback's cleanup must then wait for the publication rather than run against nothing.
     */
    @Test
    void expiryBetweenRegisteringAndPublishingStillCleansUp() throws InterruptedException {
        final TestControlledScheduler scheduler = new TestControlledScheduler();
        final FlightSqlResolver resolver = new FlightSqlResolver(new TestAuthorizationProvider(), scheduler);
        final ExpiringOnRegisterSession session = new ExpiringOnRegisterSession(scheduler);

        final Action action = new Action(FlightSqlActionHelper.CREATE_PREPARED_STATEMENT_ACTION_TYPE,
                Any.pack(ActionCreatePreparedStatementRequest.newBuilder().setQuery("SELECT 1").build())
                        .toByteArray());
        resolver.doAction(session, action, new NoopObserver());

        session.expirer.join(5_000);
        assertThat(session.expirer.isAlive()).isFalse();
        assertThat(session.isExpired()).isTrue();
        assertThat(resolver.numPreparedStatements()).isZero();
    }

    /**
     * A live session that expires on another thread as soon as a close callback is registered, and returns to the
     * registering caller only once that expiry has finished or is blocked on a monitor the caller holds.
     */
    private static final class ExpiringOnRegisterSession extends SessionState {
        Thread expirer;

        ExpiringOnRegisterSession(final TestControlledScheduler scheduler) {
            super(scheduler, new SessionService.ObfuscatingErrorTransformer(),
                    TestExecutionContext::createForUnitTests, new AuthContext.SuperUser());
            initializeExpiration(new SessionService.TokenExpiration(UUID.randomUUID(), Long.MAX_VALUE, this));
        }

        @Override
        public void addOnCloseCallback(final Closeable onClose) {
            super.addOnCloseCallback(onClose);
            expirer = new Thread(this::onExpired, "FlightSqlResolverExpiredSessionTest-expirer");
            expirer.start();
            final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
            final long currentThreadId = Thread.currentThread().getId();
            final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (expirer.isAlive()) {
                final ThreadInfo info = threads.getThreadInfo(expirer.getId());
                if (info != null && info.getThreadState() == Thread.State.BLOCKED
                        && info.getLockOwnerId() == currentThreadId) {
                    return;
                }
                if (System.nanoTime() > deadlineNanos) {
                    throw new IllegalStateException("expiry neither finished nor blocked on the registering thread");
                }
                LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(100));
            }
        }
    }

    private static final class NoopObserver implements StreamObserver<Result> {
        @Override
        public void onNext(final Result value) {}

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {}
    }
}
