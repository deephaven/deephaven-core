package io.deephaven.grpc_api.session;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.grpc_api.util.TestControlledScheduler;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.auth.AuthContext;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.deephaven.proto.backplane.grpc.ExportNotification.State.CANCELLED;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.DEPENDENCY_FAILED;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.EXPORTED;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.FAILED;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.PENDING;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.QUEUED;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.RELEASED;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.UNKNOWN;

public class SessionStateTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private LivenessScope livenessScope;
    private TestControlledScheduler scheduler;
    private SessionState session;
    private long nextExportId;

    @Before
    public void setup() {
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        scheduler = new TestControlledScheduler();
        session = new SessionState(scheduler, LiveTableMonitor.DEFAULT, AUTH_CONTEXT);
        session.setExpiration(new SessionService.TokenExpiration(UUID.randomUUID(), DBTimeUtils.nanosToTime(Long.MAX_VALUE), session));
        nextExportId = 1;
    }

    @After
    public void teardown() {
        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
        scheduler = null;
        session = null;
    }

    @Test
    public void testExportListenerOnCompleteOnRemoval() {
        final QueueingExportListener listener = new QueueingExportListener();
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        Assert.eqFalse(listener.isComplete, "listener.isComplete");
        session.removeExportListener(listener);
        Assert.eqTrue(listener.isComplete, "listener.isComplete");
    }

    @Test
    public void testExportListenerOnCompleteOnSessionExpire() {
        final QueueingExportListener listener = new QueueingExportListener();
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        Assert.eqFalse(listener.isComplete, "listener.isComplete");
        session.onExpired();
        Assert.eqTrue(listener.isComplete, "listener.isComplete");
    }

    @Test
    public void testExportListenerOnCompleteOnSessionRelease() {
        // recreate session in controllable scope
        final LivenessScope sessionScope = new LivenessScope();
        LivenessScopeStack.push(sessionScope);
        session = new SessionState(scheduler, LiveTableMonitor.DEFAULT, AUTH_CONTEXT);
        session.setExpiration(new SessionService.TokenExpiration(UUID.randomUUID(), DBTimeUtils.nanosToTime(Long.MAX_VALUE), session));
        LivenessScopeStack.pop(sessionScope);

        final QueueingExportListener listener = new QueueingExportListener();
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        Assert.eqFalse(listener.isComplete, "listener.isComplete");
        sessionScope.release();
        Assert.eqFalse(session.tryRetainReference(), "session.tryRetainReference");
        Assert.eqTrue(listener.isComplete, "listener.isComplete");
    }

    @Test
    public void textExportListenerNoExports() {
        final QueueingExportListener listener = new QueueingExportListener();
        session.addExportListener(listener);
        Assert.eq(listener.notifications.size(), "notifications.size()", 1);
        final ExportNotification refreshComplete = listener.notifications.get(listener.notifications.size() - 1);
        Assert.eq(SessionState.ticketToExportId(refreshComplete.getTicket()), "refreshComplete.getTicket()", SessionState.NON_EXPORT_ID, "SessionState.NON_EXPORT_ID");
    }

    @Test
    public void textExportListenerOneExport() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportObject<SessionState> e1 = session.<SessionState>newExport(nextExportId++).submit(() -> session);
        scheduler.runUntilQueueEmpty();
        session.addExportListener(listener);
        listener.validateNotificationQueue(e1, EXPORTED);

        // ensure export was from refresh
        Assert.eq(listener.notifications.size(), "notifications.size()", 2);
        final ExportNotification refreshComplete = listener.notifications.get(1);
        Assert.eq(SessionState.ticketToExportId(refreshComplete.getTicket()), "lastNotification.getTicket()", SessionState.NON_EXPORT_ID, "SessionState.NON_EXPORT_ID");
    }

    @Test
    public void textExportListenerAddHeadDuringRefreshComplete() {
        final MutableObject<SessionState.ExportObject<SessionState>> e1 = new MutableObject<>();
        final QueueingExportListener listener = new QueueingExportListener() {
            @Override
            public void onNext(final ExportNotification n) {
                if (SessionState.ticketToExportId(n.getTicket()) != SessionState.NON_EXPORT_ID) {
                    notifications.add(n);
                    return;
                }
                e1.setValue(session.<SessionState>newExport(nextExportId++).submit(() -> session));
            }
        };
        session.addExportListener(listener);
        Assert.eq(listener.notifications.size(), "notifications.size()", 3);
        listener.validateNotificationQueue(e1.getValue(), UNKNOWN, PENDING, QUEUED);
    }

    @Test
    public void textExportListenerAddHeadAfterRefreshComplete() {
        final QueueingExportListener listener = new QueueingExportListener();
        session.addExportListener(listener);
        final SessionState.ExportObject<SessionState> e1 = session.<SessionState>newExport(nextExportId++).submit(() -> session);
        scheduler.runUntilQueueEmpty();
        Assert.eq(listener.notifications.size(), "notifications.size()", 5);
        listener.validateIsRefreshComplete(0);
        listener.validateNotificationQueue(e1, UNKNOWN, PENDING, QUEUED, EXPORTED);
    }

    @Test
    public void testExportListenerInterestingRefresh() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportObject<SessionState> e1 = session.<SessionState>getExport(nextExportId++);
        final SessionState.ExportObject<SessionState> e4 = session.<SessionState>newExport(nextExportId++).submit(() -> session); // exported
        final SessionState.ExportObject<SessionState> e5 = session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> e7 = session.<SessionState>newExport(nextExportId++).submit(() -> { throw new RuntimeException(); }); // failed
        final SessionState.ExportObject<SessionState> e8 = session.<SessionState>newExport(nextExportId++).require(e7).submit(() -> session); // dependency failed
        scheduler.runUntilQueueEmpty();
        e5.release(); // released

        final SessionState.ExportObject<SessionState> e6 = session.<SessionState>newExport(nextExportId++).submit(() -> session);
        e6.cancel();

        final SessionState.ExportObject<SessionState> e3 = session.<SessionState>newExport(nextExportId++).submit(() -> session); // queued
        final SessionState.ExportObject<SessionState> e2 = session.<SessionState>newExport(nextExportId++).require(e3).submit(() -> session); // pending

        session.addExportListener(listener);
        listener.validateIsRefreshComplete(-1);
        listener.validateNotificationQueue(e1, UNKNOWN);
        listener.validateNotificationQueue(e2, PENDING);
        listener.validateNotificationQueue(e3, QUEUED);
        listener.validateNotificationQueue(e4, EXPORTED);
        listener.validateNotificationQueue(e5); // Released
        listener.validateNotificationQueue(e6); // Cancelled
        listener.validateNotificationQueue(e7); // Failed
        listener.validateNotificationQueue(e8); // Dependency Failed
    }

    @Test
    public void testExportListenerInterestingUpdates() {
        final QueueingExportListener listener = new QueueingExportListener();
        session.addExportListener(listener);

        final SessionState.ExportObject<SessionState> e1 = session.<SessionState>getExport(nextExportId++);
        final SessionState.ExportObject<SessionState> e4 = session.<SessionState>newExport(nextExportId++).submit(() -> session); // exported
        final SessionState.ExportObject<SessionState> e5 = session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> e7 = session.<SessionState>newExport(nextExportId++).submit(() -> {
            throw new RuntimeException();
        }); // failed
        final SessionState.ExportObject<SessionState> e8 = session.<SessionState>newExport(nextExportId++).require(e7).submit(() -> session); // dependency failed
        scheduler.runUntilQueueEmpty();
        e5.release(); // released

        final SessionState.ExportObject<SessionState> e6 = session.<SessionState>newExport(nextExportId++).getExport();
        e6.cancel();

        final SessionState.ExportObject<SessionState> e3 = session.<SessionState>newExport(nextExportId++).submit(() -> session); // queued
        final SessionState.ExportObject<SessionState> e2 = session.<SessionState>newExport(nextExportId++).require(e3).submit(() -> session); // pending

        listener.validateIsRefreshComplete(0);
        listener.validateNotificationQueue(e1, UNKNOWN);
        listener.validateNotificationQueue(e2, UNKNOWN, PENDING);
        listener.validateNotificationQueue(e3, UNKNOWN, PENDING, QUEUED);
        listener.validateNotificationQueue(e4, UNKNOWN, PENDING, QUEUED, EXPORTED);
        listener.validateNotificationQueue(e5, UNKNOWN, PENDING, QUEUED, EXPORTED, RELEASED);
        listener.validateNotificationQueue(e6, UNKNOWN, CANCELLED);
        listener.validateNotificationQueue(e7, UNKNOWN, PENDING, QUEUED, FAILED);
        listener.validateNotificationQueue(e8, UNKNOWN, PENDING, DEPENDENCY_FAILED);
    }

    @Test
    public void testExportListenerUpdateBeforeSeqSent() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == b1.getExportId()) {
                    refreshing = false;
                    b2.submit(() -> session); // pending && queued
                }
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(-1);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, QUEUED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerUpdateDuringSeqSent() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == b2.getExportId()) {
                    refreshing = false;
                    b2.submit(() -> session); // pending && queued
                }
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(-1);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN, QUEUED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerUpdateAfterSeqSent() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == b3.getExportId()) {
                    refreshing = false;
                    b2.submit(() -> session); // pending && queued
                }
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(3);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN, PENDING, QUEUED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerUpdatePostRefresh() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        session.addExportListener(listener);
        b2.submit(() -> session); // pending && queued
        listener.validateIsRefreshComplete(3);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN, PENDING, QUEUED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerTerminalBeforeSeqSent() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                if (refreshing && getExportId(n) == b1.getExportId()) {
                    refreshing = false;
                    b2.getExport().cancel();
                }
                super.onNext(n);
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(-1);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerTerminalDuringSeqSent() {
        final ArrayList<ExportNotification> notifications = new ArrayList<>();
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == b2.getExportId()) {
                    refreshing = false;
                    b2.getExport().cancel();
                }
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(-1);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN, CANCELLED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerTerminalAfterSeqSent() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == b3.getExportId()) {
                    refreshing = false;
                    b2.getExport().cancel();
                }
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(3);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN, CANCELLED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerTerminalDuringRefreshComplete() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == SessionState.NON_EXPORT_ID) {
                    refreshing = false;
                    b2.getExport().cancel();
                }
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(3);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN, CANCELLED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerTerminalAfterRefreshComplete() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);

        session.addExportListener(listener);
        b2.getExport().cancel();

        listener.validateIsRefreshComplete(3);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN, CANCELLED);
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerNewExportAtRefreshTail() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);
        final MutableObject<SessionState.ExportBuilder<SessionState>> b4 = new MutableObject<>();

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == b3.getExportId()) {
                    refreshing = false;
                    LivenessScopeStack.push(livenessScope);
                    b4.setValue(session.newExport(nextExportId++));
                    LivenessScopeStack.pop(livenessScope);
                }
            }
        };
        session.addExportListener(listener);
        listener.validateIsRefreshComplete(4); // new export occurs prior to refresh completing
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN);
        listener.validateNotificationQueue(b3, UNKNOWN);
        listener.validateNotificationQueue(b4.getValue(), UNKNOWN);
    }

    @Test
    public void testExportListenerNewExportDuringRefreshComplete() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);
        final MutableObject<SessionState.ExportBuilder<SessionState>> b4 = new MutableObject<>();

        final QueueingExportListener listener = new QueueingExportListener() {
            boolean refreshing = true;

            @Override
            public void onNext(final ExportNotification n) {
                super.onNext(n);
                if (refreshing && getExportId(n) == SessionState.NON_EXPORT_ID) {
                    refreshing = false;
                    LivenessScopeStack.push(livenessScope);
                    b4.setValue(session.newExport(nextExportId++));
                    LivenessScopeStack.pop(livenessScope);
                }
            }
        };

        session.addExportListener(listener);
        listener.validateIsRefreshComplete(3); // refresh completes, then we see new export
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN);
        listener.validateNotificationQueue(b3, UNKNOWN);
        listener.validateNotificationQueue(b4.getValue(), UNKNOWN);
    }

    @Test
    public void testExportListenerNewExportAfterRefreshComplete() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportObject<SessionState> b1 = session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> b2 = session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> b3 = session.<SessionState>newExport(nextExportId++).submit(() -> session);

        session.addExportListener(listener);
        final SessionState.ExportObject<SessionState> b4 = session.<SessionState>newExport(nextExportId++).submit(() -> session);

        // for fun we'll flush after refresh
        scheduler.runUntilQueueEmpty();

        listener.validateIsRefreshComplete(3);
        listener.validateNotificationQueue(b1, QUEUED, EXPORTED);
        listener.validateNotificationQueue(b2, QUEUED, EXPORTED);
        listener.validateNotificationQueue(b3, QUEUED, EXPORTED);
        listener.validateNotificationQueue(b4, UNKNOWN, PENDING, QUEUED, EXPORTED);
    }

    @Test
    public void testExportListenerServerSideExports() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportObject<SessionState> e1 = session.newServerSideExport(session);
        session.addExportListener(listener);
        final SessionState.ExportObject<SessionState> e2 = session.newServerSideExport(session);

        listener.validateIsRefreshComplete(1);
        listener.validateNotificationQueue(e1, EXPORTED);
        listener.validateNotificationQueue(e2, UNKNOWN, EXPORTED);
    }

    private static long getExportId(final ExportNotification notification) {
        return SessionState.ticketToExportId(notification.getTicket());
    }

    private static class QueueingExportListener implements StreamObserver<ExportNotification> {
        boolean isComplete = false;
        final ArrayList<ExportNotification> notifications = new ArrayList<>();

        @Override
        public void onNext(final ExportNotification value) {
            if (isComplete) {
                throw new IllegalStateException("illegal to invoke onNext after onComplete");
            }
            notifications.add(value);
        }

        @Override
        public void onError(final Throwable t) {
            isComplete = true;
        }

        @Override
        public void onCompleted() {
            isComplete = true;
        }

        private void validateIsRefreshComplete(int offset) {
            if (offset < 0) {
                offset += notifications.size();
            }
            final ExportNotification notification = notifications.get(offset);
            Assert.eq(getExportId(notification), "getExportId(notification)", SessionState.NON_EXPORT_ID, "SessionState.NON_EXPORT_ID");
        }

        private void validateNotificationQueue(final SessionState.ExportBuilder<?> export, final ExportNotification.State... states) {
            validateNotificationQueue(export.getExport(), states);
        }

        private void validateNotificationQueue(final SessionState.ExportObject<?> export, final ExportNotification.State... states) {
            final long exportId = export.getExportId();

            final List<ExportNotification.State> foundStates = notifications.stream()
                    .filter(n -> getExportId(n) == exportId)
                    .map(ExportNotification::getExportState)
                    .collect(Collectors.toList());
            boolean error = foundStates.size() != states.length;
            for (int offset = 0; !error && offset < states.length; ++offset) {
                error = !foundStates.get(offset).equals(states[offset]);
            }
            if (error) {
                final String found = foundStates.stream().map(ExportNotification.State::toString).collect(Collectors.joining(", "));
                final String expected = Arrays.stream(states).map(ExportNotification.State::toString).collect(Collectors.joining(", "));
                throw new AssertionFailure("Notification Queue Differs. Expected: " + expected + " Found: " + found);
            }
        }
    }
}
