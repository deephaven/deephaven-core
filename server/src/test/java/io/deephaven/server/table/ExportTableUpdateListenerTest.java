//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.util.systemicmarking.SystemicObjectTracker;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.i;

public class ExportTableUpdateListenerTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    private ControlledUpdateGraph updateGraph;

    private TestControlledScheduler scheduler;
    private TestSessionState session;
    private QueuingResponseObserver observer;
    private SessionService.ErrorTransformer errorTransformer;

    @Before
    public void setup() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
        SystemicObjectTracker.markThreadSystemic();

        scheduler = new TestControlledScheduler();
        session = new TestSessionState();
        observer = new QueuingResponseObserver();

        errorTransformer = new SessionService.ObfuscatingErrorTransformer();
    }

    @After
    public void tearDown() {
        updateGraph.resetForUnitTests(true);

        scheduler = null;
        session = null;
        observer = null;

        executionContextCloseable.close();
    }

    private ExportedTableUpdateListener createListener(
            final SessionState session,
            final QueuingResponseObserver observer) {
        return new ExportedTableUpdateListener(session, observer, errorTransformer);
    }

    @Test
    public void testLifeCycleStaticTable() {
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        expectNoMessage(); // the run is empty

        // create and export the table
        final QueryTable src = TstUtils.testTable(RowSetFactory.flat(100).toTracking());
        final SessionState.ExportObject<QueryTable> t1 = session.newServerSideExport(src);

        // validate we receive an initial table size update
        expectSizes(t1.getExportId(), 100);

        // no update on release
        t1.release();
        expectNoMessage();
    }

    @Test
    public void testRefreshStaticTable() {
        // create and export the table
        final QueryTable src = TstUtils.testTable(RowSetFactory.flat(1024).toTracking());
        final SessionState.ExportObject<QueryTable> t1 = session.newServerSideExport(src);

        // now add the listener
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update in the run
        expectSizes(t1.getExportId(), 1024);

        // no update on release
        t1.release();
        expectNoMessage();
    }

    @Test
    public void testLifeCycleTickingTable() {
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        expectNoMessage(); // the run is empty

        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(RowSetFactory.flat(42).toTracking());
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // validate we receive an initial table size update
        expectSizes(t1.getExportId(), 42);

        // validate we're subscribed
        addRowsToSource(src, 42);
        expectSizes(t1.getExportId(), 84);

        // no update on release
        t1.release();
        addRowsToSource(src, 2);
        expectNoMessage();
    }

    @Test
    public void testRefreshTickingTable() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(RowSetFactory.flat(42).toTracking());
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // now add the listener
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectSizes(t1.getExportId(), 42);

        // validate we're subscribed
        addRowsToSource(src, 42);
        expectSizes(t1.getExportId(), 84);

        // no update on release
        t1.release();
        addRowsToSource(src, 2);
        expectNoMessage();
    }

    @Test
    public void testSessionClose() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(RowSetFactory.flat(42).toTracking());
        // create t1 in global query scope
        final SessionState.ExportObject<QueryTable> t1 = session.newServerSideExport(src);

        // now add the listener
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectSizes(t1.getExportId(), 42);

        // validate we're subscribed
        addRowsToSource(src, 42);
        expectSizes(t1.getExportId(), 84);

        // release session && validate export object is not dead
        session.onExpired();
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        Assert.eqTrue(t1.tryRetainReference(), "t1.tryRetainReference()");
        t1.dropReference();

        // no update if table ticks
        addRowsToSource(src, 2);
        expectNoMessage();
    }

    @Test
    public void testPropagatesError() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(RowSetFactory.flat(42).toTracking());
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // now add the listener
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectSizes(t1.getExportId(), 42);

        updateGraph.runWithinUnitTestCycle(() -> {
            src.notifyListenersOnError(new RuntimeException("awful error occurred!"), null);
        });

        final ExportedTableUpdateMessage msg = observer.msgQueue.poll();
        final Ticket updateId = msg.getExportId();
        Assert.equals(updateId, "updateId", t1.getExportId(), "t1.getExportId()");
        Assert.eq(msg.getSize(), "msg.getSize()", 42);
        Assert.eqFalse(msg.getUpdateFailureMessage().isEmpty(), "msg.getUpdateFailureMessage().isEmpty()");
        Assert.eqFalse(msg.getUpdateFailureMessage().contains("awful"), "msg.contains('awful')");
    }

    @Test
    public void testListenerClosed() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(RowSetFactory.flat(42).toTracking());
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // now add the listener
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectSizes(t1.getExportId(), 42);

        // verify count state before the closing
        Assert.eq(session.numExportListeners(), "session.numExportListeners()", 1);
        Assert.eq(observer.countPostComplete, "observer.countPostComplete", 0);
        Assert.eqTrue(src.hasListeners(), "src.hasListeners()");

        // close the observer and tickle close detection logic
        observer.onCompleted();
        addRowsToSource(src, 42);
        expectNoMessage();
        Assert.eq(observer.countPostComplete, "observer.countPostComplete", 1);
        Assert.eqFalse(src.hasListeners(), "src.hasListeners()");

        // the session removes the listener when it next notifies it
        Assert.eq(session.numExportListeners(), "session.numExportListeners()", 1);
        t1.release();
        Assert.eq(session.numExportListeners(), "session.numExportListeners()", 0);

        // the actual ExportedTableUpdateListener should be "live", and should no longer be listening

        addRowsToSource(src, 2);
        expectNoMessage();
    }

    @Test
    public void testTableSizeUsesPrev() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(RowSetFactory.flat(42).toTracking());
        final MutableObject<SessionState.ExportObject<QueryTable>> t1 = new MutableObject<>();

        // now add the listener
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectNoMessage();

        // export mid-tick
        // Must be off-thread to use concurrent instantiation
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added = RowSetFactory.fromRange(src.getRowSet().lastRowKey() + 1, src.getRowSet().lastRowKey() + 42);
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = RowSetShiftData.EMPTY;
            addToTable(src, update.added());

            // Must be off-thread to use concurrent instantiation
            final Thread thread = new Thread(() -> {
                try (final SafeCloseable ignored = LivenessScopeStack.open();
                        final SafeCloseable ignored2 = executionContext.open()) {
                    t1.setValue(session.newServerSideExport(src));
                }
            });
            thread.start();
            try {
                thread.join();
            } catch (final InterruptedException ie) {
                throw new UncheckedDeephavenException(ie);
            }

            updateGraph.markSourcesRefreshedForUnitTests();
            src.notifyListeners(update);
        }, false);

        // we should get both a run and the update in the same flush
        expectSizes(t1.getValue().getExportId(), 42, 84);
    }

    @Test
    public void testNewExportWaitingForUpdateGraphLockDoesNotBlockUpdates() throws Exception {
        // create and export the ticking table
        final QueryTable src = TstUtils.testRefreshingTable(RowSetFactory.flat(42).toTracking());
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // now add the listener
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        expectSizes(t1.getExportId(), 42);

        // a table whose snapshots must wait for the update graph lock
        final QueryTable lockedSnapshotSrc = new QueryTable(RowSetFactory.flat(10).toTracking(), Map.of()) {
            @Override
            public TrackingRowSet getRowSet() {
                if (ConstructSnapshot.getConcurrentAttemptClockValue() != 0) {
                    throw new ConstructSnapshot.NoSnapshotAllowedException();
                }
                return super.getRowSet();
            }
        };
        lockedSnapshotSrc.setRefreshing(true);

        final CompletableFuture<SessionState.ExportObject<QueryTable>> t2 = new CompletableFuture<>();
        final Thread exportThread = new Thread(() -> {
            try (final SafeCloseable ignored = LivenessScopeStack.open();
                    final SafeCloseable ignored2 = executionContext.open()) {
                t2.complete(session.newServerSideExport(lockedSnapshotSrc));
            } catch (final Throwable failure) {
                t2.completeExceptionally(failure);
            }
        }, "locked-snapshot-exporter");
        exportThread.setDaemon(true);

        // the update graph delivers src's update while the export's snapshot waits for the update graph lock
        final CompletableFuture<Void> cycle = new CompletableFuture<>();
        final Thread cycleThread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                updateGraph.startCycleForUnitTests(false);
                try {
                    exportThread.start();
                    awaitWaitingForUpdateGraphLock(exportThread, t2);
                    final RowSet added = RowSetFactory.fromRange(42, 83);
                    addToTable(src, added);
                    src.notifyListeners(added, i(), i());
                } finally {
                    updateGraph.markSourcesRefreshedForUnitTests();
                    updateGraph.completeCycleForUnitTests();
                }
                cycle.complete(null);
            } catch (final Throwable failure) {
                cycle.completeExceptionally(failure);
            }
        }, "update-cycle-driver");
        cycleThread.setDaemon(true);
        cycleThread.start();

        cycle.get(30, TimeUnit.SECONDS);
        final SessionState.ExportObject<QueryTable> lockedSnapshotExport = t2.get(30, TimeUnit.SECONDS);
        expectSizes(t1.getExportId(), 84);
        expectSizes(lockedSnapshotExport.getExportId(), 10);

        // validate we're subscribed
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(10, 14);
            lockedSnapshotSrc.getRowSet().writableCast().insert(added);
            lockedSnapshotSrc.notifyListeners(added, i(), i());
        });
        expectSizes(lockedSnapshotExport.getExportId(), 15);
    }

    @Test
    public void testInitialSizeOvertakenByUpdateIsNotSent() throws Exception {
        final ExportedTableUpdateListener listener = createListener(session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        expectNoMessage(); // the run is empty

        // a table that pauses its exporter after subscribing the export's listener
        final CountDownLatch subscribed = new CountDownLatch(1);
        final CountDownLatch resume = new CountDownLatch(1);
        final QueryTable src = new QueryTable(RowSetFactory.flat(10).toTracking(), Map.of()) {
            @Override
            public boolean addUpdateListener(
                    @NotNull final TableUpdateListener updateListener, final long requiredLastNotificationStep) {
                final boolean added = super.addUpdateListener(updateListener, requiredLastNotificationStep);
                if (added) {
                    subscribed.countDown();
                    try {
                        Assert.eqTrue(resume.await(30, TimeUnit.SECONDS), "resume.await(30, TimeUnit.SECONDS)");
                    } catch (final InterruptedException ie) {
                        throw new UncheckedDeephavenException(ie);
                    }
                }
                return added;
            }
        };
        src.setRefreshing(true);

        final CompletableFuture<SessionState.ExportObject<QueryTable>> t1 = new CompletableFuture<>();
        final Thread exportThread = new Thread(() -> {
            try (final SafeCloseable ignored = LivenessScopeStack.open();
                    final SafeCloseable ignored2 = executionContext.open()) {
                t1.complete(session.newServerSideExport(src));
            } catch (final Throwable failure) {
                t1.completeExceptionally(failure);
            }
        }, "paused-exporter");
        exportThread.setDaemon(true);
        exportThread.start();
        Assert.eqTrue(subscribed.await(30, TimeUnit.SECONDS), "subscribed.await(30, TimeUnit.SECONDS)");

        // the subscribed listener sends the updated size before the exporter sends the initial size
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet added = RowSetFactory.fromRange(10, 14);
            src.getRowSet().writableCast().insert(added);
            src.notifyListeners(added, i(), i());
        });
        resume.countDown();

        expectSizes(t1.get(30, TimeUnit.SECONDS).getExportId(), 15);
        expectNoMessage();
    }

    /**
     * Wait until {@code thread} is parked waiting for the update graph lock. If {@code thread} completes first, its
     * result or failure is reported instead.
     */
    private static void awaitWaitingForUpdateGraphLock(
            final Thread thread,
            final CompletableFuture<?> result) throws Exception {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            if (result.isDone()) {
                throw new AssertionError(thread.getName() + " completed without waiting for the update graph lock: "
                        + result.get());
            }
            final ThreadInfo threadInfo = threadMXBean.getThreadInfo(thread.getId());
            if (threadInfo != null && threadInfo.getLockInfo() != null
                    && threadInfo.getLockInfo().getClassName().startsWith(ReentrantReadWriteLock.class.getName())) {
                return;
            }
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError(thread.getName() + " is not waiting for the update graph lock: " + threadInfo);
            }
            Thread.sleep(10);
        }
    }

    private void addRowsToSource(final QueryTable src, final long nRows) {
        updateGraph.runWithinUnitTestCycle(() -> {
            final TableUpdateImpl update = new TableUpdateImpl();
            update.added =
                    RowSetFactory.fromRange(src.getRowSet().lastRowKey() + 1, src.getRowSet().lastRowKey() + nRows);
            update.removed = i();
            update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = RowSetShiftData.EMPTY;
            addToTable(src, update.added());
            src.notifyListeners(update);
        });
    }

    private void expectSizes(final Ticket exportId, final long... sizes) {
        for (long size : sizes) {
            final ExportedTableUpdateMessage msg = observer.msgQueue.poll();
            final Ticket updateId = msg.getExportId();
            Assert.equals(updateId, "updateId", exportId, "exportId");
            Assert.eq(msg.getSize(), "msg.getSize()", size);
            Assert.eqTrue(msg.getUpdateFailureMessage().isEmpty(), "msg.getUpdateFailureMessage().isEmpty()");
        }
    }

    private void expectNoMessage() {
        // flush our terminal notification
        updateGraph.runWithinUnitTestCycle(() -> {
        });
        final ExportedTableUpdateMessage batch = observer.msgQueue.poll();
        Assert.eqNull(batch, "batch");
    }

    public class TestSessionState extends SessionState {
        public TestSessionState() {
            super(scheduler, new SessionService.ObfuscatingErrorTransformer(), TestExecutionContext::createForUnitTests,
                    AUTH_CONTEXT);
            initializeExpiration(new SessionService.TokenExpiration(UUID.randomUUID(),
                    DateTimeUtils.epochMillis(DateTimeUtils.epochNanosToInstant(Long.MAX_VALUE)), this));
        }
    }

    public static class QueuingResponseObserver implements StreamObserver<ExportedTableUpdateMessage> {
        boolean complete = false;
        long countPostComplete = 0;
        Queue<ExportedTableUpdateMessage> msgQueue = new ArrayDeque<>();

        @Override
        public void onNext(final ExportedTableUpdateMessage msg) {
            if (complete) {
                countPostComplete++;
                throw new UncheckedDeephavenException("already closed");
            }
            msgQueue.add(msg);
        }

        @Override
        public void onError(final Throwable t) {
            throw new UnsupportedOperationException("we have no reason to error the observer");
        }

        @Override
        public void onCompleted() {
            complete = true;
        }
    }
}
