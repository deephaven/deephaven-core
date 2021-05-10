package io.deephaven.grpc_api.table;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.SystemicObjectTracker;
import io.deephaven.db.util.liveness.LivenessScope;
import io.deephaven.db.util.liveness.LivenessScopeStack;
import io.deephaven.db.v2.ModifiedColumnSet;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ShiftAwareListener;
import io.deephaven.db.v2.TstUtils;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.util.SafeCloseable;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.util.auth.AuthContext;
import io.deephaven.grpc_api.session.SessionService;
import io.deephaven.grpc_api.session.SessionState;
import io.deephaven.grpc_api.util.TestControlledScheduler;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateBatchMessage;
import io.deephaven.proto.backplane.grpc.ExportedTableUpdateMessage;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.UUID;

import static io.deephaven.db.v2.TstUtils.addToTable;
import static io.deephaven.db.v2.TstUtils.i;

public class ExportTableUpdateListenerTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private static final LiveTableMonitor liveTableMonitor = LiveTableMonitor.DEFAULT;
    private TestControlledScheduler scheduler;
    private TestSessionState session;
    private QueuingResponseObserver observer;

    @Before
    public void setup() {
        LiveTableMonitor.DEFAULT.enableUnitTestMode();
        LiveTableMonitor.DEFAULT.resetForUnitTests(false);
        SystemicObjectTracker.markThreadSystemic();

        scheduler = new TestControlledScheduler();
        session = new TestSessionState();
        observer = new QueuingResponseObserver();
    }

    @After
    public void tearDown() {
        LiveTableMonitor.DEFAULT.resetForUnitTests(true);

        scheduler = null;
        session = null;
        observer = null;
    }

    @Test
    public void testLifeCycleStaticTable() {
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        expectBatch(0); // the refresh is empty

        // create and export the table
        final QueryTable src = TstUtils.testTable(Index.FACTORY.getFlatIndex(100));
        final SessionState.ExportObject<QueryTable> t1 = session.newServerSideExport(src);

        // validate we receive an initial table size update
        expectBatchWithSizes(t1.getExportId(), 100);

        // no update on release
        t1.release();
        expectNoBatch();
    }

    @Test
    public void testRefreshStaticTable() {
        // create and export the table
        final QueryTable src = TstUtils.testTable(Index.FACTORY.getFlatIndex(1024));
        final SessionState.ExportObject<QueryTable> t1 = session.newServerSideExport(src);

        // now add the listener
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update in the refresh
        expectBatchWithSizes(t1.getExportId(), 1024);

        // no update on release
        t1.release();
        expectNoBatch();
    }

    @Test
    public void testLifeCycleTickingTable() {
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        expectBatch(0); // the refresh is empty

        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(42));
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // validate we receive an initial table size update
        expectBatchWithSizes(t1.getExportId(), 42);

        // validate we're subscribed
        addRowsToSource(src, 42);
        expectBatchWithSizes(t1.getExportId(), 84);

        // no update on release
        t1.release();
        addRowsToSource(src, 2);
        expectNoBatch();
    }

    @Test
    public void testRefreshTickingTable() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(42));
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // now add the listener
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectBatchWithSizes(t1.getExportId(), 42);

        // validate we're subscribed
        addRowsToSource(src, 42);
        expectBatchWithSizes(t1.getExportId(), 84);

        // no update on release
        t1.release();
        addRowsToSource(src, 2);
        expectNoBatch();
    }

    @Test
    public void testSessionClose() {
        // recreate session so we can close it
        final LivenessScope sessionScope = new LivenessScope();
        LivenessScopeStack.push(sessionScope);
        session = new TestSessionState();
        LivenessScopeStack.pop(sessionScope);

        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(42));
        // create t1 in global query scope
        final SessionState.ExportObject<QueryTable> t1 = session.newServerSideExport(src);

        // now add the listener
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectBatchWithSizes(t1.getExportId(), 42);

        // validate we're subscribed
        addRowsToSource(src, 42);
        expectBatchWithSizes(t1.getExportId(), 84);

        // release session && validate export object is not dead
        sessionScope.release();
        Assert.eqFalse(session.tryRetainReference(), "session.tryRetainReference()");
        Assert.eqTrue(t1.tryRetainReference(), "t1.tryRetainReference()");
        t1.dropReference();

        // no update if table ticks
        addRowsToSource(src, 2);
        expectNoBatch();
    }

    @Test
    public void testPropagatesError() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(42));
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // now add the listener
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectBatchWithSizes(t1.getExportId(), 42);

        liveTableMonitor.runWithinUnitTestCycle(() -> {
            src.notifyListenersOnError(new RuntimeException("awful error occurred!"), null);
        });

        final ExportedTableUpdateMessage msg = expectBatch(1).getUpdates(0);
        final long updateId = SessionState.ticketToExportId(msg.getExportId());
        Assert.eq(updateId, "updateId", t1.getExportId(), "t1.getExportId()");
        Assert.eq(msg.getSize(), "msg.getSize()", 42);
        Assert.eqFalse(msg.getUpdateFailureMessage().isEmpty(), "msg.getUpdateFailureMessage().isEmpty()");

        // validate that our error is not directly embedded in the update (that would be a security concern)
        Assert.eqFalse(msg.getUpdateFailureMessage().contains("awful"), "msg.contains('awful')");
    }

    @Test
    public void testListenerClosed() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(42));
        final SessionState.ExportObject<QueryTable> t1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            t1 = session.newServerSideExport(src);
        }

        // now add the listener
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectBatchWithSizes(t1.getExportId(), 42);

        // verify count state before the closing
        Assert.eq(session.numExportListeners(), "session.numExportListeners()", 1);
        Assert.eq(observer.countPostComplete, "observer.countPostComplete", 0);
        Assert.eqTrue(src.hasListeners(), "src.hasListeners()");

        // close the observer and tickle close detection logic
        observer.onCompleted();
        addRowsToSource(src, 42);
        expectNoBatch();
        Assert.eq(session.numExportListeners(), "session.numExportListeners()", 0);
        Assert.eq(observer.countPostComplete, "observer.countPostComplete", 1);
        Assert.eqFalse(src.hasListeners(), "src.hasListeners()");

        // the actual ExportedTableUpdateListener should be "live", and should no longer be listening

        addRowsToSource(src, 2);
        expectNoBatch();
    }

    @Test
    public void testTableSizeUsesPrev() {
        // create and export the table
        final QueryTable src = TstUtils.testRefreshingTable(Index.FACTORY.getFlatIndex(42));
        final MutableObject<SessionState.ExportObject<QueryTable>> t1 = new MutableObject<>();

        // now add the listener
        final ExportedTableUpdateListener listener = new ExportedTableUpdateListener(liveTableMonitor, session, observer);
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }

        // validate we receive an initial table size update
        expectBatch(0);

        // export mid-tick
        liveTableMonitor.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByRange(src.getIndex().lastKey() + 1, src.getIndex().lastKey() + 42);
            update.removed = update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = IndexShiftData.EMPTY;
            addToTable(src, update.added);

            // Must be off-thread to use concurrent instantiation
            final Thread thread = new Thread(() -> {
                try (final SafeCloseable scope = LivenessScopeStack.open()) {
                    t1.setValue(session.newServerSideExport(src));
                }
            });
            thread.start();
            try {
                thread.join();
            } catch (final InterruptedException ie) {
                throw new UncheckedDeephavenException(ie);
            }

            src.notifyListeners(update);
        });

        // we should get both a refresh and the update in the same flush
        expectBatchWithSizes(t1.getValue().getExportId(), 42, 84);
    }

    private void addRowsToSource(final QueryTable src, final long nRows) {
        liveTableMonitor.runWithinUnitTestCycle(() -> {
            final ShiftAwareListener.Update update = new ShiftAwareListener.Update();
            update.added = Index.FACTORY.getIndexByRange(src.getIndex().lastKey() + 1, src.getIndex().lastKey() + nRows);
            update.removed = update.modified = i();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.shifted = IndexShiftData.EMPTY;
            addToTable(src, update.added);
            src.notifyListeners(update);
        });
    }

    private void expectBatchWithSizes(final long exportId, final long... sizes) {
        final ExportedTableUpdateBatchMessage batch = expectBatch(sizes.length);
        for (int i = 0; i < sizes.length; ++i) {
            final ExportedTableUpdateMessage msg = batch.getUpdates(i);
            final long updateId = SessionState.ticketToExportId(msg.getExportId());
            Assert.eq(updateId, "updateId", exportId, "exportId");
            Assert.eq(msg.getSize(), "msg.getSize()", sizes[i]);
            Assert.eqTrue(msg.getUpdateFailureMessage().isEmpty(), "msg.getUpdateFailureMessage().isEmpty()");
        }
    }

    private ExportedTableUpdateBatchMessage expectBatch(final long size) {
        liveTableMonitor.runWithinUnitTestCycle(() -> {}); // flush our terminal notification
        final ExportedTableUpdateBatchMessage batch = observer.msgQueue.poll();
        Assert.neqNull(batch, "batch");
        Assert.eq(batch.getUpdatesCount(), "batch.getUpdatesCount()", size);
        return batch;
    }

    private void expectNoBatch() {
        liveTableMonitor.runWithinUnitTestCycle(() -> {}); // flush our terminal notification
        final ExportedTableUpdateBatchMessage batch = observer.msgQueue.poll();
        Assert.eqNull(batch, "batch");
    }

    public class TestSessionState extends SessionState {
        public TestSessionState() {
            super(scheduler, liveTableMonitor, AUTH_CONTEXT);
            setExpiration(new SessionService.TokenExpiration(UUID.randomUUID(), DBTimeUtils.nanosToTime(Long.MAX_VALUE), this));
        }
    }

    public static class QueuingResponseObserver implements StreamObserver<ExportedTableUpdateBatchMessage> {
        boolean complete = false;
        long countPostComplete = 0;
        Queue<ExportedTableUpdateBatchMessage> msgQueue = new ArrayDeque<>();

        @Override
        public void onNext(final ExportedTableUpdateBatchMessage msg) {
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
