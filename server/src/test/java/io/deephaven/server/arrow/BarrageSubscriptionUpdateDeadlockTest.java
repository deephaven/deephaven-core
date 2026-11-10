//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.google.protobuf.ByteString;
import io.deephaven.auth.AuthContext;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.util.TableTools;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.auth.AllowAllAuthorizationProvider;
import io.deephaven.server.session.ExportTicketResolver;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Lock-order tests for a {@link BarrageSubscriptionRequestHandler} whose subscription is changed while the
 * {@link io.deephaven.server.barrage.BarrageMessageProducer} is writing to the same stream.
 */
public class BarrageSubscriptionUpdateDeadlockTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();
    private static final int EXPORT_ID = 1;
    private static final long JOIN_TIMEOUT_MILLIS = 10_000;

    private ExecutionContext executionContext;
    private SafeCloseable executionContextCloseable;
    /** runs the session's exports, including the handler's export-resolution continuation */
    private TestControlledScheduler sessionScheduler;
    /** runs the producer's propagation job */
    private TestControlledScheduler producerScheduler;
    private SessionState session;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        sessionScheduler = new TestControlledScheduler();
        producerScheduler = new SynchronizedTestControlledScheduler();
        session = new SessionState(sessionScheduler, new SessionService.ObfuscatingErrorTransformer(),
                TestExecutionContext::createForUnitTests, AUTH_CONTEXT) {
            {
                initializeExpiration(new SessionService.TokenExpiration(UUID.randomUUID(),
                        DateTimeUtils.epochMillis(DateTimeUtils.epochNanosToInstant(Long.MAX_VALUE)), this));
            }
        };
    }

    @After
    public void tearDown() {
        session = null;
        sessionScheduler = null;
        producerScheduler = null;
        executionContextCloseable.close();
        executionContextCloseable = null;
        executionContext = null;
    }

    /**
     * An invalid subscription change (full to viewport) races a write that fails, on a transport that runs the call's
     * cancel handler inline on the writing thread (as the Jetty servlet transport does).
     *
     * <ul>
     * <li>W, the producer's propagation job, writes the initial snapshot and holds the response stream's monitor; its
     * write fails, and the cancel handler closes the marshaller, which closes the request handler.</li>
     * <li>B, delivering the client's next request, holds the request handler's monitor and then the producer's monitor
     * in {@code updateSubscription}, and reports the invalid change to the response stream.</li>
     * </ul>
     */
    @Test
    public void testInvalidSubscriptionChangeDoesNotDeadlockWithCancelFromFailedWrite() throws Exception {
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = openFullSubscription(call);

        final CountDownLatch writerHoldsResponseStream = new CountDownLatch(1);
        final AtomicReference<Thread> updaterThread = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        call.onWrite = () -> {
            call.onWrite = null;
            writerHoldsResponseStream.countDown();
            // wait for the update to block on this call's monitor, which this thread holds while writing, or to finish
            awaitBlockedOn(updaterThread, call);
            // the write fails; the servlet transport cancels the call on this thread
            call.onCancelHandler.run();
        };

        final Thread writer = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                producerScheduler.runUntilQueueEmpty();
            } catch (final Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "BarrageSubscriptionUpdateDeadlockTest-writer");
        writer.setDaemon(true);

        final Thread updater = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                assertTrue(writerHoldsResponseStream.await(JOIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS));
                // a full subscription cannot change to a viewport, so the producer rejects this request
                marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 9)));
            } catch (final Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "BarrageSubscriptionUpdateDeadlockTest-updater");
        updater.setDaemon(true);
        updaterThread.set(updater);

        // the updater starts first, so the writer cannot mistake it for having finished
        updater.start();
        writer.start();

        final long deadline = System.currentTimeMillis() + JOIN_TIMEOUT_MILLIS;
        writer.join(Math.max(1, deadline - System.currentTimeMillis()));
        updater.join(Math.max(1, deadline - System.currentTimeMillis()));
        if (writer.isAlive() || updater.isAlive()) {
            throw new AssertionError("subscription update and inline cancel did not finish; deadlocked:\n"
                    + describeDeadlockedThreads());
        }
        if (failure.get() != null) {
            throw new AssertionError("worker thread failed", failure.get());
        }
        assertTrue("the marshaller should be closed", marshaller.isClosed());
    }

    /**
     * The producer reports an invalid subscription change to the client as {@code INVALID_ARGUMENT}, and ends the
     * stream with that error rather than a completion.
     */
    @Test
    public void testInvalidSubscriptionChangeIsReportedToClient() {
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = openFullSubscription(call);
        producerScheduler.runUntilQueueEmpty();
        assertNull(call.error);

        // a full subscription cannot change to a viewport
        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 9)));
        producerScheduler.runUntilQueueEmpty();

        assertNotNull("the client should be told why its request was rejected", call.error);
        final Status status = Status.fromThrowable(call.error);
        assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertEquals("cannot change from full subscription to viewport or vice versa", status.getDescription());
        assertEquals("the stream ends with the error, not a completion", 0, call.completions);
    }

    /**
     * Exports a table and opens a DoExchange full subscription to it on {@code call}. The producer's propagation job is
     * scheduled but has not run.
     */
    private ArrowFlightUtil.DoExchangeMarshaller openFullSubscription(final InlineCancelCallObserver call) {
        final Table table = TableTools.emptyTable(100).update("I = i");
        session.newExport(EXPORT_ID).submit(() -> table);
        sessionScheduler.runUntilQueueEmpty();

        final SessionService.ErrorTransformer errorTransformer = new SessionService.ObfuscatingErrorTransformer();
        final BarrageMessageWriterImpl.Factory streamGeneratorFactory = new BarrageMessageWriterImpl.Factory();
        final AllowAllAuthorizationProvider authProvider = new AllowAllAuthorizationProvider();
        final TicketRouter ticketRouter =
                new TicketRouter(authProvider, Set.of(new ExportTicketResolver(authProvider)));
        final ExchangeMarshaller tableMarshaller = new TableExchangeMarshaller.Factory().create(
                producerScheduler, errorTransformer, streamGeneratorFactory, ImmediateJobScheduler::new);

        final ArrowFlightUtil.DoExchangeMarshaller marshaller = new ArrowFlightUtil.DoExchangeMarshaller(
                ticketRouter,
                streamGeneratorFactory,
                List.of(tableMarshaller),
                Set.of(new BarrageSubscriptionRequestHandler.BarrageSubscriptionRequestHandlerFactory()),
                errorTransformer,
                session,
                call);

        // the export is already resolved, so the continuation subscribes when the session runs
        marshaller.onNext(subscriptionRequest(null));
        sessionScheduler.runUntilQueueEmpty();
        return marshaller;
    }

    private static InputStream subscriptionRequest(@Nullable final RowSet viewport) {
        final ByteBuffer viewportBuffer = viewport == null ? null : BarrageProtoUtil.toByteBuffer(viewport);
        final byte[] metadata = BarrageUtil.createSubscriptionRequestMetadataBytes(
                ExportTicketHelper.exportIdToBytes(EXPORT_ID), null, viewportBuffer, null, false,
                BarrageMessageType.BarrageSubscriptionRequest);
        return Flight.FlightData.newBuilder()
                .setAppMetadata(ByteString.copyFrom(metadata))
                .build()
                .toByteString()
                .newInput();
    }

    /**
     * Waits until {@code thread} is blocked entering {@code monitor}, or has finished without needing it. Gives up
     * after the join timeout, so the test reports what is stuck instead of hanging here.
     */
    private static void awaitBlockedOn(final AtomicReference<Thread> thread, final Object monitor) {
        final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        final int monitorId = System.identityHashCode(monitor);
        final long deadline = System.currentTimeMillis() + JOIN_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline && thread.get().isAlive()) {
            final ThreadInfo info = bean.getThreadInfo(thread.get().getId());
            final LockInfo lock = info == null ? null : info.getLockInfo();
            if (info != null && info.getThreadState() == Thread.State.BLOCKED && lock != null
                    && lock.getIdentityHashCode() == monitorId) {
                return;
            }
            Thread.onSpinWait();
        }
    }

    private static String describeDeadlockedThreads() {
        final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        final long[] ids = bean.findDeadlockedThreads();
        if (ids == null) {
            return "(the JVM reports no deadlocked threads)";
        }
        final StringBuilder sb = new StringBuilder();
        for (final ThreadInfo info : bean.getThreadInfo(ids, true, true)) {
            sb.append('"').append(info.getThreadName()).append("\" ").append(info.getThreadState())
                    .append(" on ").append(info.getLockName())
                    .append(" owned by \"").append(info.getLockOwnerName()).append("\"\n");
            final StackTraceElement[] stack = info.getStackTrace();
            for (int ii = 0; ii < stack.length; ii++) {
                sb.append("\tat ").append(stack[ii]).append('\n');
                for (final MonitorInfo locked : info.getLockedMonitors()) {
                    if (locked.getLockedStackDepth() == ii) {
                        sb.append("\t- locked ").append(locked).append('\n');
                    }
                }
            }
            sb.append('\n');
        }
        return sb.toString();
    }

    /**
     * Makes enqueueing safe from other threads while one thread runs the queue.
     */
    private static final class SynchronizedTestControlledScheduler extends TestControlledScheduler {
        @Override
        public synchronized void runAtTime(final long epochMillis, @NotNull final Runnable command) {
            super.runAtTime(epochMillis, command);
        }

        @Override
        public synchronized void runAfterDelay(final long delayMs, @NotNull final Runnable command) {
            super.runAfterDelay(delayMs, command);
        }

        @Override
        public synchronized void runImmediately(@NotNull final Runnable command) {
            super.runImmediately(command);
        }
    }

    /**
     * A call whose writes can run the cancel handler, as the servlet transport does when a write fails.
     */
    private static final class InlineCancelCallObserver extends ServerCallStreamObserver<InputStream> {
        volatile Runnable onCancelHandler;
        /** runs on a write, while the writer holds the response stream's monitor */
        volatile Runnable onWrite;

        @Override
        public void onNext(final InputStream value) {
            final Runnable toRun = onWrite;
            if (toRun != null) {
                toRun.run();
            }
        }

        volatile Throwable error;
        volatile int completions;

        @Override
        public void onError(final Throwable t) {
            error = t;
        }

        @Override
        public void onCompleted() {
            ++completions;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(final Runnable onCancelHandler) {
            this.onCancelHandler = onCancelHandler;
        }

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
