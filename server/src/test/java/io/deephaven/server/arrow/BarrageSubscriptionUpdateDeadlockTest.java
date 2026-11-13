//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import com.google.protobuf.ByteString;
import io.deephaven.auth.AuthContext;
import io.deephaven.barrage.flatbuf.BarrageSnapshotRequest;
import io.deephaven.barrage.flatbuf.BarrageSubscriptionRequest;
import io.deephaven.barrage.flatbuf.BarrageMessageType;
import io.deephaven.extensions.barrage.BarrageMessageWriter;
import io.deephaven.extensions.barrage.BarrageMessageWriterImpl;
import io.deephaven.extensions.barrage.BarragePerformanceLog;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.util.ImmediateJobScheduler;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
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
import io.grpc.stub.StreamObserver;
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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    private ControlledUpdateGraph updateGraph;
    /** runs the session's exports, including the handler's export-resolution continuation */
    private TestControlledScheduler sessionScheduler;
    /** runs the producer's propagation job */
    private TestControlledScheduler producerScheduler;
    private SessionState session;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests();
        executionContextCloseable = executionContext.open();
        updateGraph = executionContext.getUpdateGraph().cast();
        updateGraph.enableUnitTestMode();
        updateGraph.resetForUnitTests(false);
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
        updateGraph.resetForUnitTests(true);
        updateGraph = null;
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
            cancelAsGrpcDoes(call, marshaller);
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
     * A request that arrives while the rejection of an earlier one is being sent is ignored: the client sees why its
     * subscription ended, not a {@code NOT_FOUND} for a subscription that the producer has already removed.
     */
    @Test
    public void testRequestDuringRejectionIsIgnored() throws Exception {
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = openFullSubscription(call);
        producerScheduler.runUntilQueueEmpty();

        final AtomicReference<Throwable> laterRequestFailure = new AtomicReference<>();
        final AtomicBoolean laterRequestFinished = new AtomicBoolean();
        call.onErrorHook = () -> {
            call.onErrorHook = null;
            // the producer has removed the subscription and is sending its error
            final Thread laterRequest = new Thread(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(10, 19)));
                } catch (final Throwable t) {
                    laterRequestFailure.set(t);
                }
                laterRequestFinished.set(true);
            }, "BarrageSubscriptionUpdateDeadlockTest-laterRequest");
            laterRequest.setDaemon(true);
            laterRequest.start();
            try {
                laterRequest.join(JOIN_TIMEOUT_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        // a full subscription cannot change to a viewport
        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 9)));
        producerScheduler.runUntilQueueEmpty();

        assertTrue("the later request should finish", laterRequestFinished.get());
        assertNull("the later request should be ignored", laterRequestFailure.get());
        assertEquals(Status.Code.INVALID_ARGUMENT, Status.fromThrowable(call.error).getCode());
        assertEquals(0, call.completions);
    }

    /**
     * A write that fails after a rejection has been recorded sends the client the write's error; the rejection is not
     * sent afterwards as a second error.
     */
    @Test
    public void testRejectionAfterFailedWriteIsNotSentAgain() throws Exception {
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = openFullSubscription(call);

        final AtomicReference<Thread> rejectingThread = new AtomicReference<>();
        final AtomicReference<Throwable> rejectingFailure = new AtomicReference<>();
        call.onWrite = () -> {
            call.onWrite = null;
            // while the initial snapshot is being written, the client asks for a change that is rejected
            final Thread rejecting = new Thread(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 9)));
                } catch (final Throwable t) {
                    rejectingFailure.set(t);
                }
            }, "BarrageSubscriptionUpdateDeadlockTest-rejecting");
            rejecting.setDaemon(true);
            rejectingThread.set(rejecting);
            rejecting.start();
            // The request records the rejection and returns. It does not schedule the propagation job, because this
            // thread holds the job's run lock and runs the job again when it releases it.
            try {
                rejecting.join(JOIN_TIMEOUT_MILLIS);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("the write failed");
        };

        producerScheduler.runUntilQueueEmpty();
        rejectingThread.get().join(JOIN_TIMEOUT_MILLIS);
        assertFalse("the rejected request should finish", rejectingThread.get().isAlive());
        assertNull(rejectingFailure.get());
        producerScheduler.runUntilQueueEmpty();

        assertEquals("the client should be sent one error", 1, call.errors);
    }

    /**
     * A source table failure ends the stream with the producer's error; a later rejected request does not send a second
     * error.
     */
    @Test
    public void testRejectionAfterSourceFailureIsNotSentAgain() {
        final QueryTable source = TstUtils.testRefreshingTable(RowSetFactory.flat(10).toTracking(),
                TableTools.intCol("I", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = openFullSubscription(call, source);
        producerScheduler.runUntilQueueEmpty();
        assertNull(call.error);

        updateGraph.runWithinUnitTestCycle(
                () -> source.notifyListenersOnError(new RuntimeException("the source failed"), null));
        producerScheduler.runUntilQueueEmpty();
        assertEquals("the source failure should end the stream", 1, call.errors);

        // a full subscription cannot change to a viewport
        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 9)));
        producerScheduler.runUntilQueueEmpty();

        assertEquals("the client should be sent one error", 1, call.errors);
    }

    /**
     * Another thread reports an error to the stream while the producer's write fails and is cancelled inline. gRPC
     * reports the cancel to the request observer after running the cancel handler, and the marshaller sends that error
     * through {@link io.deephaven.extensions.barrage.util.GrpcUtil}, which locks the listener before the call. The
     * producer writes to the listener directly, so both must take the two locks in the same order.
     */
    @Test
    public void testErrorFromAnotherThreadDoesNotDeadlockWithCancelFromFailedWrite() throws Exception {
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = openFullSubscription(call);

        final AtomicReference<Thread> reporterThread = new AtomicReference<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        call.onWrite = () -> {
            call.onWrite = null;
            // while the initial snapshot is being written, an export failure is reported to the stream
            final Thread reporter = new Thread(() -> {
                try (final SafeCloseable ignored = executionContext.open()) {
                    marshaller.onError(new IllegalStateException("the export failed"));
                } catch (final Throwable t) {
                    failure.compareAndSet(null, t);
                }
            }, "BarrageSubscriptionUpdateDeadlockTest-reporter");
            reporter.setDaemon(true);
            reporterThread.set(reporter);
            reporter.start();
            // wait for the reporter to block on one of the stream's locks, which this thread holds while writing
            awaitBlocked(reporterThread);
            // the write fails; the servlet transport cancels the call on this thread
            cancelAsGrpcDoes(call, marshaller);
        };

        final Thread writer = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                producerScheduler.runUntilQueueEmpty();
            } catch (final Throwable t) {
                failure.compareAndSet(null, t);
            }
        }, "BarrageSubscriptionUpdateDeadlockTest-writer");
        writer.setDaemon(true);
        writer.start();

        final long deadline = System.currentTimeMillis() + JOIN_TIMEOUT_MILLIS;
        writer.join(Math.max(1, deadline - System.currentTimeMillis()));
        while (reporterThread.get() == null && System.currentTimeMillis() < deadline) {
            Thread.onSpinWait();
        }
        assertNotNull("the writer should have started the reporter", reporterThread.get());
        reporterThread.get().join(Math.max(1, deadline - System.currentTimeMillis()));
        if (writer.isAlive() || reporterThread.get().isAlive()) {
            throw new AssertionError("error report and inline cancel did not finish; deadlocked:\n"
                    + describeDeadlockedThreads());
        }
        if (failure.get() != null) {
            throw new AssertionError("worker thread failed", failure.get());
        }
        assertTrue("the marshaller should be closed", marshaller.isClosed());
    }

    /**
     * An invalid change that arrives before the export resolves is applied as the subscription starts, and the client
     * is sent its rejection.
     */
    @Test
    public void testInvalidChangeQueuedBeforeExportResolvesIsReported() {
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller =
                newMarshaller(call, TableTools.emptyTable(100).update("I = i"));

        marshaller.onNext(subscriptionRequest(null));
        // a full subscription cannot change to a viewport; the export has not resolved, so the request is queued
        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 9)));
        sessionScheduler.runUntilQueueEmpty();
        producerScheduler.runUntilQueueEmpty();

        assertEquals("the client should be sent one error", 1, call.errors);
        final Status status = Status.fromThrowable(call.error);
        assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertEquals("cannot change from full subscription to viewport or vice versa", status.getDescription());
        assertEquals(0, call.completions);
    }

    /**
     * A reverse viewport on a blink table is rejected, even after a valid change that the propagation job has not yet
     * applied.
     */
    @Test
    public void testBlinkTableReverseViewportIsReported() {
        final QueryTable table = TstUtils.testRefreshingTable(RowSetFactory.flat(10).toTracking(),
                TableTools.intCol("I", 0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
        table.setAttribute(Table.BLINK_TABLE_ATTRIBUTE, true);
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = newMarshaller(call, table);
        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 4)));
        sessionScheduler.runUntilQueueEmpty();
        producerScheduler.runUntilQueueEmpty();
        assertNull(call.error);

        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(2, 6)));
        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 4), true));
        producerScheduler.runUntilQueueEmpty();

        assertEquals("the client should be sent one error", 1, call.errors);
        final Status status = Status.fromThrowable(call.error);
        assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertEquals("Reverse viewport is not supported for blink tables", status.getDescription());
        assertEquals(0, call.completions);
    }

    /**
     * The propagation job fails after it has taken a rejected subscription out of the producer's lists, but before it
     * has sent the rejection. The producer's failure handling still ends that client's stream with its rejection, and
     * sends it without holding the producer's monitor.
     */
    @Test
    public void testRejectionIsSentWhenPropagationFails() throws Exception {
        final AtomicReference<Object> producer = new AtomicReference<>();
        final InlineCancelCallObserver call = new InlineCancelCallObserver();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = newMarshaller(call,
                TableTools.emptyTable(100).update("I = i"), producer::set);
        marshaller.onNext(subscriptionRequest(null));
        sessionScheduler.runUntilQueueEmpty();
        producerScheduler.runUntilQueueEmpty();
        assertNull(call.error);

        // a full subscription cannot change to a viewport
        marshaller.onNext(subscriptionRequest(RowSetFactory.fromRange(0, 9)));
        final AtomicReference<Boolean> sentHoldingProducerMonitor = new AtomicReference<>();
        call.onErrorHook = () -> sentHoldingProducerMonitor.set(Thread.holdsLock(producer.get()));
        // the job's bookkeeping fails after it has removed the deleted subscription from its lists
        final Field postSnapshotColumns = producer.get().getClass().getDeclaredField("postSnapshotColumns");
        postSnapshotColumns.setAccessible(true);
        postSnapshotColumns.set(producer.get(), null);
        producerScheduler.runUntilQueueEmpty();

        assertEquals("the client should be sent one error", 1, call.errors);
        final Status status = Status.fromThrowable(call.error);
        assertEquals(Status.Code.INVALID_ARGUMENT, status.getCode());
        assertEquals("cannot change from full subscription to viewport or vice versa", status.getDescription());
        assertFalse("the error must be sent without the producer's monitor", sentHoldingProducerMonitor.get());
    }

    /**
     * When the stream closes while the handler is subscribing, the handler closes the new subscription without holding
     * its own monitor. Closing a subscription can write to the stream, and a write that fails on another thread can
     * cancel the call there while it holds the stream's lock; the cancel closes the handler, which needs the monitor.
     */
    @Test
    public void testSubscriptionClosedDuringSubscribeIsClosedWithoutHandlerMonitor() {
        final Table table = TableTools.emptyTable(10);
        session.newExport(EXPORT_ID).submit(() -> table);
        sessionScheduler.runUntilQueueEmpty();

        final AtomicReference<ArrowFlightUtil.DoExchangeMarshaller> marshallerRef = new AtomicReference<>();
        final AtomicReference<ArrowFlightUtil.DoExchangeMarshaller.Handler> handlerRef = new AtomicReference<>();
        final AtomicReference<Boolean> closedHoldingHandlerMonitor = new AtomicReference<>();

        final ExchangeRequestHandlerFactory handlerFactory = new ExchangeRequestHandlerFactory() {
            private final ExchangeRequestHandlerFactory delegate =
                    new BarrageSubscriptionRequestHandler.BarrageSubscriptionRequestHandlerFactory();

            @Override
            public byte type() {
                return delegate.type();
            }

            @Override
            public ArrowFlightUtil.DoExchangeMarshaller.Handler create(
                    final ArrowFlightUtil.DoExchangeMarshaller marshaller,
                    final StreamObserver<BarrageMessageWriter.MessageView> listener) {
                final ArrowFlightUtil.DoExchangeMarshaller.Handler handler = delegate.create(marshaller, listener);
                handlerRef.set(handler);
                return handler;
            }
        };

        final ExchangeMarshaller closingMarshaller = new ExchangeMarshaller() {
            @Override
            public int priority() {
                return 0;
            }

            @Override
            public boolean accept(final Object export) {
                return true;
            }

            @Override
            public void snapshot(final BarrageSnapshotRequest snapshotRequest,
                    final BarrageSnapshotOptions options,
                    final Object export,
                    final BarragePerformanceLog.SnapshotMetricsHelper metrics,
                    final StreamObserver<BarrageMessageWriter.MessageView> listener,
                    final String ticketLogName,
                    final BarrageMessageWriter.Factory streamGeneratorFactory) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Subscription subscribe(final BarrageSubscriptionRequest subscriptionRequest,
                    final BarrageSubscriptionOptions options,
                    final Object export,
                    final StreamObserver<BarrageMessageWriter.MessageView> listener) {
                // the client half-closes while the subscription is being created
                marshallerRef.get().onCompleted();
                return new Subscription() {
                    @Override
                    public LivenessReferent toManage() {
                        return null;
                    }

                    @Override
                    public void close() {
                        closedHoldingHandlerMonitor.set(Thread.holdsLock(handlerRef.get()));
                    }

                    @Override
                    public boolean update(final BarrageSubscriptionRequest subscriptionRequest) {
                        return true;
                    }
                };
            }
        };

        final SessionService.ErrorTransformer errorTransformer = new SessionService.ObfuscatingErrorTransformer();
        final AllowAllAuthorizationProvider authProvider = new AllowAllAuthorizationProvider();
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = new ArrowFlightUtil.DoExchangeMarshaller(
                new TicketRouter(authProvider, Set.of(new ExportTicketResolver(authProvider))),
                new BarrageMessageWriterImpl.Factory(),
                List.of(closingMarshaller),
                Set.of(handlerFactory),
                errorTransformer,
                session,
                new InlineCancelCallObserver());
        marshallerRef.set(marshaller);

        marshaller.onNext(subscriptionRequest(null));
        sessionScheduler.runUntilQueueEmpty();

        assertTrue("the marshaller should be closed", marshaller.isClosed());
        assertNotNull("the new subscription should be closed", closedHoldingHandlerMonitor.get());
        assertFalse("the new subscription must be closed without the handler's monitor",
                closedHoldingHandlerMonitor.get());
    }

    /**
     * Exports a table and opens a DoExchange full subscription to it on {@code call}. The producer's propagation job is
     * scheduled but has not run.
     */
    private ArrowFlightUtil.DoExchangeMarshaller openFullSubscription(final InlineCancelCallObserver call) {
        return openFullSubscription(call, TableTools.emptyTable(100).update("I = i"));
    }

    /**
     * Exports {@code table} and opens a DoExchange full subscription to it on {@code call}. The producer's propagation
     * job is scheduled but has not run.
     */
    private ArrowFlightUtil.DoExchangeMarshaller openFullSubscription(final InlineCancelCallObserver call,
            final Table table) {
        final ArrowFlightUtil.DoExchangeMarshaller marshaller = newMarshaller(call, table);
        // the export is already resolved, so the continuation subscribes when the session runs
        marshaller.onNext(subscriptionRequest(null));
        sessionScheduler.runUntilQueueEmpty();
        return marshaller;
    }

    /**
     * Exports {@code table} and opens a DoExchange stream on {@code call}, with no request sent yet.
     */
    private ArrowFlightUtil.DoExchangeMarshaller newMarshaller(final InlineCancelCallObserver call,
            final Table table) {
        return newMarshaller(call, table, producer -> {
        });
    }

    /**
     * Exports {@code table} and opens a DoExchange stream on {@code call}, with no request sent yet. Each
     * subscription's producer is passed to {@code onProducer}.
     */
    private ArrowFlightUtil.DoExchangeMarshaller newMarshaller(final InlineCancelCallObserver call,
            final Table table, final Consumer<Object> onProducer) {
        session.newExport(EXPORT_ID).submit(() -> table);
        sessionScheduler.runUntilQueueEmpty();

        final SessionService.ErrorTransformer errorTransformer = new SessionService.ObfuscatingErrorTransformer();
        final BarrageMessageWriterImpl.Factory streamGeneratorFactory = new BarrageMessageWriterImpl.Factory();
        final AllowAllAuthorizationProvider authProvider = new AllowAllAuthorizationProvider();
        final TicketRouter ticketRouter =
                new TicketRouter(authProvider, Set.of(new ExportTicketResolver(authProvider)));
        final ExchangeMarshaller tableMarshaller = new TableExchangeMarshaller.Factory().create(
                producerScheduler, errorTransformer, streamGeneratorFactory, ImmediateJobScheduler::new);

        return new ArrowFlightUtil.DoExchangeMarshaller(
                ticketRouter,
                streamGeneratorFactory,
                List.of(new ProducerCapturingMarshaller(tableMarshaller, onProducer)),
                Set.of(new BarrageSubscriptionRequestHandler.BarrageSubscriptionRequestHandlerFactory()),
                errorTransformer,
                session,
                call);
    }

    /**
     * Fails the write the way the servlet transport does: the call's cancel handler runs on the writing thread, and
     * then gRPC reports the cancel to the request observer, since the client has not half-closed.
     */
    private static void cancelAsGrpcDoes(final InlineCancelCallObserver call,
            final ArrowFlightUtil.DoExchangeMarshaller marshaller) {
        call.onCancelHandler.run();
        marshaller.onError(Status.CANCELLED.withDescription("client cancelled").asRuntimeException());
    }

    private static InputStream subscriptionRequest(@Nullable final RowSet viewport) {
        return subscriptionRequest(viewport, false);
    }

    private static InputStream subscriptionRequest(@Nullable final RowSet viewport, final boolean reverseViewport) {
        final ByteBuffer viewportBuffer = viewport == null ? null : BarrageProtoUtil.toByteBuffer(viewport);
        final byte[] metadata = BarrageUtil.createSubscriptionRequestMetadataBytes(
                ExportTicketHelper.exportIdToBytes(EXPORT_ID), null, viewportBuffer, null, reverseViewport,
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

    /**
     * Waits until {@code thread} is blocked entering any monitor, or has finished. Gives up after the join timeout, so
     * the test reports what is stuck instead of hanging here.
     */
    private static void awaitBlocked(final AtomicReference<Thread> thread) {
        final long deadline = System.currentTimeMillis() + JOIN_TIMEOUT_MILLIS;
        while (System.currentTimeMillis() < deadline && thread.get().isAlive()
                && thread.get().getState() != Thread.State.BLOCKED) {
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
     * Passes each subscription's {@link io.deephaven.server.barrage.BarrageMessageProducer} to a test.
     */
    private static final class ProducerCapturingMarshaller implements ExchangeMarshaller {
        private final ExchangeMarshaller delegate;
        private final Consumer<Object> onProducer;

        private ProducerCapturingMarshaller(final ExchangeMarshaller delegate, final Consumer<Object> onProducer) {
            this.delegate = delegate;
            this.onProducer = onProducer;
        }

        @Override
        public int priority() {
            return delegate.priority();
        }

        @Override
        public boolean accept(final Object export) {
            return delegate.accept(export);
        }

        @Override
        public void snapshot(final BarrageSnapshotRequest snapshotRequest,
                final BarrageSnapshotOptions options,
                final Object export,
                final BarragePerformanceLog.SnapshotMetricsHelper metrics,
                final StreamObserver<BarrageMessageWriter.MessageView> listener,
                final String ticketLogName,
                final BarrageMessageWriter.Factory streamGeneratorFactory) {
            delegate.snapshot(snapshotRequest, options, export, metrics, listener, ticketLogName,
                    streamGeneratorFactory);
        }

        @Override
        public Subscription subscribe(final BarrageSubscriptionRequest subscriptionRequest,
                final BarrageSubscriptionOptions options,
                final Object export,
                final StreamObserver<BarrageMessageWriter.MessageView> listener) {
            final Subscription subscription = delegate.subscribe(subscriptionRequest, options, export, listener);
            try {
                final Field producer = subscription.getClass().getDeclaredField("bmp");
                producer.setAccessible(true);
                onProducer.accept(producer.get(subscription));
            } catch (final ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
            return subscription;
        }
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
        volatile int errors;
        volatile int completions;

        /** runs when the error is written, while the writer holds the response stream's monitor */
        volatile Runnable onErrorHook;

        @Override
        public void onError(final Throwable t) {
            error = t;
            ++errors;
            final Runnable toRun = onErrorHook;
            if (toRun != null) {
                toRun.run();
            }
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
