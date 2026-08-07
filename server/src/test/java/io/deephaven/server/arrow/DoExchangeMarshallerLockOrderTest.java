//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.arrow;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.extensions.barrage.util.BarrageProtoUtil;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Regression test for DH-22370: lock-order inversion between {@link ArrowFlightUtil.DoExchangeMarshaller} and its
 * {@link ArrowFlightUtil.DoExchangeMarshaller.Handler}.
 *
 * <p>
 * The reported deadlock had two threads:
 * <ul>
 * <li>Thread A inside {@code BarrageSubscriptionRequestHandler.onExportResolved} (holding the handler's monitor)
 * reaching {@code DoExchangeMarshaller.close} via a synchronously-delivered gRPC {@code onCancel} callback and blocking
 * on the marshaller's monitor.</li>
 * <li>Thread B inside {@code DoExchangeMarshaller.onNext} (holding the marshaller's monitor) dispatching to
 * {@code Handler.handleMessage}, whose internal {@code synchronized(this)} blocked on the handler's monitor.</li>
 * </ul>
 *
 * <p>
 * This test exercises the same monitor ordering against a real {@link ArrowFlightUtil.DoExchangeMarshaller} and a
 * minimal {@link ArrowFlightUtil.DoExchangeMarshaller.Handler}. With the fix (onNext releases the marshaller monitor
 * before dispatching), both threads complete; without the fix the JVM's deadlock detector flags the cycle and the test
 * fails.
 */
public class DoExchangeMarshallerLockOrderTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private SafeCloseable executionContext;
    private TestControlledScheduler scheduler;
    private SessionState session;

    @Before
    public void setUp() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        scheduler = new TestControlledScheduler();
        // initializeExpiration is protected, so we invoke it via an instance initializer in an
        // anonymous subclass — same pattern SessionState's own tests would use from inside the package.
        session = new SessionState(scheduler, new SessionService.ObfuscatingErrorTransformer(),
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
        scheduler = null;
        executionContext.close();
        executionContext = null;
    }

    @Test(timeout = 15_000L)
    public void testOnNextDoesNotDeadlockWhenCallbackHoldsHandlerMonitor() throws Exception {
        final AtomicBoolean holdsMarshallerMonitorAtDispatch = new AtomicBoolean();
        final CountDownLatch handleMessageEntered = new CountDownLatch(1);
        final AtomicReference<ArrowFlightUtil.DoExchangeMarshaller> marshallerRef = new AtomicReference<>();

        final StreamObserver<InputStream> observer = new StreamObserver<InputStream>() {
            @Override
            public void onNext(final InputStream value) {}

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {}
        };

        final ArrowFlightUtil.DoExchangeMarshaller marshaller = new ArrowFlightUtil.DoExchangeMarshaller(
                /* ticketRouter */ null,
                /* streamGeneratorFactory */ null,
                /* exchangeMarshallers */ Collections.emptyList(),
                /* requestHandlerFactories */ Collections.emptySet(),
                /* errorTransformer */ new SessionService.ObfuscatingErrorTransformer(),
                session,
                observer);
        marshallerRef.set(marshaller);

        // Minimal Handler that mirrors BarrageSubscriptionRequestHandler's relevant synchronization pattern:
        // handleMessage synchronizes on `this`. That inner monitor is the one the "cancel-side" thread
        // holds in the real deadlock (via the synchronized onExportResolved method on the same instance).
        final ArrowFlightUtil.DoExchangeMarshaller.Handler handler =
                new ArrowFlightUtil.DoExchangeMarshaller.Handler() {
                    @Override
                    public void handleMessage(@NotNull final BarrageProtoUtil.MessageInfo message) {
                        // Capture the pre-dispatch state: if onNext is still holding the marshaller monitor here,
                        // we have the hazard that closes the lock-inversion cycle.
                        holdsMarshallerMonitorAtDispatch.set(Thread.holdsLock(marshallerRef.get()));
                        handleMessageEntered.countDown();
                        synchronized (this) {
                            // Matches BarrageSubscriptionRequestHandler.handleMessage's synchronized(this) block.
                            // Thread Y (below) will be holding this monitor when Thread X arrives, so X will park
                            // here; if X is still holding the marshaller monitor at this point, the cycle closes.
                        }
                    }

                    @Override
                    public void close() {}
                };

        // Install the handler directly so onNext takes the "requestHandler already set" dispatch path and
        // we can skip the flatbuffer machinery that would otherwise be required for factory lookup.
        marshaller.setRequestHandler(handler);

        final CountDownLatch threadYHoldsHandler = new CountDownLatch(1);
        final CountDownLatch threadYProceed = new CountDownLatch(1);
        final AtomicReference<Throwable> threadFailure = new AtomicReference<>();

        // Thread Y: simulates the onExportResolved → (gRPC onCancel) → DoExchangeMarshaller.close call chain.
        // The real onExportResolved is a `synchronized` method on the handler; we recreate the same monitor
        // acquisition here and then call marshaller.close(), which acquires the marshaller monitor internally.
        final Thread threadY = new Thread(() -> {
            try {
                synchronized (handler) {
                    threadYHoldsHandler.countDown();
                    // Give Thread X time to enter onNext and (pre-fix) block at handleMessage's
                    // synchronized(this) while still holding the marshaller monitor.
                    if (!handleMessageEntered.await(10, TimeUnit.SECONDS)) {
                        // Thread X never reached handleMessage — allow Thread Y to proceed anyway so the
                        // test can terminate; findDeadlockedThreads() will still be the source of truth.
                    }
                    // Hold a moment longer to ensure X is parked on the handler monitor.
                    Thread.sleep(200);
                    threadYProceed.countDown();
                    marshaller.close();
                }
            } catch (final Throwable t) {
                threadFailure.compareAndSet(null, t);
            }
        }, "lock-order-test-thread-Y");
        threadY.setDaemon(true);

        // Thread X: drives onNext. Pre-fix, this acquires the marshaller monitor and calls
        // handler.handleMessage under it, colliding with Thread Y to form the AB/BA cycle.
        final Thread threadX = new Thread(() -> {
            try {
                // Wait for Y to own the handler monitor so the ordering is deterministic.
                if (!threadYHoldsHandler.await(10, TimeUnit.SECONDS)) {
                    fail("Thread Y did not acquire the handler monitor before Thread X proceeded");
                }
                marshaller.onNext(new ByteArrayInputStream(new byte[0]));
            } catch (final Throwable t) {
                threadFailure.compareAndSet(null, t);
            }
        }, "lock-order-test-thread-X");
        threadX.setDaemon(true);

        threadY.start();
        threadX.start();

        // Let the race resolve (or the cycle form).
        Thread.sleep(2_000);

        final ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        final long[] deadlockedIds = bean.findDeadlockedThreads();
        if (deadlockedIds != null) {
            final StringBuilder sb = new StringBuilder(
                    "Deadlock detected between DoExchangeMarshaller.onNext and close() paths.\n");
            for (final ThreadInfo info : bean.getThreadInfo(deadlockedIds, true, true)) {
                sb.append(info);
            }
            fail(sb.toString());
        }

        threadX.join(5_000);
        threadY.join(5_000);
        assertFalse("Thread X did not terminate — possible livelock or blocked state", threadX.isAlive());
        assertFalse("Thread Y did not terminate — possible livelock or blocked state", threadY.isAlive());

        if (threadFailure.get() != null) {
            throw new AssertionError("Worker thread threw", threadFailure.get());
        }

        // Direct invariant: onNext must not hold the marshaller's monitor at the moment it dispatches to
        // the handler. This is the property the fix establishes; asserting it here makes the regression
        // failure mode explicit even on systems where ThreadMXBean.findDeadlockedThreads() is slow to
        // observe the cycle.
        assertTrue("Thread Y should have reached marshaller.close() before the test timeout",
                threadYProceed.await(1, TimeUnit.SECONDS));
        assertFalse(
                "DoExchangeMarshaller.onNext must not hold its own monitor while dispatching to Handler.handleMessage",
                holdsMarshallerMonitorAtDispatch.get());

        Assert.eq(handleMessageEntered.getCount(), "handleMessageEntered.getCount()", 0L);
    }
}
