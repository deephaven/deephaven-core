//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.browserstreaming;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionServiceGrpcImpl;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.GrpcServiceOverrideBuilder;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.util.SafeCloseable;
import io.grpc.Context;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Browsers cannot open bidirectional gRPC streams, so the server emulates them: the first message of a stream is
 * exported under the client's rpc ticket so that later messages can find it. This test covers what happens to that
 * export once the stream is over.
 */
public class BrowserStreamExportLifetimeTest {

    private static final Logger log = LoggerFactory.getLogger(BrowserStreamExportLifetimeTest.class);
    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();
    private static final long TOKEN_EXPIRE_MS = 1_000_000;

    private SafeCloseable livenessScope;
    private TestControlledScheduler scheduler;
    private SessionService sessionService;
    private SessionState session;

    @Before
    public void setup() {
        livenessScope = LivenessScopeStack.open();
        scheduler = new TestControlledScheduler();
        sessionService = new SessionService(scheduler,
                authContext -> new SessionState(scheduler, new SessionService.ObfuscatingErrorTransformer(),
                        TestExecutionContext::createForUnitTests, authContext),
                TOKEN_EXPIRE_MS, Collections.emptyMap(), Collections.emptySet());
        session = sessionService.newSession(AUTH_CONTEXT);
    }

    @After
    public void teardown() {
        sessionService.closeSession(session);
        livenessScope.close();
        session = null;
        sessionService = null;
        scheduler = null;
        livenessScope = null;
    }

    /**
     * Stands in for the real bidirectional service method; records what the server-side request stream sees.
     */
    private static final class RecordingDelegate implements GrpcServiceOverrideBuilder.BidiDelegate<String, String> {
        final List<String> received = new ArrayList<>();
        Throwable error;
        boolean completed;
        boolean serviceCancelHandlerRan;
        /** runs while a message is being delivered, so a test can react from inside the service */
        Consumer<String> onMessage;

        @Override
        public StreamObserver<String> doInvoke(final StreamObserver<String> responseObserver) {
            if (responseObserver instanceof ServerCallStreamObserver) {
                // as the real bidirectional services do, react to the client abandoning the call
                ((ServerCallStreamObserver<String>) responseObserver)
                        .setOnCancelHandler(() -> serviceCancelHandlerRan = true);
            }
            return new StreamObserver<>() {
                @Override
                public void onNext(final String value) {
                    received.add(value);
                    if (onMessage != null) {
                        onMessage.accept(value);
                    }
                }

                @Override
                public void onError(final Throwable t) {
                    error = t;
                }

                @Override
                public void onCompleted() {
                    completed = true;
                }
            };
        }
    }

    private static final class NoopObserver<T> implements StreamObserver<T> {
        @Override
        public void onNext(final T value) {}

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {}
    }

    /**
     * Stands in for the gRPC call observer of the open request; remembers the cancel handler so that the test can
     * abandon the call the way a browser would.
     */
    private static final class CapturingServerCallObserver<T> extends ServerCallStreamObserver<T> {
        /** when set, the call is already cancelled: a handler runs as soon as it is registered */
        boolean cancelledAlready;
        Runnable onCancelHandler;

        @Override
        public void onNext(final T value) {}

        @Override
        public void onError(final Throwable t) {}

        @Override
        public void onCompleted() {}

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public void setOnCancelHandler(final Runnable onCancelHandler) {
            this.onCancelHandler = onCancelHandler;
            if (cancelledAlready) {
                onCancelHandler.run();
            }
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

    /**
     * Runs {@code runnable} as the gRPC interceptors would for an emulated stream message from this session.
     */
    private void inStreamContext(final StreamData streamData, final Runnable runnable) {
        inStreamContext(session, streamData, runnable);
    }

    private static void inStreamContext(final SessionState session, final StreamData streamData,
            final Runnable runnable) {
        Context.current()
                .withValue(SessionServiceGrpcImpl.SESSION_CONTEXT_KEY, session)
                .withValue(StreamData.STREAM_DATA_KEY, streamData)
                .run(runnable);
    }

    /**
     * Runs the work queued so far without advancing the clock to the session cleanup job, which would expire the
     * session.
     */
    private void runPendingWork() {
        scheduler.runUntil(scheduler.timeAfterMs(1));
    }

    @Test
    public void testCompletedBrowserStreamReleasesItsExport() {
        final RecordingDelegate delegate = new RecordingDelegate();
        final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                        sessionService);
        final int rpcTicketId = 7;
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);

        // open: the first message arrives without a half-close, so the stream is exported under the rpc ticket
        inStreamContext(new StreamData(rpcTicket, 0, false),
                () -> method.invokeOpen("first", new NoopObserver<>()));
        runPendingWork();
        final SessionState.ExportObject<?> streamExport = session.getExportIfExists(rpcTicketId);
        Assert.neqNull(streamExport, "streamExport");
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.EXPORTED);
        Assert.eq(delegate.received.size(), "delegate.received.size()", 1);

        // next, with a half-close: the client is done with this stream
        inStreamContext(new StreamData(rpcTicket, 1, true),
                () -> method.invokeNext("last", new NoopObserver<>()));
        runPendingWork();
        Assert.eqTrue(delegate.completed, "delegate.completed");
        Assert.eqNull(delegate.error, "delegate.error");

        // a completed stream must not stay exported for the rest of the session's life
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.RELEASED);
    }

    @Test
    public void testCancelledOpenCallReleasesItsExport() {
        final RecordingDelegate delegate = new RecordingDelegate();
        final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                        sessionService);
        final int rpcTicketId = 8;
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);
        final CapturingServerCallObserver<String> openCall = new CapturingServerCallObserver<>();

        inStreamContext(new StreamData(rpcTicket, 0, false), () -> method.invokeOpen("first", openCall));
        runPendingWork();
        final SessionState.ExportObject<?> streamExport = session.getExportIfExists(rpcTicketId);
        Assert.neqNull(streamExport, "streamExport");
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.EXPORTED);
        Assert.neqNull(openCall.onCancelHandler, "openCall.onCancelHandler");

        // the browser abandons the call that opened the stream
        openCall.onCancelHandler.run();

        // both the service and the stream learn about it, and the stream's export is released
        Assert.eqTrue(delegate.serviceCancelHandlerRan, "delegate.serviceCancelHandlerRan");
        Assert.eqTrue(delegate.error instanceof StatusRuntimeException, "delegate.error instanceof SRE");
        Assert.eq(((StatusRuntimeException) delegate.error).getStatus().getCode(), "code", Status.Code.CANCELLED);
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.RELEASED);
    }

    /**
     * The export that retains a stream is defined inside the stream's constructor, and a {@code Next} call that arrived
     * out of order may already be waiting on that export: it runs as soon as the export's work does - in production on
     * another thread, before the constructor has returned - and may end the stream right there. The export and the
     * close callback must both be in place by then, or the export leaks and the service never hears that the stream
     * ended. A scheduler that runs work inline reproduces that interleaving deterministically.
     */
    @Test
    public void testStreamEndedByOutOfOrderNextDuringOpenReleasesItsExport() {
        final TestControlledScheduler inlineScheduler = new TestControlledScheduler() {
            @Override
            public void runImmediately(final Runnable command) {
                command.run();
            }

            @Override
            public void runSerially(final Runnable command) {
                command.run();
            }
        };
        final SessionService inlineSessionService = new SessionService(inlineScheduler,
                authContext -> new SessionState(inlineScheduler, new SessionService.ObfuscatingErrorTransformer(),
                        TestExecutionContext::createForUnitTests, authContext),
                TOKEN_EXPIRE_MS, Collections.emptyMap(), Collections.emptySet());
        final SessionState inlineSession = inlineSessionService.newSession(AUTH_CONTEXT);
        try {
            final RecordingDelegate delegate = new RecordingDelegate();
            final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                    new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                            inlineSessionService);
            final int rpcTicketId = 12;
            final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);

            // a (malformed) half-close at the open's own sequence arrives first and waits on the undefined export
            inStreamContext(inlineSession, new StreamData(rpcTicket, 0, true),
                    () -> method.invokeNext("close", new NoopObserver<>()));
            final SessionState.ExportObject<?> streamExport = inlineSession.getExportIfExists(rpcTicketId);
            Assert.neqNull(streamExport, "streamExport");

            // the open defines the export; the waiting Next runs inside the stream's constructor and ends the stream
            inStreamContext(inlineSession, new StreamData(rpcTicket, 0, false),
                    () -> method.invokeOpen("first", new CapturingServerCallObserver<>()));

            Assert.eqTrue(delegate.completed, "delegate.completed");
            Assert.eq(delegate.received.size(), "delegate.received.size()", 0);
            Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.RELEASED);
        } finally {
            inlineSessionService.closeSession(inlineSession);
        }
    }

    /**
     * {@code invokeOpen} rejects a session that has already expired before it builds a stream, so the constructor only
     * sees an expired session when expiry lands part way through an open. Whatever step fails, the service-side stream
     * created for the open must be told, or it hangs until collected.
     */
    @Test
    public void testOpenOnAnExpiredSessionEndsTheStream() {
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(11);
        sessionService.closeSession(session);

        for (final boolean halfClose : new boolean[] {true, false}) {
            final RecordingDelegate delegate = new RecordingDelegate();
            final BrowserStream.Factory<String, String> factory =
                    BrowserStream.factory(BrowserStream.Mode.IN_ORDER, delegate);
            try {
                factory.create(session, new StreamData(rpcTicket, 0, halfClose), new NoopObserver<>());
                Assert.statementNeverExecuted("a stream cannot be created on an expired session");
            } catch (final StatusRuntimeException expected) {
                Assert.eq(expected.getStatus().getCode(), "expected.getStatus().getCode()",
                        Status.Code.UNAUTHENTICATED);
            }
            Assert.neqNull(delegate.error, "delegate.error (halfClose=" + halfClose + ")");
            Assert.eqTrue(delegate.error instanceof StatusRuntimeException, "delegate.error instanceof SRE");
            Assert.eq(((StatusRuntimeException) delegate.error).getStatus().getCode(),
                    "delegate.error status code", Status.Code.UNAUTHENTICATED);
        }
    }

    @Test
    public void testOpenWithAnAlreadyDefinedTicketEndsTheStream() {
        final RecordingDelegate delegate = new RecordingDelegate();
        final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                        sessionService);
        final int rpcTicketId = 10;
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);
        // the client (mistakenly) reuses a ticket that already names another export
        session.newExport(rpcTicketId).submit(() -> "taken");

        try {
            inStreamContext(new StreamData(rpcTicket, 0, false),
                    () -> method.invokeOpen("first", new NoopObserver<>()));
            Assert.statementNeverExecuted("the open must fail when its ticket is already defined");
        } catch (final IllegalStateException expected) {
            // the export cannot be defined twice
        }
        // the stream that could not be exported must not linger on the session
        Assert.neqNull(delegate.error, "delegate.error");
        Assert.eqTrue(delegate.error instanceof IllegalStateException, "delegate.error instanceof ISE");
    }

    @Test
    public void testCancelDuringDeliveryDropsQueuedMessages() {
        final RecordingDelegate delegate = new RecordingDelegate();
        final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                        sessionService);
        final int rpcTicketId = 11;
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);
        final CapturingServerCallObserver<String> openCall = new CapturingServerCallObserver<>();

        inStreamContext(new StreamData(rpcTicket, 0, false), () -> method.invokeOpen("first", openCall));
        runPendingWork();
        // the third message arrives before the second and waits for it
        inStreamContext(new StreamData(rpcTicket, 2, false), () -> method.invokeNext("third", new NoopObserver<>()));
        runPendingWork();
        Assert.eq(delegate.received.size(), "delegate.received.size()", 1);

        // the browser abandons the call while the second message is being delivered
        delegate.onMessage = message -> {
            if (message.equals("second")) {
                openCall.onCancelHandler.run();
            }
        };
        inStreamContext(new StreamData(rpcTicket, 1, false), () -> method.invokeNext("second", new NoopObserver<>()));
        runPendingWork();

        // the queued third message is never delivered to a stream that has ended
        Assert.eq(delegate.received.size(), "delegate.received.size()", 2);
        Assert.eqTrue(delegate.error instanceof StatusRuntimeException, "delegate.error instanceof SRE");
        final SessionState.ExportObject<?> streamExport = session.getExportIfExists(rpcTicketId);
        Assert.neqNull(streamExport, "streamExport");
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.RELEASED);
    }

    @Test
    public void testSessionCloseEndsTheStreamAndReleasesItsExport() {
        final RecordingDelegate delegate = new RecordingDelegate();
        final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                        sessionService);
        final int rpcTicketId = 12;
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);

        inStreamContext(new StreamData(rpcTicket, 0, false),
                () -> method.invokeOpen("first", new NoopObserver<>()));
        runPendingWork();
        final SessionState.ExportObject<?> streamExport = session.getExportIfExists(rpcTicketId);
        Assert.neqNull(streamExport, "streamExport");
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.EXPORTED);

        // the login session ends: its exports are released and its streams are closed
        sessionService.closeSession(session);

        Assert.eqTrue(delegate.error instanceof StatusRuntimeException, "delegate.error instanceof SRE");
        Assert.eq(((StatusRuntimeException) delegate.error).getStatus().getCode(), "code", Status.Code.CANCELLED);
        Assert.eqTrue(SessionState.isExportStateTerminal(streamExport.getState()),
                "SessionState.isExportStateTerminal(streamExport.getState())");
    }

    @Test
    public void testFailedDeliveryEndsTheStreamAndReleasesItsExport() {
        final RecordingDelegate delegate = new RecordingDelegate();
        final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                        sessionService);
        final int rpcTicketId = 13;
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);

        inStreamContext(new StreamData(rpcTicket, 0, false),
                () -> method.invokeOpen("first", new NoopObserver<>()));
        runPendingWork();
        final SessionState.ExportObject<?> streamExport = session.getExportIfExists(rpcTicketId);
        Assert.neqNull(streamExport, "streamExport");
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.EXPORTED);

        // the service rejects the next message; that ends the stream
        delegate.onMessage = message -> {
            if (message.equals("second")) {
                throw new IllegalStateException("service rejected the message");
            }
        };
        inStreamContext(new StreamData(rpcTicket, 1, false),
                () -> method.invokeNext("second", new NoopObserver<>()));
        runPendingWork();

        Assert.eqTrue(delegate.error instanceof IllegalStateException, "delegate.error instanceof ISE");
        Assert.eq(streamExport.getState(), "streamExport.getState()", ExportNotification.State.RELEASED);
    }

    @Test
    public void testOpenCallCancelledBeforeHandlersRegisterStillEndsTheStream() {
        final RecordingDelegate delegate = new RecordingDelegate();
        final GrpcServiceOverrideBuilder.BrowserStreamMethod<String, String, Object> method =
                new GrpcServiceOverrideBuilder.BrowserStreamMethod<>(log, BrowserStream.Mode.IN_ORDER, delegate,
                        sessionService);
        final int rpcTicketId = 9;
        final Ticket rpcTicket = ExportTicketHelper.wrapExportIdInTicket(rpcTicketId);
        final CapturingServerCallObserver<String> openCall = new CapturingServerCallObserver<>();
        // the browser aborts before the server has finished wiring up the stream
        openCall.cancelledAlready = true;

        inStreamContext(new StreamData(rpcTicket, 0, false), () -> method.invokeOpen("first", openCall));
        runPendingWork();

        Assert.eqTrue(delegate.serviceCancelHandlerRan, "delegate.serviceCancelHandlerRan");
        Assert.eqTrue(delegate.error instanceof StatusRuntimeException, "delegate.error instanceof SRE");
        Assert.eq(delegate.received.size(), "delegate.received.size()", 0);
        final SessionState.ExportObject<?> streamExport = session.getExportIfExists(rpcTicketId);
        Assert.neqNull(streamExport, "streamExport");
        Assert.eqTrue(SessionState.isExportStateTerminal(streamExport.getState()),
                "SessionState.isExportStateTerminal(streamExport.getState())");
    }
}
