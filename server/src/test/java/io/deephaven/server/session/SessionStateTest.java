//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.auth.AuthContext;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.testutil.testcase.FakeProcessEnvironment;
import io.deephaven.hash.KeyedIntObjectHashMap;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.process.ProcessEnvironment;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.deephaven.proto.backplane.grpc.ExportNotification.State.*;
import static io.deephaven.proto.util.ExportTicketHelper.ticketToExportId;
import static org.junit.Assert.assertThrows;

public class SessionStateTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private SafeCloseable executionContext;
    private LivenessScope livenessScope;
    private TestControlledScheduler scheduler;
    private SessionState session;
    private int nextExportId;
    private ProcessEnvironment oldProcessEnvironment;

    @Before
    public void setup() {
        executionContext = TestExecutionContext.createForUnitTests().open();
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        scheduler = new TestControlledScheduler();
        session = new SessionState(scheduler, scheduler, new SessionService.ObfuscatingErrorTransformer(),
                TestExecutionContext::createForUnitTests, AUTH_CONTEXT);
        session.initializeExpiration(new SessionService.TokenExpiration(UUID.randomUUID(),
                DateTimeUtils.epochMillis(DateTimeUtils.epochNanosToInstant(Long.MAX_VALUE)), session));
        nextExportId = 1;

        oldProcessEnvironment = ProcessEnvironment.tryGet();
        ProcessEnvironment.set(FakeProcessEnvironment.INSTANCE, true);
    }

    @After
    public void teardown() {
        if (oldProcessEnvironment == null) {
            ProcessEnvironment.clear();
        } else {
            ProcessEnvironment.set(oldProcessEnvironment, true);
        }

        LivenessScopeStack.pop(livenessScope);
        livenessScope.release();
        livenessScope = null;
        scheduler = null;
        session = null;
        executionContext.close();
    }

    @Test
    public void testDestroyOnExportRelease() {
        final MutableBoolean success = new MutableBoolean();
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            exportObj = session.newExport(nextExportId++)
                    .onSuccess(success::setTrue)
                    .submit(() -> export);
        }

        // no ref counts yet
        Assert.eq(export.refCount, "export.refCount", 0);
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");

        // export the object; should inc ref count
        scheduler.runUntilQueueEmpty();
        Assert.eq(export.refCount, "export.refCount", 1);
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");

        // assert lookup is same object
        Assert.eq(session.getExport(nextExportId - 1), "session.getExport(nextExport - 1)", exportObj, "exportObj");
        Assert.equals(exportObj.getExportId(), "exportObj.getExportId()",
                ExportTicketHelper.wrapExportIdInTicket(nextExportId - 1),
                "nextExportId - 1");

        // release
        exportObj.release();
        Assert.eq(export.refCount, "export.refCount", 0);
    }

    @Test
    public void testServerExportDestroyOnExportRelease() {
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            exportObj = session.newServerSideExport(export);
        }

        // better have ref count
        Assert.eq(export.refCount, "export.refCount", 1);

        // assert lookup is same object
        Assert.eq(session.getExport(exportObj.getExportId(), "test"),
                "session.getExport(exportObj.getExportId())", exportObj, "exportObj");

        // release
        exportObj.release();
        Assert.eq(export.refCount, "export.refCount", 0);
    }

    @Test
    public void testDestroyOnSessionRelease() {
        final MutableBoolean success = new MutableBoolean();
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            exportObj = session.newExport(nextExportId++)
                    .onSuccess(success::setTrue)
                    .submit(() -> export);
        }

        // no ref counts yet
        Assert.eq(export.refCount, "export.refCount", 0);
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");

        // export the object; should inc ref count
        scheduler.runUntilQueueEmpty();
        Assert.eq(export.refCount, "export.refCount", 1);
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");

        // assert lookup is same object
        Assert.eq(session.getExport(nextExportId - 1),
                "session.getExport(nextExport - 1)", exportObj, "exportObj");
        Assert.equals(exportObj.getExportId(), "exportObj.getExportId()",
                ExportTicketHelper.wrapExportIdInTicket(nextExportId - 1),
                "nextExportId - 1");

        // release
        session.onExpired();
        Assert.eq(export.refCount, "export.refCount", 0);
    }

    @Test
    public void testReleasePropagatesToOtherSessionChildren() {
        final MutableBoolean error = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            exportObj = session.newExport(nextExportId++)
                    .onSuccess(success::setTrue)
                    .onError((result, errorContext, cause, dependentId) -> error.setTrue())
                    .submit(() -> export);
        }

        // no ref counts yet
        Assert.eq(export.refCount, "export.refCount", 0);
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");

        final MutableBoolean otherSuccess = new MutableBoolean();
        final MutableBoolean otherError = new MutableBoolean();
        final SessionState other =
                new SessionState(scheduler, scheduler, new SessionService.ObfuscatingErrorTransformer(),
                        TestExecutionContext::createForUnitTests, AUTH_CONTEXT);
        other.initializeExpiration(new SessionService.TokenExpiration(UUID.randomUUID(),
                DateTimeUtils.epochMillis(DateTimeUtils.epochNanosToInstant(Long.MAX_VALUE)), other));
        final SessionState.ExportObject<Object> otherExportObj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            otherExportObj = other.newExport(nextExportId++)
                    .require(exportObj)
                    .onSuccess(otherSuccess::setTrue)
                    .onError((result, errorContext, cause, dependentId) -> otherError.setTrue())
                    .submit(exportObj::get);
        }

        // release
        session.onExpired();

        // export the object; should not inc ref count or alter state
        scheduler.runUntilQueueEmpty();
        Assert.eq(export.refCount, "export.refCount", 0);
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eqTrue(error.booleanValue(), "error.booleanValue()");
        Assert.eqFalse(otherSuccess.booleanValue(), "otherSuccess.booleanValue()");
        Assert.eqTrue(otherError.booleanValue(), "otherError.booleanValue()");

        Assert.eq(otherExportObj.getState(), "otherExportObj.getState()", DEPENDENCY_FAILED);
    }

    @Test
    public void testServerExportDestroyOnSessionRelease() {
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            exportObj = session.newServerSideExport(export);
        }

        // better have ref count
        Assert.eq(export.refCount, "export.refCount", 1);

        // assert lookup is same object
        Assert.eq(session.getExport(exportObj.getExportId(), "test"),
                "session.getExport(exportObj.getExportId())", exportObj, "exportObj");

        // release
        session.onExpired();
        Assert.eq(export.refCount, "export.refCount", 0);
    }

    @Test
    public void testWorkItemNoDependencies() {
        final Object export = new Object();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onSuccess(success::setTrue)
                .submit(() -> export);
        expectException(IllegalStateException.class, exportObj::get);
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.get(), "exportObj.get()", export, "export");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.EXPORTED);
    }

    @Test
    public void testThrowInExportMain() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> {
                    throw new RuntimeException("submit exception");
                });
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        scheduler.runUntilQueueEmpty();
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.FAILED);
    }

    @Test
    public void testThrowInErrorHandler() {
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onErrorHandler(err -> {
                    throw new RuntimeException("error handler exception");
                })
                .onSuccess(success::setTrue)
                .submit(() -> {
                    submitted.setTrue();
                    throw new RuntimeException("submit exception");
                });
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        boolean caught = false;
        try {
            scheduler.runUntilQueueEmpty();
        } catch (final FakeProcessEnvironment.FakeFatalException ignored) {
            caught = true;
        }
        Assert.eqTrue(caught, "caught");
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.FAILED);
    }

    @Test
    public void testThrowInSuccessHandler() {
        final MutableBoolean failed = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onErrorHandler(err -> failed.setTrue())
                .onSuccess(ignored -> {
                    throw new RuntimeException("on success exception");
                }).submit(submitted::setTrue);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(failed.booleanValue(), "success.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        boolean caught = false;
        try {
            scheduler.runUntilQueueEmpty();
        } catch (final FakeProcessEnvironment.FakeFatalException ignored) {
            caught = true;
        }
        Assert.eqTrue(caught, "caught");
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(failed.booleanValue(), "success.booleanValue()");
        // although we will want the jvm to exit -- we expect that the export to be successful
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.EXPORTED);
    }

    @Test
    public void testCancelBeforeDefined() {
        final SessionState.ExportObject<Object> exportObj = session.getExport(nextExportId);
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.UNKNOWN);

        exportObj.cancel();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);

        // We should be able to cancel prior to definition without error.
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        session.newExport(nextExportId++)
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        scheduler.runUntilQueueEmpty();

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testCancelBeforeExport() {
        final SessionState.ExportObject<?> d1 = session.getExport(nextExportId++);

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.PENDING);
        exportObj.cancel();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);
        scheduler.runUntilQueueEmpty();

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testCancelDuringExport() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableObject<LivenessArtifact> export = new MutableObject<>();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> {
                    session.getExport(nextExportId - 1).cancel();
                    export.setValue(new PublicLivenessArtifact());
                    return export;
                });

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");

        if (export.getValue().tryRetainReference()) {
            throw new IllegalStateException("this should be destroyed");
        }
    }

    @Test
    public void testCancelPostExport() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableObject<LivenessArtifact> export = new MutableObject<>();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            exportObj = session.newExport(nextExportId++)
                    .onErrorHandler(err -> errored.setTrue())
                    .onSuccess(success::setTrue)
                    .submit(() -> {
                        export.setValue(new PublicLivenessArtifact());
                        return export.getValue();
                    });
        }

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.EXPORTED);
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");

        if (!export.getValue().tryRetainReference()) {
            throw new IllegalStateException("this should be live");
        }
        export.getValue().dropReference();

        exportObj.cancel();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.RELEASED);
        if (export.getValue().tryRetainReference()) {
            throw new IllegalStateException("this should be destroyed");
        }
    }

    @Test
    public void testCancelPropagates() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);

        d1.cancel();
        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.DEPENDENCY_CANCELLED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testErrorPropagatesNotYetFailed() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);

        session.newExport(d1.getExportId(), "test")
                .submit(() -> {
                    throw new RuntimeException("I fail.");
                });

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.DEPENDENCY_FAILED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testErrorPropagatesAlreadyFailed() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.newExport(nextExportId++)
                .submit(() -> {
                    throw new RuntimeException("I fail.");
                });
        scheduler.runUntilQueueEmpty();
        Assert.eq(d1.getState(), "d1.getState()", ExportNotification.State.FAILED);

        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.DEPENDENCY_FAILED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testWorkItemOutOfOrderDependency() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.PENDING);

        session.newExport(d1.getExportId(), "test")
                .submit(() -> {
                });
        scheduler.runOne(); // d1
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");

        scheduler.runOne(); // d1
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.EXPORTED);
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testWorkItemDeepDependency() {
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++)
                .submit(() -> {
                });
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .submit(() -> {
                });
        final SessionState.ExportObject<Object> e3 = session.newExport(nextExportId++)
                .require(e2)
                .submit(submitted::setTrue);

        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.QUEUED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.PENDING);
        Assert.eq(e3.getState(), "e3.getState()", ExportNotification.State.PENDING);
        scheduler.runOne();
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");

        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.QUEUED);
        Assert.eq(e3.getState(), "e3.getState()", ExportNotification.State.PENDING);
        scheduler.runOne();
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");

        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.EXPORTED);
        Assert.eq(e3.getState(), "e3.getState()", ExportNotification.State.QUEUED);
        scheduler.runOne();
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eq(e3.getState(), "e3.getState()", ExportNotification.State.EXPORTED);
    }

    @Test
    public void testDependencyNotReleasedEarly() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final CountingLivenessReferent export = new CountingLivenessReferent();

        final SessionState.ExportObject<CountingLivenessReferent> e1;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            e1 = session.<CountingLivenessReferent>newExport(nextExportId++)
                    .submit(() -> export);
        }

        scheduler.runOne();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);

        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> Assert.gt(e1.get().refCount, "e1.get().refCount", 0));
        Assert.eq(e2.getState(), "e1.getState()", ExportNotification.State.QUEUED);

        e1.release();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);

        Assert.gt(export.refCount, "e1.get().refCount", 0);
        scheduler.runOne();
        Assert.eq(export.refCount, "e1.get().refCount", 0);
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testLateDependencyAlreadyReleasedFails() {
        final CountingLivenessReferent export = new CountingLivenessReferent();

        final SessionState.ExportObject<CountingLivenessReferent> e1;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            e1 = session.<CountingLivenessReferent>newExport(nextExportId++)
                    .submit(() -> export);
        }

        scheduler.runOne();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);
        e1.release();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<?> e2 = session.newExport(nextExportId++)
                .require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit((Callable<Object>) Assert::statementNeverExecuted);
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_RELEASED);
    }

    /**
     * A released export may still be retained by something else (an earlier dependent that has not run yet, or an
     * export listener inside its callback), so its reference count does not tell a late dependent that it is gone. The
     * dependency must be rejected by state, not by whether the export can still be managed.
     */
    @Test
    public void testLateDependencyOnReleasedExportStillRetainedElsewhereFails() {
        final CountingLivenessReferent export = new CountingLivenessReferent();

        final SessionState.ExportObject<CountingLivenessReferent> e1;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            e1 = session.<CountingLivenessReferent>newExport(nextExportId++)
                    .submit(() -> export);
        }

        scheduler.runOne();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);

        // another holder keeps the released export alive, as a lookup racing the release could observe it
        Assert.eqTrue(e1.tryRetainReference(), "e1.tryRetainReference()");
        try {
            e1.release();
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);

            final MutableBoolean errored = new MutableBoolean();
            final MutableBoolean ran = new MutableBoolean();
            final SessionState.ExportObject<?> e2 = session.newExport(nextExportId++)
                    .require(e1)
                    .onErrorHandler(err -> errored.setTrue())
                    .submit(() -> {
                        ran.setTrue();
                        return null;
                    });
            scheduler.runUntilQueueEmpty();
            Assert.eqFalse(ran.booleanValue(), "ran.booleanValue()");
            Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
            Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_RELEASED);
        } finally {
            e1.dropReference();
        }
    }

    @Test
    public void testNewExportRequiresPositiveId() {
        expectException(IllegalArgumentException.class, () -> session.newExport(0));
        expectException(IllegalArgumentException.class, () -> session.newExport(-1));
    }

    @Test
    public void testDependencyAlreadyReleased() {
        final SessionState.ExportObject<Object> e1;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            e1 = session.newExport(nextExportId++).submit(() -> {
            });
            scheduler.runUntilQueueEmpty();
            e1.release();
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        }

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++).require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> {
                });
        Assert.eq(e2.getState(), "e1.getState()", ExportNotification.State.DEPENDENCY_RELEASED);
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testDependencyAlreadyReleasedViaLookup() {
        // Characterization: a dependency resolved by id *after* the export was released must still surface as
        // DEPENDENCY_RELEASED, whether the released export is retained as a shell or reconstructed on lookup.
        final int releasedId = nextExportId++;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final SessionState.ExportObject<Object> e1 = session.newExport(releasedId).submit(() -> {
            });
            scheduler.runUntilQueueEmpty();
            e1.release();
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        }

        // Look the released id back up by ticket rather than holding the original reference.
        final SessionState.ExportObject<Object> lookedUp = session.getExport(releasedId);
        Assert.eq(lookedUp.getState(), "lookedUp.getState()", ExportNotification.State.RELEASED);

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++).require(lookedUp)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> {
                });
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_RELEASED);
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testReexportAtReleasedIdFails() {
        // Characterization: re-defining work at a released id must fail cleanly via the error handler (rather than
        // succeeding or throwing), since the id has been consumed.
        final int releasedId = nextExportId++;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final SessionState.ExportObject<Object> e1 = session.newExport(releasedId).submit(() -> {
            });
            scheduler.runUntilQueueEmpty();
            e1.release();
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        }

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> reexport = session.newExport(releasedId)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> new Object());
        scheduler.runUntilQueueEmpty();
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eqTrue(SessionState.isExportStateTerminal(reexport.getState()), "reexport is terminal");
    }

    /**
     * Most operations declare dependencies, and a released id's placeholder cannot manage anything, so the dependency
     * setup itself must not turn the clean failure into an exception.
     */
    @Test
    public void testReexportAtReleasedIdWithDependencyFails() {
        final int releasedId = nextExportId++;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final SessionState.ExportObject<Object> e1 = session.newExport(releasedId).submit(() -> {
            });
            scheduler.runUntilQueueEmpty();
            e1.release();
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        }
        final SessionState.ExportObject<Object> dependency = session.newExport(nextExportId++).submit(Object::new);
        scheduler.runUntilQueueEmpty();
        Assert.eq(dependency.getState(), "dependency.getState()", ExportNotification.State.EXPORTED);

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean ran = new MutableBoolean();
        final SessionState.ExportObject<Object> reexport = session.newExport(releasedId)
                .require(dependency)
                .onErrorHandler(err -> errored.setTrue())
                .submit(() -> {
                    ran.setTrue();
                    return new Object();
                });
        scheduler.runUntilQueueEmpty();
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(ran.booleanValue(), "ran.booleanValue()");
        Assert.eq(reexport.getState(), "reexport.getState()", ExportNotification.State.RELEASED);
        // the dependency is untouched by the failed re-export
        Assert.eq(dependency.getState(), "dependency.getState()", ExportNotification.State.EXPORTED);
    }

    @Test
    public void testReleasedExportRemovedFromMap() {
        // Leak guard: once an export is released, its id must not retain an ExportObject shell in the export map for
        // the remaining lifetime of the session.
        Assert.eq(session.numExports(), "session.numExports()", 0);

        final int releasedId = nextExportId++;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final SessionState.ExportObject<Object> e1 = session.newExport(releasedId).submit(() -> {
            });
            scheduler.runUntilQueueEmpty();
            Assert.eq(session.numExports(), "session.numExports()", 1);
            e1.release();
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        }

        Assert.eq(session.numExports(), "session.numExports()", 0);
    }

    /**
     * Server-side exports have negative ids, which the released-id set folds into its unsigned key space; a released
     * server-side export must leave the map and still be answered as released, not as never having existed.
     */
    @Test
    public void testReleasedServerSideExportRemovedFromMap() {
        Assert.eq(session.numExports(), "session.numExports()", 0);

        final int serverId;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final SessionState.ExportObject<Object> e1 = session.newServerSideExport(new Object());
            serverId = ticketToExportId(e1.getExportId(), "test");
            Assert.eqTrue(serverId < 0, "serverId < 0");
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);
            Assert.eq(session.numExports(), "session.numExports()", 1);
            e1.release();
            Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        }
        Assert.eq(session.numExports(), "session.numExports()", 0);

        // a server-side id that was never created is a user error, released neighbor or not
        final int bogusId = serverId - 1;
        final StatusRuntimeException bogus =
                assertThrows(StatusRuntimeException.class, () -> session.getExport(bogusId));
        Assert.eq(bogus.getStatus().getCode(), "bogus.getStatus().getCode()", Status.Code.FAILED_PRECONDITION);
        Assert.eqNull(session.getExportIfExists(bogusId), "session.getExportIfExists(bogusId)");

        // one that was released is answered as released
        final SessionState.ExportObject<Object> lookedUp = session.getExport(serverId);
        Assert.eq(lookedUp.getState(), "lookedUp.getState()", ExportNotification.State.RELEASED);
        Assert.eq(ticketToExportId(lookedUp.getExportId(), "test"), "lookedUp id", serverId);
        final SessionState.ExportObject<Object> ifExists = session.getExportIfExists(serverId);
        Assert.neqNull(ifExists, "ifExists");
        Assert.eq(ifExists.getState(), "ifExists.getState()", ExportNotification.State.RELEASED);

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean ran = new MutableBoolean();
        final SessionState.ExportObject<?> late = session.newExport(nextExportId++)
                .require(lookedUp)
                .onErrorHandler(err -> errored.setTrue())
                .submit(ran::setTrue);
        scheduler.runUntilQueueEmpty();
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(ran.booleanValue(), "ran.booleanValue()");
        Assert.eq(late.getState(), "late.getState()", ExportNotification.State.DEPENDENCY_RELEASED);
    }

    /**
     * An export listener is notified while its own monitor is held, and may look exports up from inside the callback
     * (ExportedTableUpdateListener does, for every EXPORTED notification). Meanwhile an export being created notifies
     * the same listeners from inside its constructor, which the export map runs under its own monitor. Looking up an
     * existing export must therefore never take the export map monitor, or the two close a cycle.
     */
    @Test
    public void testExportLookupFromListenerDoesNotDeadlockWithExportCreation() throws InterruptedException {
        final int existingId = nextExportId++;
        final SessionState.ExportObject<Object> existing = session.newExport(existingId).submit(Object::new);
        scheduler.runUntilQueueEmpty();
        Assert.eq(existing.getState(), "existing.getState()", ExportNotification.State.EXPORTED);

        final int completingId = nextExportId++;
        final int createdId = nextExportId++;
        final Thread[] creator = new Thread[1];
        final Thread[] lookup = new Thread[1];
        final Throwable[] failure = new Throwable[1];
        final MutableBoolean lookedUp = new MutableBoolean();
        final StreamObserver<ExportNotification> listener = new StreamObserver<>() {
            @Override
            public void onNext(final ExportNotification notification) {
                if (getExportId(notification) != completingId
                        || notification.getExportState() != ExportNotification.State.EXPORTED) {
                    return;
                }
                // We are inside ExportListener.notify, holding this listener's monitor. Create another export on a
                // second thread: it takes the export map monitor and then blocks on our monitor to notify us. Ours is
                // the only monitor it can block on, since this thread holds nothing else.
                creator[0] = new Thread(() -> session.newExport(createdId), "SessionStateTest-creator");
                creator[0].start();
                final long deadlineNanos = System.nanoTime() + 10_000_000_000L;
                while (creator[0].getState() != Thread.State.BLOCKED) {
                    if (!creator[0].isAlive() || System.nanoTime() > deadlineNanos) {
                        failure[0] = new IllegalStateException("creating an export did not block to notify us");
                        return;
                    }
                    Thread.onSpinWait();
                }
                // Now look up the existing export, as ExportedTableUpdateListener would; it must not need the map.
                lookup[0] = new Thread(() -> {
                    try {
                        session.getExport(existingId);
                        lookedUp.setTrue();
                    } catch (final Throwable t) {
                        failure[0] = t;
                    }
                }, "SessionStateTest-lookup");
                lookup[0].start();
                try {
                    lookup[0].join(5_000);
                } catch (final InterruptedException e) {
                    failure[0] = e;
                }
                if (lookup[0].isAlive()) {
                    failure[0] = new IllegalStateException(
                            "looking up an existing export from an export listener blocked on the export map");
                }
                // returning releases our monitor, which lets the creator (and with it any blocked lookup) finish
            }

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {}
        };
        session.addExportListener(listener);

        session.newExport(completingId).submit(Object::new);
        // completes on this thread, notifying the listener from under the completing export's monitor
        scheduler.runUntilQueueEmpty();

        Assert.neqNull(creator[0], "creator[0]");
        creator[0].join(5_000);
        if (lookup[0] != null) {
            lookup[0].join(5_000);
        }
        if (failure[0] != null) {
            throw new AssertionFailure("export lookup from a listener must not deadlock with export creation",
                    failure[0]);
        }
        Assert.eqTrue(lookedUp.booleanValue(), "lookedUp.booleanValue()");
        Assert.eqFalse(creator[0].isAlive(), "creator[0].isAlive()");
        Assert.neqNull(session.getExportIfExists(createdId), "session.getExportIfExists(createdId)");
    }

    /**
     * A listener's initial refresh notifies it while holding each export's monitor, and a listener may create exports
     * from its callback (see {@link #testExportListenerNewExportAtRefreshTail}), which needs the export map monitor. A
     * concurrent release of that same export must therefore not hold the export map monitor while it waits for the
     * export's monitor, or the two deadlock.
     */
    @Test
    public void testReleaseDoesNotDeadlockWithListenerCreatingExportsDuringRefresh() throws InterruptedException {
        final int existingId = nextExportId++;
        final SessionState.ExportObject<Object> existing = session.newExport(existingId).submit(Object::new);
        scheduler.runUntilQueueEmpty();
        Assert.eq(existing.getState(), "existing.getState()", ExportNotification.State.EXPORTED);

        final int createdId = nextExportId++;
        final Thread[] releaser = new Thread[1];
        final Throwable[] failure = new Throwable[1];
        final StreamObserver<ExportNotification> listener = new StreamObserver<>() {
            @Override
            public void onNext(final ExportNotification notification) {
                if (getExportId(notification) != existingId || releaser[0] != null
                        || notification.getExportState() != ExportNotification.State.EXPORTED) {
                    // only the refresh's notification of the still-exported export; not the release's own later one
                    return;
                }
                // We are inside the refresh, holding the export's monitor. Release the export from another thread; it
                // blocks on our monitor, and must not be holding the export map monitor while it does.
                releaser[0] = new Thread(existing::release, "SessionStateTest-releaser");
                releaser[0].start();
                final long deadlineNanos = System.nanoTime() + 10_000_000_000L;
                while (releaser[0].getState() != Thread.State.BLOCKED) {
                    if (!releaser[0].isAlive() || System.nanoTime() > deadlineNanos) {
                        failure[0] = new IllegalStateException("releasing the export did not block on its monitor");
                        return;
                    }
                    Thread.onSpinWait();
                }
                // Inspect what the blocked releaser holds, rather than trying to take the export map ourselves: if it
                // holds the map, creating an export here would deadlock instead of failing.
                final ThreadInfo info = ManagementFactory.getThreadMXBean()
                        .getThreadInfo(new long[] {releaser[0].getId()}, true, false)[0];
                for (final MonitorInfo held : info.getLockedMonitors()) {
                    if (held.getClassName().equals(KeyedIntObjectHashMap.class.getName())) {
                        failure[0] = new IllegalStateException(
                                "a release holds the export map monitor while waiting on the export's monitor");
                        return;
                    }
                }
                // The releaser holds nothing, so a listener may create an export from its callback, as one does in
                // testExportListenerNewExportAtRefreshTail.
                session.newExport(createdId);
                // returning releases the export's monitor, which lets the releaser finish
            }

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {}
        };
        session.addExportListener(listener);

        Assert.neqNull(releaser[0], "releaser[0]");
        releaser[0].join(5_000);
        if (failure[0] != null) {
            throw new AssertionFailure("release must not deadlock with a listener creating exports", failure[0]);
        }
        Assert.eqFalse(releaser[0].isAlive(), "releaser[0].isAlive()");
        Assert.eq(existing.getState(), "existing.getState()", ExportNotification.State.RELEASED);
        Assert.neqNull(session.getExportIfExists(createdId), "session.getExportIfExists(createdId)");
        // the release still dropped the shell: only the created export remains
        Assert.eq(session.numExports(), "session.numExports()", 1);
    }

    /**
     * Starts {@code action} on a new thread and waits for it to block on a monitor this thread holds. Records a failure
     * if it never blocks, or if it holds the export map monitor while blocked; in that case the caller must not take
     * the export map itself, which would deadlock rather than fail.
     */
    private static Thread startThreadBlockedOnCurrentThread(
            final Runnable action, final String name, final Throwable[] failure) {
        final Thread thread = new Thread(action, name);
        thread.start();
        final long currentThreadId = Thread.currentThread().getId();
        final long deadlineNanos = System.nanoTime() + 10_000_000_000L;
        while (true) {
            if (!thread.isAlive()) {
                failure[0] =
                        new IllegalStateException(name + " finished without blocking on a monitor held by this thread");
                return thread;
            }
            if (System.nanoTime() > deadlineNanos) {
                failure[0] = new IllegalStateException(name + " did not block on a monitor held by this thread");
                return thread;
            }
            if (thread.getState() != Thread.State.BLOCKED) {
                Thread.onSpinWait();
                continue;
            }
            final ThreadInfo info = ManagementFactory.getThreadMXBean()
                    .getThreadInfo(new long[] {thread.getId()}, true, false)[0];
            if (info == null || info.getThreadState() != Thread.State.BLOCKED
                    || info.getLockOwnerId() != currentThreadId) {
                continue;
            }
            for (final MonitorInfo held : info.getLockedMonitors()) {
                if (held.getClassName().equals(KeyedIntObjectHashMap.class.getName())) {
                    failure[0] = new IllegalStateException(
                            name + " holds the export map monitor while waiting on a monitor held by this thread");
                }
            }
            return thread;
        }
    }

    @Test
    public void testExportCreationDoesNotHoldExportMapWhileNotifyingListeners() throws InterruptedException {
        final int createdId = nextExportId++;
        final int createdFromCallbackId = nextExportId++;
        final Thread[] creator = new Thread[1];
        final Throwable[] failure = new Throwable[1];
        final StreamObserver<ExportNotification> listener = new StreamObserver<>() {
            @Override
            public void onNext(final ExportNotification notification) {
                if (getExportId(notification) != SessionState.NON_EXPORT_ID || creator[0] != null) {
                    // act once, on the refresh's completion notification, where only the listener monitor is held
                    return;
                }
                // Create an export from another thread. Its first notification blocks on our listener monitor, and it
                // must not be holding the export map monitor while it does.
                creator[0] = startThreadBlockedOnCurrentThread(
                        () -> session.newExport(createdId), "SessionStateTest-creator", failure);
                if (failure[0] != null) {
                    return;
                }
                // The creator holds its own export's monitor but not the map, so a listener may create an export.
                session.newExport(createdFromCallbackId);
                // returning releases the listener monitor, which lets the creator finish
            }

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {}
        };
        session.addExportListener(listener);

        Assert.neqNull(creator[0], "creator[0]");
        creator[0].join(5_000);
        if (failure[0] != null) {
            throw new AssertionFailure("creating an export must not deadlock with a listener creating exports",
                    failure[0]);
        }
        Assert.eqFalse(creator[0].isAlive(), "creator[0].isAlive()");
        Assert.neqNull(session.getExportIfExists(createdId), "session.getExportIfExists(createdId)");
        Assert.neqNull(session.getExportIfExists(createdFromCallbackId),
                "session.getExportIfExists(createdFromCallbackId)");
    }

    @Test
    public void testSessionExpiryDoesNotHoldExportMapWhileCancellingExports() throws InterruptedException {
        final SessionState.ExportObject<Object> existing = session.newExport(nextExportId++).submit(Object::new);
        scheduler.runUntilQueueEmpty();
        Assert.eq(existing.getState(), "existing.getState()", ExportNotification.State.EXPORTED);

        final int createdId = nextExportId++;
        final Thread[] expirer = new Thread[1];
        final Throwable[] failure = new Throwable[1];
        final StreamObserver<ExportNotification> listener = new StreamObserver<>() {
            @Override
            public void onNext(final ExportNotification notification) {
                if (getExportId(notification) != SessionState.NON_EXPORT_ID || expirer[0] != null) {
                    // act once, on the refresh's completion notification, where only the listener monitor is held
                    return;
                }
                // Expire the session from another thread. Cancelling the existing export notifies us and so blocks on
                // our listener monitor; the expiry must not be holding the export map monitor while it does.
                expirer[0] = startThreadBlockedOnCurrentThread(session::onExpired, "SessionStateTest-expirer", failure);
                if (failure[0] != null) {
                    return;
                }
                // The session has already expired, so a listener creating an export from its callback is told so
                // rather than left waiting on the expiry.
                try {
                    session.newExport(createdId);
                    failure[0] = new IllegalStateException("creating an export on an expired session did not fail");
                } catch (final StatusRuntimeException err) {
                    if (err.getStatus().getCode() != Status.Code.UNAUTHENTICATED) {
                        failure[0] = err;
                    }
                }
                // returning releases the listener monitor, which lets the expiry finish
            }

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {}
        };
        session.addExportListener(listener);

        Assert.neqNull(expirer[0], "expirer[0]");
        expirer[0].join(5_000);
        if (failure[0] != null) {
            throw new AssertionFailure("session expiry must not deadlock with a listener creating exports",
                    failure[0]);
        }
        Assert.eqFalse(expirer[0].isAlive(), "expirer[0].isAlive()");
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        // cancelling an exported export releases it
        Assert.eq(existing.getState(), "existing.getState()", ExportNotification.State.RELEASED);
    }

    /**
     * A listener callback may create exports, and a cancel in progress holds its export's monitor while it notifies
     * listeners. Creating an export must therefore never take the monitor of an export that is already published, or a
     * callback creating one would deadlock with that cancel. Pinned at an id a client referenced out of order, so the
     * export already exists when its definition arrives. The creator runs on its own thread in place of the callback,
     * so that a creator that needs the cancelled export's monitor is observed blocked rather than deadlocking the test.
     */
    @Test
    public void testExportCreationDoesNotTakePublishedExportMonitor() throws InterruptedException {
        final int outOfOrderId = nextExportId++;
        final SessionState.ExportObject<Object> found = session.getExport(outOfOrderId);
        Assert.eq(found.getState(), "found.getState()", ExportNotification.State.UNKNOWN);

        final Thread[] canceller = new Thread[1];
        final Throwable[] failure = new Throwable[1];
        final StreamObserver<ExportNotification> listener = new StreamObserver<>() {
            @Override
            public void onNext(final ExportNotification notification) {
                if (getExportId(notification) != SessionState.NON_EXPORT_ID || canceller[0] != null) {
                    // act once, on the refresh's completion notification, where only the listener monitor is held
                    return;
                }
                // Cancel the out-of-order export from another thread; it holds that export's monitor and blocks on
                // our listener monitor.
                canceller[0] = startThreadBlockedOnCurrentThread(found::cancel, "SessionStateTest-canceller", failure);
                if (failure[0] != null) {
                    return;
                }
                // Now the export's definition arrives, as it could from this callback.
                final Thread creator = new Thread(() -> session.newExport(outOfOrderId), "SessionStateTest-creator");
                creator.start();
                try {
                    creator.join(2_000);
                } catch (final InterruptedException e) {
                    failure[0] = e;
                    return;
                }
                if (creator.isAlive()) {
                    failure[0] = new IllegalStateException(
                            "defining an export at an out-of-order id blocked on that export's monitor");
                }
                // returning releases the listener monitor, which lets the canceller, and then the creator, finish
            }

            @Override
            public void onError(final Throwable t) {}

            @Override
            public void onCompleted() {}
        };
        session.addExportListener(listener);

        Assert.neqNull(canceller[0], "canceller[0]");
        canceller[0].join(5_000);
        if (failure[0] != null) {
            throw new AssertionFailure("creating an export must not take a published export's monitor", failure[0]);
        }
        Assert.eqFalse(canceller[0].isAlive(), "canceller[0].isAlive()");
        Assert.eq(found.getState(), "found.getState()", ExportNotification.State.CANCELLED);
    }

    @Test
    public void testExpiredNewExport() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(Object::new);
        scheduler.runUntilQueueEmpty();
        session.onExpired();
        expectException(StatusRuntimeException.class, exportObj::get);
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testExpiredNewNonExport() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.nonExport()
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(Object::new);
        scheduler.runUntilQueueEmpty();
        session.onExpired();
        expectException(StatusRuntimeException.class, exportObj::get);
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testExpiredServerSideExport() {
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj = session.newServerSideExport(export);
        session.onExpired();
        expectException(StatusRuntimeException.class, exportObj::get);
    }

    @Test
    public void testExpiresBeforeExport() {
        session.onExpired();
        expectException(StatusRuntimeException.class, () -> session.newServerSideExport(new Object()));
        expectException(StatusRuntimeException.class, () -> session.nonExport());
        expectException(StatusRuntimeException.class, () -> session.newExport(nextExportId++));
        expectException(StatusRuntimeException.class, () -> session.getExport(nextExportId++));
    }

    @Test
    public void testExpireBeforeNonExportSubmit() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportBuilder<Object> exportBuilder = session.nonExport();
        session.onExpired();
        exportBuilder
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testExpireBeforeExportSubmit() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportBuilder<Object> exportBuilder = session.newExport(nextExportId++);
        session.onExpired();
        exportBuilder
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testExpireDuringExport() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final CountingLivenessReferent export = new CountingLivenessReferent();
        session.newExport(nextExportId++)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> {
                    session.onExpired();
                    return export;
                });
        scheduler.runUntilQueueEmpty();
        Assert.eq(export.refCount, "export.refCount", 0);
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testDependencyFailed() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        session.newExport(e1.getExportId(), "test").submit(() -> {
            throw new RuntimeException();
        });
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_FAILED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testDependencyAlreadyFailed() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(() -> {
            throw new RuntimeException();
        });
        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.FAILED);
        expectException(IllegalStateException.class, e1::get);

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_FAILED);
        expectException(IllegalStateException.class, e2::get);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testDependencyAlreadyCanceled() {
        final SessionState.ExportObject<Object> e1 = session.getExport(nextExportId++);
        e1.cancel();
        scheduler.runUntilQueueEmpty();

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_CANCELLED); // cancels propagate
        expectException(IllegalStateException.class, e2::get);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testDependencyAlreadyExported() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(() -> {
        });
        scheduler.runUntilQueueEmpty();

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.QUEUED);
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.EXPORTED);
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testDependencyReleasedBeforeExport() {
        final CountingLivenessReferent e1 = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> e1obj;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            e1obj = session.newExport(nextExportId++).submit(() -> e1);
        }
        scheduler.runUntilQueueEmpty();

        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e2obj = session.newExport(nextExportId++)
                .require(e1obj)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(() -> {
                    submitted.setTrue();
                    Assert.neqNull(e1obj.get(), "e1obj.get()");
                    Assert.gt(e1.refCount, "e1.refCount", 0);
                });

        e1obj.release();
        Assert.eq(e1obj.getState(), "e1obj.getState()", ExportNotification.State.RELEASED);

        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.refCount, "e1.refCount", 0);
        Assert.eq(e2obj.getState(), "e2obj.getState()", ExportNotification.State.EXPORTED);
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testChildCancelledFirst() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(() -> {
        });
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++).require(e1)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        e2.cancel();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.CANCELLED);
        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.CANCELLED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    public void testCannotOutOfOrderServerExports() {
        // server-side exports must already exist
        expectException(StatusRuntimeException.class, () -> session.getExport(-1));
    }

    @Test
    public void testGetExpiration() {
        final SessionService.TokenExpiration expiration = session.getExpiration();
        Assert.eq(expiration.session, "expiration.session", session, "session");
        session.onExpired();
        Assert.eqNull(session.getExpiration(), "session.getExpiration()");
    }

    @Test
    public void testExpiredByTime() {
        session.updateExpiration(
                new SessionService.TokenExpiration(UUID.randomUUID(), scheduler.currentTimeMillis(), session));
        Assert.eqNull(session.getExpiration(), "session.getExpiration()"); // already expired
        expectException(StatusRuntimeException.class, () -> session.newServerSideExport(new Object()));
        expectException(StatusRuntimeException.class, () -> session.nonExport());
        expectException(StatusRuntimeException.class, () -> session.newExport(nextExportId++));
        expectException(StatusRuntimeException.class, () -> session.getExport(nextExportId++));
    }

    @Test
    public void testGetAuthContext() {
        Assert.eq(session.getAuthContext(), "session.getAuthContext()", AUTH_CONTEXT, "AUTH_CONTEXT");
    }

    @Test
    public void testReleaseIsNotProactive() {
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++)
                .onErrorHandler(err -> errored.setTrue())
                .onSuccess(success::setTrue)
                .submit(submitted::setTrue);
        e1.release();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.QUEUED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqTrue(success.booleanValue(), "success.booleanValue()");
    }

    @Test
    @Ignore // TODO (core#33)
    public void testWorkItemDirectCycle() {
        final SessionState.ExportObject<Object> e1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> e2 = session.getExport(nextExportId++);
        session.newExport(e1.getExportId(), "test").require(e2).submit(() -> {
        });
        session.newExport(e2.getExportId(), "test").require(e1).submit(() -> {
        });
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.FAILED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.FAILED);
    }

    @Test
    @Ignore // TODO (core#33)
    public void testWorkItemNonTrivialCycle() {
        final SessionState.ExportObject<Object> e1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> e2 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> e3 = session.getExport(nextExportId++);
        session.newExport(e1.getExportId(), "test").require(e2).submit(() -> {
        });
        session.newExport(e2.getExportId(), "test").require(e3).submit(() -> {
        });
        session.newExport(e3.getExportId(), "test").require(e1).submit(() -> {
        });
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.FAILED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.FAILED);
        Assert.eq(e3.getState(), "e3.getState()", ExportNotification.State.FAILED);
    }

    @Test
    @Ignore // TODO (core#33)
    public void testCycleErrorPropagates() {
        final SessionState.ExportObject<Object> e1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> e2 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> e3 = session.newExport(nextExportId++).require(e1, e2).submit(() -> {
        });
        session.newExport(e1.getExportId(), "test").require(e2).submit(() -> {
        });
        session.newExport(e2.getExportId(), "test").require(e1).submit(() -> {
        });
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.FAILED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.FAILED);
        Assert.eq(e3.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_FAILED);
    }

    @Test
    @Ignore // TODO (core#33)
    public void testNonExportCycle() {
        final SessionState.ExportBuilder<Object> b1 = session.nonExport();
        final SessionState.ExportBuilder<Object> b2 = session.nonExport();
        final SessionState.ExportBuilder<Object> b3 = session.nonExport();
        b1.require(b2.getExport()).submit(() -> {
        });
        b2.require(b3.getExport()).submit(() -> {
        });
        b3.require(b1.getExport()).submit(() -> {
        });
        Assert.eq(b1.getExport().getState(), "b1.getExport().getState()", ExportNotification.State.FAILED);
        Assert.eq(b2.getExport().getState(), "b2.getExport().getState()", ExportNotification.State.FAILED);
        Assert.eq(b3.getExport().getState(), "b3.getExport().getState()", ExportNotification.State.FAILED);
    }

    @Test
    public void testExportListenerOnCompleteOnRemoval() {
        final QueueingExportListener listener = new QueueingExportListener();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        Assert.eqFalse(listener.isComplete, "listener.isComplete");
        session.removeExportListener(listener);
        Assert.eqTrue(listener.isComplete, "listener.isComplete");
    }

    @Test
    public void testExportListenerOnCompleteOnSessionExpire() {
        final QueueingExportListener listener = new QueueingExportListener();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            session.addExportListener(listener);
        }
        Assert.eqFalse(listener.isComplete, "listener.isComplete");
        session.onExpired();
        Assert.eqTrue(listener.isComplete, "listener.isComplete");
    }

    @Test
    public void testThrowingOnCloseCallbackOnExpiryIsFatal() {
        final CountingLivenessReferent export = new CountingLivenessReferent();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            session.newServerSideExport(export);
        }
        Assert.eq(export.refCount, "export.refCount", 1);
        // the session holds close callbacks weakly; keep ours reachable, and collect now so a dropped reference fails
        // here rather than only under GC pressure
        final Closeable onClose = () -> {
            throw new IOException("close failed");
        };
        session.addOnCloseCallback(onClose);
        System.gc();

        boolean fatal = false;
        try {
            session.onExpired();
        } catch (final FakeProcessEnvironment.FakeFatalException expected) {
            fatal = true;
        }
        Assert.eqTrue(fatal, "fatal");
        // the exports were already torn down; the session is expired either way
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        Assert.eq(export.refCount, "export.refCount", 0);
        Reference.reachabilityFence(onClose);
    }

    @Test
    public void textExportListenerNoExports() {
        final QueueingExportListener listener = new QueueingExportListener();
        session.addExportListener(listener);
        Assert.eq(listener.notifications.size(), "notifications.size()", 1);
        final ExportNotification refreshComplete = listener.notifications.get(listener.notifications.size() - 1);
        Assert.eq(ticketToExportId(refreshComplete.getTicket(), "test"), "refreshComplete.getTicket()",
                SessionState.NON_EXPORT_ID, "SessionState.NON_EXPORT_ID");
    }

    @Test
    public void textExportListenerOneExport() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportObject<SessionState> e1 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);
        scheduler.runUntilQueueEmpty();
        session.addExportListener(listener);
        listener.validateNotificationQueue(e1, EXPORTED);

        // ensure export was from run
        Assert.eq(listener.notifications.size(), "notifications.size()", 2);
        final ExportNotification refreshComplete = listener.notifications.get(1);
        Assert.eq(ticketToExportId(refreshComplete.getTicket(), "test"), "lastNotification.getTicket()",
                SessionState.NON_EXPORT_ID, "SessionState.NON_EXPORT_ID");
    }

    @Test
    public void textExportListenerAddHeadDuringRefreshComplete() {
        final MutableObject<SessionState.ExportObject<SessionState>> e1 = new MutableObject<>();
        final QueueingExportListener listener = new QueueingExportListener() {
            @Override
            public void onNext(final ExportNotification n) {
                if (ticketToExportId(n.getTicket(), "test") != SessionState.NON_EXPORT_ID) {
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
        final SessionState.ExportObject<SessionState> e1 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);
        scheduler.runUntilQueueEmpty();
        Assert.eq(listener.notifications.size(), "notifications.size()", 6);
        listener.validateIsRefreshComplete(0);
        listener.validateNotificationQueue(e1, UNKNOWN, PENDING, QUEUED, RUNNING, EXPORTED);
    }

    @Test
    public void testExportListenerInterestingRefresh() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportObject<SessionState> e1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<SessionState> e4 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session); // exported
        final SessionState.ExportObject<SessionState> e5 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> e7 =
                session.<SessionState>newExport(nextExportId++).submit(() -> {
                    throw new RuntimeException();
                }); // failed
        final SessionState.ExportObject<SessionState> e8 =
                session.<SessionState>newExport(nextExportId++).require(e7).submit(() -> session); // dependency failed
        scheduler.runUntilQueueEmpty();
        e5.release(); // released

        final SessionState.ExportObject<SessionState> e6 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);
        e6.cancel();

        final SessionState.ExportObject<SessionState> e3 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session); // queued
        final SessionState.ExportObject<SessionState> e2 =
                session.<SessionState>newExport(nextExportId++).require(e3).submit(() -> session); // pending

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

        final SessionState.ExportObject<SessionState> e1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<SessionState> e4 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session); // exported
        final SessionState.ExportObject<SessionState> e5 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> e7 =
                session.<SessionState>newExport(nextExportId++).submit(() -> {
                    throw new RuntimeException();
                }); // failed
        final SessionState.ExportObject<SessionState> e8 =
                session.<SessionState>newExport(nextExportId++).require(e7).submit(() -> session); // dependency failed
        scheduler.runUntilQueueEmpty();
        e5.release(); // released

        final SessionState.ExportObject<SessionState> e6 = session.<SessionState>newExport(nextExportId++).getExport();
        e6.cancel();

        final SessionState.ExportObject<SessionState> e3 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session); // queued
        final SessionState.ExportObject<SessionState> e2 =
                session.<SessionState>newExport(nextExportId++).require(e3).submit(() -> session); // pending

        listener.validateIsRefreshComplete(0);
        listener.validateNotificationQueue(e1, UNKNOWN);
        listener.validateNotificationQueue(e2, UNKNOWN, PENDING);
        listener.validateNotificationQueue(e3, UNKNOWN, PENDING, QUEUED);
        listener.validateNotificationQueue(e4, UNKNOWN, PENDING, QUEUED, RUNNING, EXPORTED);
        listener.validateNotificationQueue(e5, UNKNOWN, PENDING, QUEUED, RUNNING, EXPORTED, RELEASED);
        listener.validateNotificationQueue(e6, UNKNOWN, CANCELLED);
        listener.validateNotificationQueue(e7, UNKNOWN, PENDING, QUEUED, RUNNING, FAILED);
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
        listener.validateNotificationQueue(b2, PENDING, QUEUED); // PENDING is optional/racy w.r.t. spec
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
        listener.validateNotificationQueue(b2, UNKNOWN, PENDING, QUEUED); // PENDING is optional/racy w.r.t. spec
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
        listener.validateIsRefreshComplete(5); // note that we receive run complete after receiving updates to b2
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
    public void testExportListenerTerminalBeforeListenerAdd() {
        final SessionState.ExportBuilder<SessionState> b1 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b2 = session.newExport(nextExportId++);
        final SessionState.ExportBuilder<SessionState> b3 = session.newExport(nextExportId++);
        b2.getExport().cancel();

        final QueueingExportListener listener = new QueueingExportListener();

        session.addExportListener(listener);
        listener.validateIsRefreshComplete(-1);
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2);
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
        listener.validateNotificationQueue(b2, CANCELLED); // CANCELLED is optional/racy w.r.t. spec
        listener.validateNotificationQueue(b3, UNKNOWN);
    }

    @Test
    public void testExportListenerTerminalDuringSeqSent() {
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
        listener.validateIsRefreshComplete(4); // note we receive run complete after the update to b2
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
        listener.validateIsRefreshComplete(4); // new export occurs prior to run completing
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
        listener.validateIsRefreshComplete(3); // run completes, then we see new export
        listener.validateNotificationQueue(b1, UNKNOWN);
        listener.validateNotificationQueue(b2, UNKNOWN);
        listener.validateNotificationQueue(b3, UNKNOWN);
        listener.validateNotificationQueue(b4.getValue(), UNKNOWN);
    }

    @Test
    public void testExportListenerNewExportAfterRefreshComplete() {
        final QueueingExportListener listener = new QueueingExportListener();
        final SessionState.ExportObject<SessionState> b1 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> b2 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);
        final SessionState.ExportObject<SessionState> b3 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);

        session.addExportListener(listener);
        final SessionState.ExportObject<SessionState> b4 =
                session.<SessionState>newExport(nextExportId++).submit(() -> session);

        // for fun we'll flush after run
        scheduler.runUntilQueueEmpty();

        listener.validateIsRefreshComplete(3);
        listener.validateNotificationQueue(b1, QUEUED, RUNNING, EXPORTED);
        listener.validateNotificationQueue(b2, QUEUED, RUNNING, EXPORTED);
        listener.validateNotificationQueue(b3, QUEUED, RUNNING, EXPORTED);
        listener.validateNotificationQueue(b4, UNKNOWN, PENDING, QUEUED, RUNNING, EXPORTED);
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

    @Test
    public void testNonExportWithDependencyFails() {
        final SessionState.ExportObject<Object> e1 =
                session.newExport(nextExportId++).submit(() -> session);
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<Object> n1 =
                session.nonExport()
                        .require(e1)
                        .onErrorHandler(err -> errored.setTrue())
                        .onSuccess(success::setTrue)
                        .submit(() -> {
                            throw new RuntimeException("this should not reach test framework");
                        });
        scheduler.runUntilQueueEmpty();
        Assert.eq(n1.getState(), "n1.getState()", FAILED, "FAILED");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
    }

    @Test
    public void testNonExportWithDependencyReleaseOnExport() {
        final CountingLivenessReferent clr = new CountingLivenessReferent();

        final SessionState.ExportObject<Object> e1;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            e1 = session.newExport(nextExportId++).submit(() -> clr);
        }
        final SessionState.ExportBuilder<Object> n1 = session.nonExport().require(e1);
        e1.release();
        scheduler.runUntilQueueEmpty();
        // should retain it still for the builder
        Assert.gt(clr.refCount, "clr.refCount", 0);

        n1.submit(() -> {
        });
        scheduler.runUntilQueueEmpty();
        Assert.eq(clr.refCount, "clr.refCount", 0);
    }

    @Test
    public void testCascadingStatusRuntimeFailureDeliversToErrorHandler() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++)
                .submit(() -> {
                    throw Status.DATA_LOSS.asRuntimeException();
                });

        final MutableBoolean submitRan = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableObject<Throwable> caughtErr = new MutableObject<>();
        final StreamObserver<?> observer = new StreamObserver<>() {
            @Override
            public void onNext(Object value) {
                throw new RuntimeException("this should not reach test framework");
            }

            @Override
            public void onError(Throwable t) {
                caughtErr.setValue(t);
            }

            @Override
            public void onCompleted() {
                throw new RuntimeException("this should not reach test framework");
            }
        };
        session.newExport(nextExportId++)
                .onError(observer)
                .onSuccess(success::setTrue)
                .require(e1)
                .submit(submitRan::setTrue);

        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(submitRan.booleanValue(), "submitRan.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eqTrue(caughtErr.getValue() instanceof StatusRuntimeException,
                "caughtErr.getValue() instanceof StatusRuntimeException");

        final StatusRuntimeException sre = (StatusRuntimeException) caughtErr.getValue();
        Assert.eq(sre.getStatus(), "sre.getStatus()", Status.DATA_LOSS, "Status.DATA_LOSS");
    }

    @Test
    public void testCascadingStatusRuntimeFailureDeliversToErrorHandlerAlreadyFailed() {
        final SessionState.ExportObject<Object> e1 = SessionState.wrapAsFailedExport(
                Status.DATA_LOSS.asRuntimeException());

        final MutableBoolean submitRan = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final MutableObject<Throwable> caughtErr = new MutableObject<>();
        final StreamObserver<?> observer = new StreamObserver<>() {
            @Override
            public void onNext(Object value) {
                throw new RuntimeException("this should not reach test framework");
            }

            @Override
            public void onError(Throwable t) {
                caughtErr.setValue(t);
            }

            @Override
            public void onCompleted() {
                throw new RuntimeException("this should not reach test framework");
            }
        };
        session.newExport(nextExportId++)
                .onError(observer)
                .onSuccess(success::setTrue)
                .require(e1)
                .submit(submitRan::setTrue);

        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(submitRan.booleanValue(), "submitRan.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eqTrue(caughtErr.getValue() instanceof StatusRuntimeException,
                "caughtErr.getValue() instanceof StatusRuntimeException");

        final StatusRuntimeException sre = (StatusRuntimeException) caughtErr.getValue();
        Assert.eq(sre.getStatus(), "sre.getStatus()", Status.DATA_LOSS, "Status.DATA_LOSS");
    }

    @Test
    public void testDestroyedExportObjectDependencyFailsNotThrows() {
        final SessionState.ExportObject<?> failedExport;
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            failedExport = SessionState.wrapAsFailedExport(new RuntimeException());
        }
        Assert.eqFalse(failedExport.tryIncrementReferenceCount(), "failedExport.tryIncrementReferenceCount()");
        final MutableBoolean errored = new MutableBoolean();
        final MutableBoolean success = new MutableBoolean();
        final SessionState.ExportObject<?> result =
                session.newExport(nextExportId++)
                        .onErrorHandler(err -> errored.setTrue())
                        .onSuccess(success::setTrue)
                        .require(failedExport)
                        .submit(failedExport::get);
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eqFalse(success.booleanValue(), "success.booleanValue()");
        Assert.eq(result.getState(), "result.getState()", DEPENDENCY_FAILED);
    }

    // region DH-23571 / DH-23573: racing teardown of a queued parent export and its dependent

    /**
     * The participants in a racing teardown: a queued parent that depends on an exported grand-parent, and a child that
     * depends on the parent.
     */
    private final class TeardownRace {
        final CountingLivenessReferent grandParentResult = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> grandParent;
        final SessionState.ExportObject<Object> parent;
        final SessionState.ExportObject<Object> child;
        final MutableBoolean parentRan = new MutableBoolean();
        final AtomicInteger parentErrors = new AtomicInteger();
        final AtomicInteger childErrors = new AtomicInteger();

        TeardownRace(final boolean childIsExport) {
            // a throw-away scope, so that only the exports (and their dependents) keep each other alive
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                grandParent = session.newServerSideExport(grandParentResult);
                parent = session.<Object>newExport(nextExportId++)
                        .require(grandParent)
                        .onError((state, errorContext, cause, dependentId) -> parentErrors.incrementAndGet())
                        .submit(parentRan::setTrue);
                final SessionState.ExportBuilder<Object> childBuilder =
                        childIsExport ? session.newExport(nextExportId++) : session.nonExport();
                child = childBuilder
                        .require(parent)
                        .onError((state, errorContext, cause, dependentId) -> childErrors.incrementAndGet())
                        .submit(() -> {
                        });
            }
            // the parent's work is scheduled but has not run; the child is waiting on the parent
            Assert.eq(parent.getState(), "parent.getState()", QUEUED);
            Assert.eq(child.getState(), "child.getState()", PENDING);
            Assert.eq(grandParentResult.refCount, "grandParentResult.refCount", 1);
        }
    }

    /**
     * Runs {@code parentTeardown} on another thread and cancels the child once that thread is blocked on the child's
     * monitor, landing the cancellation between propagation's unlocked state read and its locked state change.
     *
     * @return the throwable that escaped {@code parentTeardown}, or null if it completed normally
     */
    private static Throwable cancelChildDuringTeardown(
            final SessionState.ExportObject<?> child,
            final Runnable parentTeardown) throws InterruptedException {
        final AtomicReference<Throwable> escaped = new AtomicReference<>();
        final Thread teardownThread = new Thread(() -> {
            try {
                parentTeardown.run();
            } catch (final Throwable t) {
                escaped.set(t);
            }
        }, "SessionStateTest-teardown");
        synchronized (child) {
            teardownThread.start();
            awaitBlockedOnMonitorHeldByCurrentThread(teardownThread);
            child.cancel();
        }
        teardownThread.join(TimeUnit.SECONDS.toMillis(30));
        Assert.eqFalse(teardownThread.isAlive(), "teardownThread.isAlive()");
        return escaped.get();
    }

    private static void awaitBlockedOnMonitorHeldByCurrentThread(final Thread thread) {
        final ThreadMXBean threads = ManagementFactory.getThreadMXBean();
        final long currentThreadId = Thread.currentThread().getId();
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (thread.isAlive()) {
            final ThreadInfo info = threads.getThreadInfo(thread.getId());
            if (info != null && info.getThreadState() == Thread.State.BLOCKED
                    && info.getLockOwnerId() == currentThreadId) {
                return;
            }
            if (System.nanoTime() > deadlineNanos) {
                throw new IllegalStateException("thread never blocked on a monitor held by this thread");
            }
            Thread.onSpinWait();
        }
        throw new IllegalStateException("thread finished without contending for the child's monitor");
    }

    @Test
    public void testParentCancelRacingChildCancelTearsDownParent() throws InterruptedException {
        final TeardownRace race = new TeardownRace(true);

        final Throwable escaped = cancelChildDuringTeardown(race.child, race.parent::cancel);
        if (escaped != null) {
            throw new AssertionFailure("cancelling the parent must not throw when its child is concurrently cancelled",
                    escaped);
        }
        Assert.eq(race.parent.getState(), "race.parent.getState()", CANCELLED);
        Assert.eq(race.child.getState(), "race.child.getState()", CANCELLED);
        Assert.eq(race.parentErrors.get(), "race.parentErrors.get()", 1);
        Assert.eq(race.childErrors.get(), "race.childErrors.get()", 1);

        // a torn down parent no longer manages its dependency, so releasing the grand-parent frees its result
        race.grandParent.release();
        Assert.eq(race.grandParentResult.refCount, "race.grandParentResult.refCount", 0);

        // the parent's queued work must be a harmless no-op
        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(race.parentRan.booleanValue(), "race.parentRan.booleanValue()");
        Assert.eq(race.parent.getState(), "race.parent.getState()", CANCELLED);
    }

    @Test
    public void testSessionExpiryRacingChildCancelReleasesEverything() throws InterruptedException {
        // a non-export child is not in the export map, so the expiry sweep reaches it only through the parent's
        // failure propagation
        final TeardownRace race = new TeardownRace(false);
        final CountingLivenessReferent unrelatedResult = new CountingLivenessReferent();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            session.newServerSideExport(unrelatedResult);
        }
        Assert.eq(unrelatedResult.refCount, "unrelatedResult.refCount", 1);
        final MutableBoolean onCloseInvoked = new MutableBoolean();
        // the session holds close callbacks weakly; keep ours reachable, and collect now so a dropped reference fails
        // here rather than only under GC pressure
        final Closeable onClose = onCloseInvoked::setTrue;
        session.addOnCloseCallback(onClose);
        System.gc();
        final QueueingExportListener listener = new QueueingExportListener();
        session.addExportListener(listener);

        final Throwable escaped = cancelChildDuringTeardown(race.child, session::onExpired);
        if (escaped != null) {
            throw new AssertionFailure("session expiry must not throw when an export is concurrently cancelled",
                    escaped);
        }
        Assert.eqTrue(session.isExpired(), "session.isExpired()");
        Assert.eq(race.parent.getState(), "race.parent.getState()", CANCELLED);
        Assert.eq(race.child.getState(), "race.child.getState()", CANCELLED);

        // every export the session held must have been released, and the session-scoped resources closed
        Assert.eq(unrelatedResult.refCount, "unrelatedResult.refCount", 0);
        Assert.eq(race.grandParentResult.refCount, "race.grandParentResult.refCount", 0);
        Assert.eqTrue(onCloseInvoked.booleanValue(), "onCloseInvoked.booleanValue()");
        Assert.eqTrue(listener.isComplete, "listener.isComplete");
        Assert.eq(session.numExportListeners(), "session.numExportListeners()", 0);
        Reference.reachabilityFence(onClose);
    }

    // endregion

    private static long getExportId(final ExportNotification notification) {
        return ticketToExportId(notification.getTicket(), "test");
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
            Assert.eq(getExportId(notification), "getExportId(notification)", SessionState.NON_EXPORT_ID,
                    "SessionState.NON_EXPORT_ID");
        }

        private void validateNotificationQueue(final SessionState.ExportBuilder<?> export,
                final ExportNotification.State... states) {
            validateNotificationQueue(export.getExport(), states);
        }

        private void validateNotificationQueue(final SessionState.ExportObject<?> export,
                final ExportNotification.State... states) {
            final Ticket exportId = export.getExportId();

            final List<ExportNotification.State> foundStates = notifications.stream()
                    .filter(n -> n.getTicket().equals(exportId))
                    .map(ExportNotification::getExportState)
                    .collect(Collectors.toList());
            boolean error = foundStates.size() != states.length;
            for (int offset = 0; !error && offset < states.length; ++offset) {
                error = !foundStates.get(offset).equals(states[offset]);
            }
            if (error) {
                final String found =
                        foundStates.stream().map(ExportNotification.State::toString).collect(Collectors.joining(", "));
                final String expected =
                        Arrays.stream(states).map(ExportNotification.State::toString).collect(Collectors.joining(", "));
                throw new AssertionFailure("Notification Queue Differs. Expected: " + expected + " Found: " + found);
            }
        }
    }

    /**
     * Throw an exception if lambda either does not throw, or throws an exception that is not assignable to
     * expectedExceptionType
     */
    private static <T extends Exception> void expectException(Class<T> expectedExceptionType, Runnable lambda) {
        String nameOfCaughtException = "(no exception)";
        try {
            lambda.run();
        } catch (Exception actual) {
            if (expectedExceptionType.isAssignableFrom(actual.getClass())) {
                return;
            }
            nameOfCaughtException = actual.getClass().getSimpleName();
        }
        throw new RuntimeException(String.format("Expected exception %s, got %s",
                expectedExceptionType.getSimpleName(), nameOfCaughtException));
    }

    // LivenessArtifact's constructor is private
    private static class PublicLivenessArtifact extends LivenessArtifact {
        public PublicLivenessArtifact() {}
    }

    private static class CountingLivenessReferent implements LivenessReferent {
        long refCount = 0;
        boolean everRetained = false;

        @Override
        public boolean tryRetainReference() {
            ++refCount;
            everRetained = true;
            return true;
        }

        @Override
        public void dropReference() {
            --refCount;
        }

        @Override
        public WeakReference<? extends LivenessReferent> getWeakReference() {
            return new WeakReference<>(this);
        }
    }
}
