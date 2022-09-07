/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.AssertionFailure;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.table.ExportTableUpdateListenerTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.engine.liveness.LivenessArtifact;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.liveness.LivenessScope;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.liveness.LivenessStateException;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.util.SafeCloseable;
import io.deephaven.auth.AuthContext;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.ref.WeakReference;
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
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.RUNNING;
import static io.deephaven.proto.backplane.grpc.ExportNotification.State.UNKNOWN;
import static io.deephaven.proto.util.ExportTicketHelper.ticketToExportId;

public class SessionStateTest {

    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private LivenessScope livenessScope;
    private TestControlledScheduler scheduler;
    private SessionState session;
    private int nextExportId;

    @Before
    public void setup() {
        livenessScope = new LivenessScope();
        LivenessScopeStack.push(livenessScope);
        scheduler = new TestControlledScheduler();
        session = new SessionState(scheduler, ExecutionContext::createForUnitTests, AUTH_CONTEXT);
        session.initializeExpiration(new SessionService.TokenExpiration(UUID.randomUUID(),
                DateTimeUtils.nanosToTime(Long.MAX_VALUE), session));
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
    public void testDestroyOnExportRelease() {
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            exportObj = session.newExport(nextExportId++).submit(() -> export);
        }

        // no ref counts yet
        Assert.eq(export.refCount, "export.refCount", 0);

        // export the object; should inc ref count
        scheduler.runUntilQueueEmpty();
        Assert.eq(export.refCount, "export.refCount", 1);

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
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
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
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            exportObj = session.newExport(nextExportId++).submit(() -> export);
        }

        // no ref counts yet
        Assert.eq(export.refCount, "export.refCount", 0);

        // export the object; should inc ref count
        scheduler.runUntilQueueEmpty();
        Assert.eq(export.refCount, "export.refCount", 1);

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
    public void testServerExportDestroyOnSessionRelease() {
        final CountingLivenessReferent export = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
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
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++).submit(() -> export);
        expectException(IllegalStateException.class, exportObj::get);
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.get(), "exportObj.get()", export, "export");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.EXPORTED);
    }

    @Test
    public void testThrowInExportMain() {
        final MutableBoolean errored = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onErrorHandler(err -> errored.setTrue())
                .submit(() -> {
                    throw new RuntimeException("submit exception");
                });
        Assert.eqFalse(errored.booleanValue(), "errored.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        scheduler.runUntilQueueEmpty();
        Assert.eqTrue(errored.booleanValue(), "errored.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.FAILED);
    }

    @Test
    public void testThrowInErrorHandler() {
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .onErrorHandler(err -> {
                    throw new RuntimeException("error handler exception");
                })
                .submit(() -> {
                    submitted.setTrue();
                    throw new RuntimeException("submit exception");
                });
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);
        scheduler.runUntilQueueEmpty();
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.FAILED);
    }

    @Test
    public void testCancelBeforeDefined() {
        final SessionState.ExportObject<Object> exportObj = session.getExport(nextExportId);
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.UNKNOWN);

        exportObj.cancel();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);

        final MutableBoolean submitted = new MutableBoolean();
        expectException(StatusRuntimeException.class,
                () -> session.newExport(nextExportId++).submit(submitted::setTrue));
        scheduler.runUntilQueueEmpty();

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testCancelBeforeExport() {
        final SessionState.ExportObject<?> d1 = session.getExport(nextExportId++);

        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .submit(submitted::setTrue);

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.PENDING);
        exportObj.cancel();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);
        scheduler.runUntilQueueEmpty();

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testCancelDuringExport() {
        final MutableObject<LivenessArtifact> export = new MutableObject<>();
        final SessionState.ExportObject<Object> exportObj =
                session.newExport(nextExportId++).submit(() -> {
                    session.getExport(nextExportId - 1).cancel();
                    export.setValue(new PublicLivenessArtifact());
                    return export;
                });

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.CANCELLED);

        if (export.getValue().tryRetainReference()) {
            throw new IllegalStateException("this should be destroyed");
        }
    }

    @Test
    public void testCancelPostExport() {
        final MutableObject<LivenessArtifact> export = new MutableObject<>();
        final SessionState.ExportObject<Object> exportObj;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            exportObj = session.newExport(nextExportId++).submit(() -> {
                export.setValue(new PublicLivenessArtifact());
                return export.getValue();
            });
        }

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.EXPORTED);

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
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .submit(submitted::setTrue);

        d1.cancel();
        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.DEPENDENCY_CANCELLED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testErrorPropagatesNotYetFailed() {
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .submit(submitted::setTrue);

        session.newExport(d1.getExportId(), "test")
                .submit(() -> {
                    throw new RuntimeException("I fail.");
                });

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.DEPENDENCY_FAILED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testErrorPropagatesAlreadyFailed() {
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.newExport(nextExportId++)
                .submit(() -> {
                    throw new RuntimeException("I fail.");
                });
        scheduler.runUntilQueueEmpty();
        Assert.eq(d1.getState(), "d1.getState()", ExportNotification.State.FAILED);

        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .submit(submitted::setTrue);

        scheduler.runUntilQueueEmpty();
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.DEPENDENCY_FAILED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testWorkItemOutOfOrderDependency() {
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> d1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++)
                .require(d1)
                .submit(submitted::setTrue);

        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.PENDING);

        session.newExport(d1.getExportId(), "test")
                .submit(() -> {
                });
        scheduler.runOne(); // d1
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.QUEUED);

        scheduler.runOne(); // d1
        Assert.eq(exportObj.getState(), "exportObj.getState()", ExportNotification.State.EXPORTED);
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
        final CountingLivenessReferent export = new CountingLivenessReferent();

        final SessionState.ExportObject<CountingLivenessReferent> e1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            e1 = session.<CountingLivenessReferent>newExport(nextExportId++)
                    .submit(() -> export);
        }

        scheduler.runOne();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);

        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .submit(() -> Assert.gt(e1.get().refCount, "e1.get().refCount", 0));
        Assert.eq(e2.getState(), "e1.getState()", ExportNotification.State.QUEUED);

        e1.release();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);

        Assert.gt(export.refCount, "e1.get().refCount", 0);
        scheduler.runOne();
        Assert.eq(export.refCount, "e1.get().refCount", 0);
    }

    @Test
    public void testLateDependencyAlreadyReleasedFails() {
        final CountingLivenessReferent export = new CountingLivenessReferent();

        final SessionState.ExportObject<CountingLivenessReferent> e1;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            e1 = session.<CountingLivenessReferent>newExport(nextExportId++)
                    .submit(() -> export);
        }

        scheduler.runOne();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.EXPORTED);
        e1.release();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);

        final MutableBoolean errored = new MutableBoolean();
        expectException(LivenessStateException.class, () -> {
            final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                    .require(e1)
                    .onErrorHandler(err -> errored.setTrue())
                    .submit(() -> Assert.gt(e1.get().refCount, "e1.get().refCount", 0));
        });
    }

    @Test
    public void testNewExportRequiresPositiveId() {
        expectException(IllegalArgumentException.class, () -> session.newExport(0));
        expectException(IllegalArgumentException.class, () -> session.newExport(-1));
    }

    @Test
    public void testDependencyAlreadyReleased() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(() -> {
        });
        scheduler.runUntilQueueEmpty();
        e1.release();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++).require(e1).submit(() -> {
        });
        Assert.eq(e2.getState(), "e1.getState()", ExportNotification.State.DEPENDENCY_RELEASED);
    }

    @Test
    public void testExpiredNewExport() {
        final SessionState.ExportObject<Object> exportObj = session.newExport(nextExportId++).submit(Object::new);
        scheduler.runUntilQueueEmpty();
        session.onExpired();
        expectException(StatusRuntimeException.class, exportObj::get);
    }

    @Test
    public void testExpiredNewNonExport() {
        final SessionState.ExportObject<Object> exportObj = session.nonExport().submit(Object::new);
        scheduler.runUntilQueueEmpty();
        session.onExpired();
        expectException(StatusRuntimeException.class, exportObj::get);
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
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportBuilder<Object> exportBuilder = session.nonExport();
        session.onExpired();
        exportBuilder.submit(submitted::setTrue);
        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testExpireBeforeExportSubmit() {
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportBuilder<Object> exportBuilder = session.newExport(nextExportId++);
        session.onExpired();
        exportBuilder.submit(submitted::setTrue);
        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
    }

    @Test
    public void testExpireDuringExport() {
        final CountingLivenessReferent export = new CountingLivenessReferent();
        session.newExport(nextExportId++)
                .submit(() -> {
                    session.onExpired();
                    return export;
                });
        scheduler.runUntilQueueEmpty();
        Assert.eq(export.refCount, "export.refCount", 0);
    }

    @Test
    public void testDependencyFailed() {
        final SessionState.ExportObject<Object> e1 = session.getExport(nextExportId++);
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .submit(() -> {
                });
        session.newExport(e1.getExportId(), "test").submit(() -> {
            throw new RuntimeException();
        });
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_FAILED);
    }

    @Test
    public void testDependencyAlreadyFailed() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(() -> {
            throw new RuntimeException();
        });
        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.FAILED);
        expectException(IllegalStateException.class, e1::get);

        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .submit(() -> {
                });
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_FAILED);
        expectException(IllegalStateException.class, e2::get);
    }

    @Test
    public void testDependencyAlreadyCanceled() {
        final SessionState.ExportObject<Object> e1 = session.getExport(nextExportId++);
        e1.cancel();
        scheduler.runUntilQueueEmpty();

        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .submit(() -> {
                });
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.DEPENDENCY_CANCELLED); // cancels propagate
        expectException(IllegalStateException.class, e2::get);
    }

    @Test
    public void testDependencyAlreadyExported() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(() -> {
        });
        scheduler.runUntilQueueEmpty();

        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++)
                .require(e1)
                .submit(() -> {
                });
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.QUEUED);
        scheduler.runUntilQueueEmpty();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.EXPORTED);
    }

    @Test
    public void testDependencyReleasedBeforeExport() {
        final CountingLivenessReferent e1 = new CountingLivenessReferent();
        final SessionState.ExportObject<Object> e1obj;
        try (final SafeCloseable scope = LivenessScopeStack.open()) {
            e1obj = session.newExport(nextExportId++).submit(() -> e1);
        }
        scheduler.runUntilQueueEmpty();

        final SessionState.ExportObject<Object> e2obj = session.newExport(nextExportId++)
                .require(e1obj)
                .submit(() -> {
                    Assert.neqNull(e1obj.get(), "e1obj.get()");
                    Assert.gt(e1.refCount, "e1.refCount", 0);
                });

        e1obj.release();
        Assert.eq(e1obj.getState(), "e1obj.getState()", ExportNotification.State.RELEASED);

        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.refCount, "e1.refCount", 0);
        Assert.eq(e2obj.getState(), "e2obj.getState()", ExportNotification.State.EXPORTED);
    }

    @Test
    public void testChildCancelledFirst() {
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(() -> {
        });
        final SessionState.ExportObject<Object> e2 = session.newExport(nextExportId++).require(e1).submit(() -> {
        });
        e2.cancel();
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.CANCELLED);
        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.getState(), "e2.getState()", ExportNotification.State.EXPORTED);
        Assert.eq(e2.getState(), "e2.getState()", ExportNotification.State.CANCELLED);
    }

    @Test
    public void testCannotOutOfOrderServerExports() {
        // server-side exports must already exist
        expectException(StatusRuntimeException.class, () -> session.getExport(-1));
    }

    @Test
    public void testVerifyExpirationSession() {
        final SessionState session = new SessionState(scheduler, ExecutionContext::createForUnitTests, AUTH_CONTEXT);
        final SessionService.TokenExpiration expiration =
                new SessionService.TokenExpiration(UUID.randomUUID(), DateTimeUtils.nanosToTime(Long.MAX_VALUE),
                        session);
        expectException(IllegalArgumentException.class, () -> this.session.initializeExpiration(expiration));
        expectException(IllegalArgumentException.class, () -> this.session.updateExpiration(expiration));
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
                new SessionService.TokenExpiration(UUID.randomUUID(), scheduler.currentTime(), session));
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
        final MutableBoolean submitted = new MutableBoolean();
        final SessionState.ExportObject<Object> e1 = session.newExport(nextExportId++).submit(submitted::setTrue);
        e1.release();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.QUEUED);
        Assert.eqFalse(submitted.booleanValue(), "submitted.booleanValue()");
        scheduler.runUntilQueueEmpty();
        Assert.eq(e1.getState(), "e1.getState()", ExportNotification.State.RELEASED);
        Assert.eqTrue(submitted.booleanValue(), "submitted.booleanValue()");
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

        final SessionState.ExportObject<SessionState> e1 = session.<SessionState>getExport(nextExportId++);
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
        final SessionState.ExportObject<Object> n1 =
                session.nonExport()
                        .require(e1)
                        .submit(() -> {
                            throw new RuntimeException("this should not reach test framework");
                        });
        scheduler.runUntilQueueEmpty();
        Assert.eq(n1.getState(), "n1.getState()", FAILED, "FAILED");
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
                .require(e1)
                .submit(submitRan::setTrue);

        scheduler.runUntilQueueEmpty();
        Assert.eqFalse(submitRan.booleanValue(), "submitRan.booleanValue()");
        Assert.eqTrue(caughtErr.getValue() instanceof StatusRuntimeException, "caughtErr.getValue()");

        final StatusRuntimeException sre = (StatusRuntimeException) caughtErr.getValue();
        Assert.eq(sre.getStatus(), "sre.getStatus()", Status.DATA_LOSS, "Status.DATA_LOSS");
    }

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
    };
}
