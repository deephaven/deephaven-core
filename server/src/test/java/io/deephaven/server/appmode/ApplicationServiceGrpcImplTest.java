//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.appmode;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.TestExecutionContext;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.NoLanguageDeephavenSession;
import io.deephaven.engine.util.ScriptSession;
import io.deephaven.plugin.type.ObjectTypeLookup;
import io.deephaven.proto.backplane.grpc.FieldsChangeUpdate;
import io.deephaven.proto.backplane.grpc.ListFieldsRequest;
import io.deephaven.server.object.TypeLookup;
import io.deephaven.server.session.SessionService;
import io.deephaven.server.session.SessionServiceGrpcImpl;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.util.TestControlledScheduler;
import io.deephaven.auth.AuthContext;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ApplicationServiceGrpcImplTest {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    private static final long TOKEN_EXPIRE_MS = 1_000_000;
    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private TestControlledScheduler scheduler;
    private SessionService sessionService;
    private ApplicationServiceGrpcImpl applicationServiceGrpcImpl;

    @Before
    public void setup() {
        scheduler = new TestControlledScheduler();
        sessionService = new SessionService(scheduler,
                authContext -> new SessionState(scheduler, new SessionService.ObfuscatingErrorTransformer(),
                        TestExecutionContext::createForUnitTests, authContext),
                TOKEN_EXPIRE_MS, Collections.emptyMap(), Collections.emptySet());
        applicationServiceGrpcImpl = new ApplicationServiceGrpcImpl(scheduler, sessionService,
                new TypeLookup(ObjectTypeLookup.NoOp.INSTANCE));
    }

    @After
    public void teardown() {
        scheduler = null;
        sessionService = null;
    }

    /**
     * Confirm that if an observer listening to onListFields fails that the service will correctly remove it.
     */
    @Test
    public void onListFieldsSubscribeFailedObserver() {
        // to trigger this, we need at least two clients, one will break after its first message
        AtomicBoolean faultyObserverShouldStillWork = new AtomicBoolean(true);

        // start two subscriptions to the app fields, spoofing auth as different connections - the first will behave,
        // the second will not
        Context.current().withValue(SessionServiceGrpcImpl.SESSION_CONTEXT_KEY, sessionService.newSession(AUTH_CONTEXT))
                .wrap(() -> {
                    applicationServiceGrpcImpl.listFields(ListFieldsRequest.getDefaultInstance(),
                            new FaultyObserver<>(faultyObserverShouldStillWork));
                }).run();
        CountingObserver<FieldsChangeUpdate> workingObserver = new CountingObserver<>();
        Context.current().withValue(SessionServiceGrpcImpl.SESSION_CONTEXT_KEY, sessionService.newSession(AUTH_CONTEXT))
                .wrap(() -> {
                    applicationServiceGrpcImpl.listFields(ListFieldsRequest.getDefaultInstance(), workingObserver);
                }).run();

        assertEquals(1, workingObserver.messageCount);

        // break the client now that they have gotten their first update
        faultyObserverShouldStillWork.set(false);

        // trigger a change
        ScriptSession scriptSession = new NoLanguageDeephavenSession(
                ExecutionContext.getContext().getUpdateGraph(),
                ExecutionContext.getContext().getOperationInitializer());
        scriptSession.getQueryScope().putParam("key", "hello world");
        ScriptSession.Changes changes = new ScriptSession.Changes();
        changes.created.put("key", "Object");
        applicationServiceGrpcImpl.onScopeChanges(scriptSession, changes);

        // have the scheduler do some work, so we can send updates to broken users
        scheduler.runUntilQueueEmpty();

        // verify the second subscriber got its call
        assertEquals(2, workingObserver.messageCount);
    }

    /**
     * Confirm that an existing ListFields subscription won't prevent a new one from getting an accurate initial
     * snapshot
     */
    @Test
    public void onListFieldsSubscribeNewObserver() {
        // start two subscriptions to the app fields, spoofing auth as different connections - the first will behave,
        // the second will not
        Context.current().withValue(SessionServiceGrpcImpl.SESSION_CONTEXT_KEY, sessionService.newSession(AUTH_CONTEXT))
                .run(() -> {
                    // Open a subscription, leave it running for the duration of the test
                    applicationServiceGrpcImpl.listFields(ListFieldsRequest.getDefaultInstance(),
                            new CountingObserver<>());

                    // Trigger a change, don't ask the test scheduler to pick this up
                    ScriptSession scriptSession = new NoLanguageDeephavenSession(
                            ExecutionContext.getContext().getUpdateGraph(),
                            ExecutionContext.getContext().getOperationInitializer());
                    scriptSession.getQueryScope().putParam("key", "hello world");
                    ScriptSession.Changes changes = new ScriptSession.Changes();
                    changes.created.put("key", "Object");
                    applicationServiceGrpcImpl.onScopeChanges(scriptSession, changes);

                    // Open a second subscription, ensure it has the new object
                    CountDownLatch latch = new CountDownLatch(1);
                    applicationServiceGrpcImpl.listFields(ListFieldsRequest.getDefaultInstance(),
                            new StreamObserver<>() {
                                @Override
                                public void onNext(FieldsChangeUpdate fieldsChangeUpdate) {
                                    assertEquals(1, fieldsChangeUpdate.getCreatedCount());
                                    assertEquals("key", fieldsChangeUpdate.getCreated(0).getFieldName());
                                    latch.countDown();
                                }

                                @Override
                                public void onError(Throwable throwable) {}

                                @Override
                                public void onCompleted() {}
                            });

                    try {
                        assertTrue(latch.await(1, TimeUnit.SECONDS));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private static class CountingObserver<T> implements StreamObserver<T> {
        private int messageCount;

        @Override
        public void onNext(T value) {
            messageCount++;
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }

    private static class FaultyObserver<T> implements StreamObserver<T> {
        private final AtomicBoolean working;

        public FaultyObserver(AtomicBoolean working) {
            this.working = working;
        }

        @Override
        public void onNext(T value) {
            if (!working.get()) {
                throw new RuntimeException("whoops");
            }
        }

        @Override
        public void onError(Throwable t) {

        }

        @Override
        public void onCompleted() {

        }
    }
}
