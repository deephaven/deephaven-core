/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.appmode;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
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
import io.deephaven.util.SafeCloseable;
import io.deephaven.auth.AuthContext;
import io.grpc.Context;
import io.grpc.stub.StreamObserver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;

public class ApplicationServiceGrpcImplTest {
    private static final long TOKEN_EXPIRE_MS = 1_000_000;
    private static final AuthContext AUTH_CONTEXT = new AuthContext.SuperUser();

    private SafeCloseable livenessScope;
    private TestControlledScheduler scheduler;
    private SessionService sessionService;
    private ApplicationServiceGrpcImpl applicationServiceGrpcImpl;

    @Before
    public void setup() {
        livenessScope = LivenessScopeStack.open();
        scheduler = new TestControlledScheduler();
        sessionService = new SessionService(scheduler,
                authContext -> new SessionState(scheduler, ExecutionContext::createForUnitTests, authContext),
                TOKEN_EXPIRE_MS, Optional.empty(), Collections.emptyMap());
        applicationServiceGrpcImpl = new ApplicationServiceGrpcImpl(scheduler, sessionService,
                new TypeLookup(ObjectTypeLookup.NoOp.INSTANCE));
    }

    @After
    public void teardown() {
        livenessScope.close();

        scheduler = null;
        sessionService = null;
        livenessScope = null;
    }

    /**
     * Confirm that if an observer listening to onListFields fails that the service will correctly remove it.
     */
    @Test
    public void onListFieldsSubscribeFailedObserver() {
        // to trigger this, we need at least two clients, one will break after its first message
        AtomicBoolean faultyObserverShouldStillWork = new AtomicBoolean(true);

        // start two subscriptions to the app fields, spoofing auth as different connections - the first will behave,
        // the
        // second will not
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
        ScriptSession scriptSession = new NoLanguageDeephavenSession();
        scriptSession.setVariable("key", "hello world");
        ScriptSession.Changes changes = new ScriptSession.Changes();
        changes.created.put("key", "Object");
        applicationServiceGrpcImpl.onScopeChanges(scriptSession, changes);

        // have the scheduler do some work, so we can send updates to broken users
        scheduler.runUntilQueueEmpty();

        // verify the second subscriber got its call
        assertEquals(2, workingObserver.messageCount);
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
