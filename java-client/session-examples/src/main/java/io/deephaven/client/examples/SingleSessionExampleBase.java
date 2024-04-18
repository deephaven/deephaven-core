//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactory;
import io.grpc.ManagedChannel;

import java.util.concurrent.ScheduledExecutorService;

abstract class SingleSessionExampleBase extends SessionExampleBase {

    private volatile Session session;

    @Override
    protected void execute(SessionFactory sessionFactory) throws Exception {
        final Session localSession;
        try (final Session ignored = (session = localSession = sessionFactory.newSession())) {
            execute(localSession);
        } finally {
            session = null;
        }
    }

    protected abstract void execute(Session session) throws Exception;

    @Override
    protected void onShutdown(ScheduledExecutorService scheduler, ManagedChannel managedChannel) {
        final Session localSession = session;
        if (localSession != null) {
            localSession.close();
        }
        super.onShutdown(scheduler, managedChannel);
    }
}
