package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactory;

abstract class SingleSessionExampleBase extends SessionExampleBase {

    @Override
    protected void execute(SessionFactory sessionFactory) throws Exception {
        try (final Session session = sessionFactory.newSession()) {
            execute(session);
        }
    }

    protected abstract void execute(Session session) throws Exception;
}
