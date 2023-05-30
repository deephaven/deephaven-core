/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.audit;

/**
 * TODO is this necessary to keep? The AuditEvent class doesn't exist and this can't be used anywhere. A simple
 * interface that hides the context necessary for creating and logging {@code AuditEvent}s. Useful so that callers are
 * only responsible for the core parts of the event, namely {@code AuditEvent#getEvent()} and
 * {@code AuditEvent#getDetails()}.
 */
public interface AuditEventLoggerBasic {
    void log(String event, String details);

    enum Null implements AuditEventLoggerBasic {
        INSTANCE;

        @Override
        public void log(String event, String details) {
            // ignore
        }
    }
}
