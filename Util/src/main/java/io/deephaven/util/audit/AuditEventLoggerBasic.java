package io.deephaven.util.audit;

/**
 * An simple interface that hides the context necessary for creating and logging
 * {@link AuditEvent}s. Useful so that callers are only responsible for the core parts of the event,
 * namely {@link AuditEvent#getEvent()} and {@link AuditEvent#getDetails()}.
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
