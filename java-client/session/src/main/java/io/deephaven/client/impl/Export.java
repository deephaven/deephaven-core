package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.qst.table.TableSpec;

import java.util.List;
import java.util.Objects;

/**
 * An export represents a server-side Table that is being kept alive.
 *
 * <p>
 * Callers must maintain ownership of their exports, and close them when no longer needed.
 *
 * @see Session
 */
public final class Export implements AutoCloseable, HasExportId {

    private final ExportStates.State state;
    private final Listener listener;
    private boolean released;

    Export(ExportStates.State state, Listener listener) {
        this.state = Objects.requireNonNull(state);
        this.listener = Objects.requireNonNull(listener);
        this.released = false;
    }

    @Override
    public ExportId exportId() {
        return new ExportId("Table", state.exportId());
    }

    @Override
    public PathId pathId() {
        return exportId().pathId();
    }

    @Override
    public TicketId ticketId() {
        return exportId().ticketId();
    }

    /**
     * The session.
     *
     * @return the session
     */
    public Session session() {
        return state.session();
    }

    /**
     * The table spec.
     *
     * @return the table spec
     */
    public TableSpec table() {
        return state.table();
    }

    /**
     * True if {@code this} has been {@link #release() released}.
     *
     * @return true if released
     */
    public synchronized boolean isReleased() {
        return released;
    }

    /**
     * Creates a new reference export that has its own ownership and lifecycle. Must not be called after {@code this}
     * export has been {@link #release() released}.
     *
     * @param listener the listener
     * @return the new reference export
     */
    public synchronized Export newReference(Listener listener) {
        if (released) {
            throw new IllegalStateException("Should not take newRef after release");
        }
        return state.newReference(listener);
    }

    /**
     * Releases {@code this} export. May be called multiple times without adverse effect.
     */
    public synchronized boolean release() {
        if (released) {
            return false;
        }
        state.release(this);
        released = true;
        return true;
    }

    Listener listener() {
        return listener;
    }

    /**
     * @see #release()
     */
    @Override
    public void close() {
        release();
    }

    @Override
    public String toString() {
        return "Export{id=" + exportId().id() + '}';
    }

    public String toReadableString() {
        return exportId().toString();
    }
}
