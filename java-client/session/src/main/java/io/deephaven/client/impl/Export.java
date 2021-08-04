package io.deephaven.client.impl;

import io.deephaven.client.impl.ExportRequest.Listener;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.TableSpec;

import java.util.Objects;

/**
 * An export represents a server-side object that is being kept alive.
 *
 * <p>
 * Callers must maintain ownership of their exports, and close them when no longer needed.
 *
 * @see Session
 */
public final class Export implements AutoCloseable {

    private final ExportStates.State state;
    private final Listener listener;
    private boolean released;

    Export(ExportStates.State state, Listener listener) {
        this.state = Objects.requireNonNull(state);
        this.listener = Objects.requireNonNull(listener);
        this.released = false;
    }

    /**
     * The ticket.
     *
     * @return the ticket
     */
    public Ticket ticket() {
        return state.ticket();
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
     * Creates a new reference export that has its own ownership and lifecycle. Must not be called
     * after {@code this} export has been {@link #release() released}.
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
    public synchronized void release() {
        if (released) {
            return;
        }
        state.release(this);
        released = true;
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
        return "Export{ticket=" + ExportTicketHelper.toReadableString(state.ticket()) + '}';
    }
}
