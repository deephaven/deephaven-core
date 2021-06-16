package io.deephaven.web.client.api;

import elemental2.core.Int32Array;
import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;

/**
 * Replacement for TableHandle, wraps up Ticket plus current export state. We only consider the lower bytes for
 * hashing (since until we've got millions of tickets it won't matter).
 */
public class TableTicket {

    /**
     *   UNKNOWN: 0,
     *   PENDING: 1,
     *   QUEUED: 2,
     *   EXPORTED: 3,
     *   RELEASED: 4,
     *   CANCELLED: 5,
     *   FAILED: 6,
     *   DEPENDENCY_FAILED: 7,
     *   DEPENDENCY_NEVER_FOUND: 8
     */
    public enum State {
        UNKNOWN,
        PENDING,
        QUEUED,
        EXPORTED,
        RELEASED,
        CANCELLED,
        FAILED,
        DEPENDENCY_FAILED,
        DEPENDENCY_NEVER_FOUND;
    }
    private final Uint8Array ticket;
    private final int highBytes;
    private final int lowBytes;
    private State state = State.PENDING;
    private boolean isConnected = true;

    public TableTicket(final Uint8Array ticket) {
        this.ticket = ticket;
        final Int32Array int32s = new Int32Array(ticket.buffer);
        highBytes = int32s.getAnyAt(1).asInt();
        lowBytes = int32s.getAnyAt(0).asInt();
    }

    public Uint8Array getTicket() {
        return ticket;
    }

    public boolean isConnected() {
        return isConnected;
    }

    public void setConnected(final boolean connected) {
        isConnected = connected;
    }


    public State getState() {
        return state;
    }

    public void setState(final State state) {
        this.state = state;
    }

    public void setState(double stateOrdinal) {
        this.state = State.values()[(int) stateOrdinal];
    }

    public boolean isResolved() {
        return isConnected && state == State.EXPORTED;
    }

    public Ticket makeTicket() {
        Ticket ticket = new Ticket();
        ticket.setTicket(getTicket());
        return ticket;
    }

    public TableReference makeTableReference() {
        TableReference reference = new TableReference();
        reference.setTicket(makeTicket());
        return reference;
    }

    @Override
    public String toString() {
        return "TableTicket{" +
                "ticket=" + ticket +
                ", state=" + state +
                ", isConnected=" + isConnected +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final TableTicket that = (TableTicket) o;

        if (highBytes != that.highBytes) return false;
        return lowBytes == that.lowBytes;
    }

    @Override
    public int hashCode() {
        // high bytes is nearly always 0 or -1, at least until we have millions of tickets created, and
        // in either case the low bytes will not collide, so we can just use the low bytes to hash
        return lowBytes;
    }
}
