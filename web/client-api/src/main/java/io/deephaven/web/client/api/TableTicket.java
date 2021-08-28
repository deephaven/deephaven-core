package io.deephaven.web.client.api;

import elemental2.core.Uint8Array;
import io.deephaven.javascript.proto.dhinternal.arrow.flight.protocol.flight_pb.FlightDescriptor;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.ticket_pb.Ticket;
import io.deephaven.javascript.proto.dhinternal.io.deephaven.proto.table_pb.TableReference;
import io.deephaven.web.client.api.console.JsVariableDefinition;

/**
 * Replacement for TableHandle, wraps up Ticket plus current export state. We only consider the lower bytes for hashing
 * (since until we've got millions of tickets it won't matter).
 */
public class TableTicket {
    public static Ticket createTicket(JsVariableDefinition varDef) {
        Ticket ticket = new Ticket();
        ticket.setTicket(varDef.getId());
        return ticket;
    }

    public static TableReference createTableRef(JsVariableDefinition varDef) {
        TableReference tableRef = new TableReference();
        tableRef.setTicket(createTicket(varDef));
        return tableRef;
    }

    /**
     * UNKNOWN: 0, PENDING: 1, PUBLISHING: 2, QUEUED: 3, RUNNING: 4, EXPORTED: 5, RELEASED: 6, CANCELLED: 7, FAILED: 8,
     * DEPENDENCY_FAILED: 9, DEPENDENCY_NEVER_FOUND: 10, DEPENDENCY_CANCELLED: 11, DEPENDENCY_RELEASED: 12
     */
    public enum State {
        UNKNOWN, PENDING, PUBLISHING, QUEUED, RUNNING, EXPORTED, RELEASED, CANCELLED, FAILED, DEPENDENCY_FAILED, DEPENDENCY_NEVER_FOUND, DEPENDENCY_CANCELLED, DEPENDENCY_RELEASED;
    }

    private final Uint8Array ticket;
    private final int exportId;
    private State state = State.PENDING;
    private boolean isConnected = true;

    public TableTicket(final Uint8Array ticket) {
        this.ticket = ticket;

        int id = 0;
        for (int ii = 4; ii >= 1; --ii) {
            id = (id << 8) | ticket.getAt(ii).intValue();
        }
        this.exportId = id;
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

    public FlightDescriptor makeFlightDescriptor() {
        FlightDescriptor flightDescriptor = new FlightDescriptor();
        flightDescriptor.setType(FlightDescriptor.DescriptorType.getPATH());
        flightDescriptor.setPathList(new String[] {"export", exportId + ""});

        return flightDescriptor;
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
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        final TableTicket that = (TableTicket) o;

        return exportId == that.exportId;
    }

    @Override
    public int hashCode() {
        return exportId;
    }
}
