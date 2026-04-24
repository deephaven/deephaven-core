//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api;

import com.google.protobuf.ByteString;
import io.deephaven.proto.backplane.grpc.TableReference;
import io.deephaven.proto.backplane.grpc.Ticket;
import org.apache.arrow.flight.impl.Flight;

import java.util.Arrays;

/**
 * Replacement for TableHandle, wraps up export tickets plus current export state. We only consider the lower bytes for
 * hashing (since until we've got millions of tickets it won't matter).
 */
public class TableTicket {

    /**
     * UNKNOWN: 0, PENDING: 1, PUBLISHING: 2, QUEUED: 3, RUNNING: 4, EXPORTED: 5, RELEASED: 6, CANCELLED: 7, FAILED: 8,
     * DEPENDENCY_FAILED: 9, DEPENDENCY_NEVER_FOUND: 10, DEPENDENCY_CANCELLED: 11, DEPENDENCY_RELEASED: 12
     */
    public enum State {
        UNKNOWN, PENDING, PUBLISHING, QUEUED, RUNNING, EXPORTED, RELEASED, CANCELLED, FAILED, DEPENDENCY_FAILED, DEPENDENCY_NEVER_FOUND, DEPENDENCY_CANCELLED, DEPENDENCY_RELEASED;
    }

    private final Ticket ticket;
    private final int exportId;
    private State state = State.PENDING;
    private boolean isConnected = true;

    public TableTicket(final Ticket ticket) {
        this.ticket = ticket;

        ByteString bytes = ticket.getTicket();

        int id = 0;
        for (int ii = 4; ii >= 1; --ii) {
            id = (id << 8) | bytes.byteAt(ii);
        }
        this.exportId = id;
    }

    public Ticket getTicket() {
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
        return getTicket();
    }

    public TableReference makeTableReference() {
        return TableReference.newBuilder().setTicket(makeTicket()).build();
    }

    public Flight.FlightDescriptor makeFlightDescriptor() {
        return Flight.FlightDescriptor.newBuilder()
                .setType(Flight.FlightDescriptor.DescriptorType.PATH)
                .addAllPath(Arrays.asList("export", exportId + ""))
                .build();
    }

    @Override
    public String toString() {
        return "TableTicket{" +
                "ticket=" + ticket.getTicket() +
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
