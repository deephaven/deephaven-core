package io.deephaven.client.impl;

import com.google.protobuf.ByteStringAccess;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.TicketTable;

import java.util.Objects;

/**
 * An opaque holder that represents a generic flight ticket.
 */
public final class TicketId implements HasTicketId {

    private final byte[] ticket;

    public TicketId(byte[] ticket) {
        this.ticket = Objects.requireNonNull(ticket);
    }

    TicketId(Ticket ticket) {
        this(ticket.getTicket().toByteArray());
    }

    @Override
    public TicketId ticketId() {
        return this;
    }

    byte[] bytes() {
        return ticket;
    }

    Ticket ticket() {
        return Ticket.newBuilder().setTicket(ByteStringAccess.wrap(ticket)).build();
    }

    public TicketTable table() {
        return TicketTable.of(ticket);
    }
}
