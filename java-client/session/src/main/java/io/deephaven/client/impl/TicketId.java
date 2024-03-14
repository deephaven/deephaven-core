//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.protobuf.ByteStringAccess;
import io.deephaven.qst.table.TicketTable;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * An opaque holder that represents a generic flight ticket.
 */
public final class TicketId implements HasTicketId {

    static TicketId from(io.deephaven.proto.backplane.grpc.Ticket ticket) {
        return new TicketId(ticket.getTicket().toByteArray());
    }

    private final byte[] ticket;

    public TicketId(byte[] ticket) {
        this.ticket = Objects.requireNonNull(ticket);
    }

    @Override
    public TicketId ticketId() {
        return this;
    }

    public TypedTicket toTypedTicket(String type) {
        return new TypedTicket(type, this);
    }

    public TicketTable table() {
        return TicketTable.of(ticket);
    }

    byte[] bytes() {
        return ticket;
    }

    io.deephaven.proto.backplane.grpc.Ticket proto() {
        return io.deephaven.proto.backplane.grpc.Ticket.newBuilder().setTicket(ByteStringAccess.wrap(ticket)).build();
    }

    @Override
    public String toString() {
        return new String(ticket, StandardCharsets.UTF_8);
    }
}
