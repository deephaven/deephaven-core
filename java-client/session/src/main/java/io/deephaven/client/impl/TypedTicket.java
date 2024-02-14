/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;


import io.deephaven.proto.backplane.grpc.TypedTicket.Builder;
import io.deephaven.proto.util.ExportTicketHelper;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/**
 * A holder that represents a Deephaven ticket and accompanying type.
 */
public final class TypedTicket implements HasTypedTicket {

    static TypedTicket from(io.deephaven.proto.backplane.grpc.TypedTicket proto) {
        final String type = proto.getType();
        final TicketId ticket = TicketId.from(proto.getTicket());
        return new TypedTicket(type.isEmpty() ? null : type, ticket);
    }

    private final String type;
    private final TicketId ticket;

    public TypedTicket(String type, TicketId ticket) {
        if (type != null && type.isEmpty()) {
            throw new IllegalArgumentException("Must use null instead of empty string to represent no type");
        }
        this.type = type;
        this.ticket = Objects.requireNonNull(ticket);
    }

    public TypedTicket(String type, HasTicketId ticket) {
        this(type, ticket.ticketId());
    }

    public Optional<String> type() {
        return Optional.ofNullable(type);
    }

    @Override
    public TypedTicket typedTicket() {
        return this;
    }

    @Override
    public TicketId ticketId() {
        return ticket;
    }

    @Override
    public String toString() {
        return (type == null ? "?:" : type + ":") + ticket;
    }

    ExportId toExportId() {
        final int exportId = ExportTicketHelper.ticketToExportId(ByteBuffer.wrap(ticket.bytes()), "exportId");
        return new ExportId(type, exportId);
    }

    io.deephaven.proto.backplane.grpc.TypedTicket proto() {
        final Builder builder = io.deephaven.proto.backplane.grpc.TypedTicket.newBuilder()
                .setTicket(ticket.proto());
        if (type != null) {
            builder.setType(type);
        }
        return builder.build();
    }
}
