/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;


import com.google.protobuf.ByteStringAccess;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.proto.backplane.grpc.TypedTicket.Builder;
import io.deephaven.proto.util.ExportTicketHelper;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;

/**
 * A holder that represents a Deephaven ticket and accompanying type.
 */
public final class TypedTicket implements HasTypedTicket {

    static TypedTicket of(io.deephaven.proto.backplane.grpc.TypedTicket proto) {
        final String type = proto.getType();
        return new TypedTicket(type.isEmpty() ? null : type, proto.getTicket().getTicket().toByteArray());
    }

    private final String type;
    private final byte[] ticket;

    public TypedTicket(String type, byte[] ticket) {
        if (type != null && type.isEmpty()) {
            throw new IllegalArgumentException("Must use null instead of empty string to represent no type");
        }
        this.type = type;
        this.ticket = Objects.requireNonNull(ticket);
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
        return new TicketId(ticket);
    }

    ExportId toExportId() {
        final int exportId = ExportTicketHelper.ticketToExportId(ByteBuffer.wrap(ticket), "exportId");
        return new ExportId(type, exportId);
    }

    io.deephaven.proto.backplane.grpc.TypedTicket proto() {
        final Builder builder = io.deephaven.proto.backplane.grpc.TypedTicket.newBuilder()
                .setTicket(Ticket.newBuilder().setTicket(ByteStringAccess.wrap(ticket)));
        if (type != null) {
            builder.setType(type);
        }
        return builder.build();
    }
}
