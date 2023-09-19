/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.MessageStream;
import io.deephaven.proto.backplane.grpc.Data;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * Data and typed tickets to be sent from the client to the server as part of a
 * {@link Bidirectional#messageStream(MessageStream) bidirection message stream}.
 */
public final class DataAndTypedTickets {
    private final ByteBuffer data;
    private final List<? extends HasTypedTicket> tickets;

    public DataAndTypedTickets(ByteBuffer data, List<? extends HasTypedTicket> tickets) {
        this.data = Objects.requireNonNull(data);
        this.tickets = Objects.requireNonNull(tickets);
    }

    public ByteBuffer data() {
        return data;
    }

    public List<? extends HasTypedTicket> tickets() {
        return tickets;
    }

    Data proto() {
        return Data.newBuilder()
                .setPayload(ByteString.copyFrom(data()))
                .addAllExportedReferences(() -> tickets()
                        .stream()
                        .map(HasTypedTicket::typedTicket)
                        .map(TypedTicket::proto)
                        .iterator())
                .build();
    }
}
