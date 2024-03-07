//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.protobuf.ByteString;
import io.deephaven.client.impl.ObjectService.Bidirectional;
import io.deephaven.client.impl.ObjectService.MessageStream;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Objects;

/**
 * Data sent from the client to the server as part of a {@link Bidirectional#connect(MessageStream) bidirection message
 * stream}.
 */
public final class ClientData {
    private final ByteBuffer data;
    private final List<? extends HasTypedTicket> tickets;

    public ClientData(ByteBuffer data, List<? extends HasTypedTicket> tickets) {
        this.data = Objects.requireNonNull(data);
        this.tickets = Objects.requireNonNull(tickets);
    }

    public ByteBuffer data() {
        return data;
    }

    public List<? extends HasTypedTicket> tickets() {
        return tickets;
    }

    io.deephaven.proto.backplane.grpc.ClientData proto() {
        return io.deephaven.proto.backplane.grpc.ClientData.newBuilder()
                .setPayload(ByteString.copyFrom(data().slice()))
                .addAllReferences(() -> tickets()
                        .stream()
                        .map(HasTypedTicket::typedTicket)
                        .map(TypedTicket::proto)
                        .iterator())
                .build();
    }
}
