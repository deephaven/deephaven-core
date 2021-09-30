package io.deephaven.client.impl;

import io.deephaven.proto.backplane.grpc.Ticket;

public interface HasTicket {
    /**
     * The ticket.
     *
     * @return the ticket
     */
    Ticket ticket();
}
