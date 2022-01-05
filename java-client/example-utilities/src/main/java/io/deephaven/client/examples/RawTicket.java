package io.deephaven.client.examples;

import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine.Option;

import java.nio.charset.StandardCharsets;

public class RawTicket implements HasTicketId {
    @Option(names = {"--ticket"}, required = true, description = "The raw ticket.")
    String ticket;

    @Override
    public TicketId ticketId() {
        return new TicketId(ticket.getBytes(StandardCharsets.UTF_8));
    }
}
