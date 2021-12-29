package io.deephaven.client.examples;

import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine.ArgGroup;

public class Ticket implements HasTicketId {
    @ArgGroup(exclusive = false)
    ScopeField scopeField;

    @ArgGroup(exclusive = false)
    ApplicationField applicationField;

    @ArgGroup(exclusive = false)
    RawTicket rawTicket;

    @Override
    public TicketId ticketId() {
        if (scopeField != null) {
            return scopeField.ticketId();
        }
        if (applicationField != null) {
            return applicationField.ticketId();
        }
        if (rawTicket != null) {
            return rawTicket.ticketId();
        }
        throw new IllegalStateException();
    }
}
