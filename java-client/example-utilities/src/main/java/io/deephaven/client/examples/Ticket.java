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

    private HasTicketId get() {
        if (scopeField != null) {
            return scopeField;
        }
        if (applicationField != null) {
            return applicationField;
        }
        if (rawTicket != null) {
            return rawTicket;
        }
        throw new IllegalStateException();
    }

    @Override
    public TicketId ticketId() {
        return get().ticketId();
    }
}
