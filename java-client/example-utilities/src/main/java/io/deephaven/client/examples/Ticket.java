//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.HasPathId;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.TicketId;
import picocli.CommandLine.ArgGroup;

public class Ticket implements HasTicketId {
    @ArgGroup(exclusive = false)
    ScopeField scopeField;

    @ArgGroup(exclusive = false)
    ApplicationField applicationField;

    @ArgGroup(exclusive = false)
    SharedField sharedField;

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
        if (sharedField != null) {
            return sharedField.ticketId();
        }
        if (rawTicket != null) {
            return rawTicket.ticketId();
        }
        throw new IllegalStateException();
    }

    public HasPathId asHasPathId() {
        return () -> {
            if (scopeField != null) {
                return scopeField.pathId();
            }
            if (applicationField != null) {
                return applicationField.pathId();
            }
            if (sharedField != null) {
                return sharedField.pathId();
            }
            if (rawTicket != null) {
                throw new IllegalArgumentException("Unable to get a path from a raw ticket");
            }
            throw new IllegalStateException();
        };
    }
}
