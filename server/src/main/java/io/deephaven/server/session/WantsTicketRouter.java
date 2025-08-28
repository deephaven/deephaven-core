//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

/**
 * For TicketResolvers that want to be able to resolve arbitrary tickets, implement this interface and the TicketRouter
 * that is associated with this resolver will set itself.
 */
public interface WantsTicketRouter {
    void setTicketRouter(TicketRouter ticketRouter);
}
