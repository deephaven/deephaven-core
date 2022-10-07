/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

public abstract class TicketResolverBase implements TicketResolver {
    private final byte ticketPrefix;
    private final String flightDescriptorRoute;

    public TicketResolverBase(final byte ticketPrefix, final String flightDescriptorRoute) {
        this.ticketPrefix = ticketPrefix;
        this.flightDescriptorRoute = flightDescriptorRoute;
    }

    @Override
    public byte ticketRoute() {
        return ticketPrefix;
    }

    @Override
    public String flightDescriptorRoute() {
        return flightDescriptorRoute;
    }
}
