//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.server.auth.AuthorizationProvider;

public abstract class TicketResolverBase implements TicketResolver {

    protected final Authorization authorization;
    private final byte ticketPrefix;
    private final String flightDescriptorRoute;

    public TicketResolverBase(
            final AuthorizationProvider authProvider,
            final byte ticketPrefix, final String flightDescriptorRoute) {
        this.authorization = authProvider.getTicketResolverAuthorization();
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
