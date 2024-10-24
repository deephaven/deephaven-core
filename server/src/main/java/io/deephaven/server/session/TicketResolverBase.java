//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import io.deephaven.server.auth.AuthorizationProvider;

import java.util.Objects;

public abstract class TicketResolverBase extends PathResolverPrefixedBase {

    protected final Authorization authorization;
    private final byte ticketPrefix;

    public TicketResolverBase(
            final AuthorizationProvider authProvider,
            final byte ticketPrefix,
            final String flightDescriptorRoute) {
        super(flightDescriptorRoute);
        this.authorization = Objects.requireNonNull(authProvider.getTicketResolverAuthorization());
        this.ticketPrefix = ticketPrefix;
    }

    @Override
    public final byte ticketRoute() {
        return ticketPrefix;
    }
}
