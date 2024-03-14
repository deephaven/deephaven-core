//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteBuffer;

public class NoopTicketResolverAuthorization implements TicketResolver.Authorization {
    @Override
    public <T> T transform(T source) {
        return source;
    }

    @Override
    public void authorizePublishRequest(TicketResolver ticketResolver, ByteBuffer ticket) {
        // always allowed
    }

    @Override
    public void authorizePublishRequest(TicketResolver ticketResolver, Flight.FlightDescriptor descriptor) {
        // always allowed
    }
}
