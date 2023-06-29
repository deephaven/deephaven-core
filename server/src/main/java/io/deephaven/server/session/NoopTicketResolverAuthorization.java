package io.deephaven.server.session;

import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteBuffer;

public class NoopTicketResolverAuthorization implements TicketResolver.Authorization {
    @Override
    public <T> T transform(T source) {
        return source;
    }

    @Override
    public void authorizePublishRequest(ByteBuffer ticket) {
        // always allowed
    }

    @Override
    public void authorizePublishRequest(Flight.FlightDescriptor descriptor) {
        // always allowed
    }
}
