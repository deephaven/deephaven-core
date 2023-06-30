package io.deephaven.server.session;

import com.google.rpc.Code;
import io.deephaven.proto.util.Exceptions;
import org.apache.arrow.flight.impl.Flight;

import java.nio.ByteBuffer;

public class NoopTicketResolverAuthorization implements TicketResolver.Authorization {
    @Override
    public <T> T transform(T source) {
        return source;
    }

    @Override
    public void authorizePublishRequest(TicketResolver ticketResolver, ByteBuffer ticket) {
        throw Exceptions.statusRuntimeException(Code.PERMISSION_DENIED, "deny all");
    }

    @Override
    public void authorizePublishRequest(TicketResolver ticketResolver, Flight.FlightDescriptor descriptor) {
        throw Exceptions.statusRuntimeException(Code.PERMISSION_DENIED, "deny all");
    }
}
