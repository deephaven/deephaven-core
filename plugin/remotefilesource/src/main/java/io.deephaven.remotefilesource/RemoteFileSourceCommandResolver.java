package io.deephaven.remotefilesource;

import io.deephaven.server.session.CommandResolver;
import io.deephaven.server.session.SessionState;
import io.deephaven.server.session.TicketRouter;
import io.deephaven.server.session.WantsTicketRouter;

import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public class RemoteFileSourceCommandResolver implements CommandResolver, WantsTicketRouter {
    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(@Nullable final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId) {
        return null;
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // nothing to do
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        // no tickets
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean handlesCommand(final Flight.FlightDescriptor descriptor) {
        return false;
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        // no publishing
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session,
            final Flight.FlightDescriptor descriptor, final String logId,
            @Nullable final Runnable onPublish) {
        // no publishing
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId) {
        return null;
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session,
            final ByteBuffer ticket,
            final String logId) {
        // no tickets
        throw new UnsupportedOperationException();
    }

    @Override
    public void setTicketRouter(TicketRouter ticketRouter) {
        // not needed
    }

    @Override
    public byte ticketRoute() {
        return 0;
    }
}
