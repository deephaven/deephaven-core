//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.protobuf.ByteString;
import io.deephaven.engine.table.Table;
import io.deephaven.server.auth.AuthorizationProvider;
import io.deephaven.server.session.SessionState.ExportObject;
import io.grpc.StatusRuntimeException;
import org.apache.arrow.flight.impl.Flight;
import org.apache.arrow.flight.impl.Flight.FlightDescriptor;
import org.apache.arrow.flight.impl.Flight.FlightInfo;
import org.jetbrains.annotations.Nullable;

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

    // These methods can be elevated to TicketResolver in the future if necessary.

    protected abstract Flight.Ticket getTicket(FlightDescriptor descriptor, String logId);

    protected abstract StatusRuntimeException notFound(FlightDescriptor descriptor, String logId);

    private SessionState.ExportObject<Table> resolveTable(@Nullable SessionState session,
            Flight.FlightDescriptor descriptor, String logId) {
        return resolve(session, descriptor, logId).map(x -> asTable(x, descriptor, logId));
    }

    private Table asTable(Object o, FlightDescriptor descriptor, String logId) {
        if (!(o instanceof Table)) {
            throw notFound(descriptor, logId);
        }
        return (Table) o;
    }

    @Override
    public final ExportObject<ByteString> getSchema(@Nullable SessionState session, FlightDescriptor descriptor,
            String logId) {
        return resolveTable(session, descriptor, logId).map(TicketRouter::getSchema);
    }

    @Override
    public final ExportObject<FlightInfo> flightInfoFor(@Nullable SessionState session, FlightDescriptor descriptor,
            String logId) {
        return resolveTable(session, descriptor, logId)
                .map(table -> TicketRouter.getFlightInfo(table, descriptor, getTicket(descriptor, logId)));
    }
}
