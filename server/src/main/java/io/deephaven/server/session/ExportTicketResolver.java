/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.session;

import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
import io.deephaven.extensions.barrage.util.GrpcUtil;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.auth.AuthorizationProvider;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static io.deephaven.proto.util.ExportTicketHelper.TICKET_PREFIX;
import static io.deephaven.proto.util.ExportTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;

/**
 * Note that the export ticket resolver does not run the export results through the auth table transformation. This is
 * because any source tables will be transformed as they are initially exported to the session.
 */
@Singleton
public class ExportTicketResolver extends TicketResolverBase {

    @Inject
    public ExportTicketResolver(final AuthorizationProvider authProvider) {
        super(authProvider, TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return ExportTicketHelper.toReadableString(ticket, logId);
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no exports can exist without a session to search");
        }

        final SessionState.ExportObject<?> export = resolve(session, descriptor, logId);
        return session.<Flight.FlightInfo>nonExport()
                .require(export)
                .submit(() -> {
                    if (export.get() instanceof Table) {
                        return TicketRouter.getFlightInfo((Table) export.get(), descriptor,
                                FlightExportTicketHelper.descriptorToFlightTicket(descriptor, logId));
                    }

                    throw GrpcUtil.statusRuntimeException(Code.NOT_FOUND,
                            "Could not resolve '" + logId + "': flight '" + descriptor.toString() + " does not exist");
                });
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // sessions do not expose tickets via list flights
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        if (session == null) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no exports can exist without an active session");
        }

        return session.getExport(ExportTicketHelper.ticketToExportId(ticket, logId));
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no exports can exist without a session to search");
        }

        return session.getExport(FlightExportTicketHelper.descriptorToExportId(descriptor, logId));
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final ByteBuffer ticket, final String logId) {
        return session.newExport(ExportTicketHelper.ticketToExportId(ticket, logId));
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        return session.newExport(FlightExportTicketHelper.descriptorToExportId(descriptor, logId));
    }
}
