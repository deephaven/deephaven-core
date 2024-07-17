//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.session;

import com.google.common.collect.MapMaker;
import com.google.protobuf.ByteString;
import com.google.rpc.Code;
import io.deephaven.engine.table.Table;
import io.deephaven.flightsql.DeephavenFlightSqlProducer;
import io.deephaven.proto.flight.util.FlightExportTicketHelper;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.proto.util.ExportTicketHelper;
import io.deephaven.server.auth.AuthorizationProvider;
import org.apache.arrow.flight.FlightRuntimeException;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import static io.deephaven.proto.util.FlightSqlTicketHelper.FLIGHT_DESCRIPTOR_ROUTE;
import static io.deephaven.proto.util.FlightSqlTicketHelper.TICKET_PREFIX;

@Singleton
public class FlightSqlTicketResolver extends TicketResolverBase {

    private final ConcurrentMap<ByteString, SessionState.ExportObject<?>> sharedVariables = new MapMaker()
            .weakValues()
            .makeMap();

    @Inject
    public FlightSqlTicketResolver(
            final AuthorizationProvider authProvider) {
        super(authProvider, (byte) TICKET_PREFIX, FLIGHT_DESCRIPTOR_ROUTE);
    }

    @Override
    public String getLogNameFor(final ByteBuffer ticket, final String logId) {
        return ExportTicketHelper.toReadableString(ticket, logId);
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED, String.format(
                    "Could not resolve '%s': no session to handoff to", logId));
        }

        Table table;
        try {
            table = DeephavenFlightSqlProducer.processCommand(descriptor, logId);
        } catch (FlightRuntimeException e) {
            throw Exceptions.statusRuntimeException(e.status().code(), String.format(
                    "Could not resolve '%s': %s", logId, e.status().description()));
        }
        table = DeephavenFlightSqlProducer.processCommand(descriptor, logId);
        SessionState.ExportObject export = session.newServerSideExport(table);
        return session.<Flight.FlightInfo>nonExport()
                .require(export)
                .submit(() -> {
                    Object result = export.get();
                    if (result instanceof Table) {
                        result = authorization.transform(result);
                    }
                    Flight.Ticket flightTicket =
                            FlightExportTicketHelper.exportIdToFlightTicket(export.getExportIdInt());
                    Flight.FlightDescriptor flightPathDescriptor =
                            FlightExportTicketHelper.ticketToDescriptor(flightTicket, logId);
                    if (result instanceof Table) {
                        return TicketRouter.getFlightInfo((Table) result, flightPathDescriptor,
                                FlightExportTicketHelper.descriptorToFlightTicket(flightPathDescriptor, logId));
                    }

                    throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                            "Could not support '" + logId + "': flight '" + descriptor + "' not supported");
                });
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // shared tickets are otherwise private, so we don't need to do anything here
    }

    // TODO
    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final ByteBuffer ticket, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no exports can exist without an active session");
        }

        return session.getExport(ExportTicketHelper.ticketToExportId(ticket, logId));
    }

    // TODO
    @Override
    public <T> SessionState.ExportObject<T> resolve(
            @Nullable final SessionState session, final Flight.FlightDescriptor descriptor, final String logId) {
        if (session == null) {
            throw Exceptions.statusRuntimeException(Code.UNAUTHENTICATED,
                    "Could not resolve '" + logId + "': no exports can exist without a session to search");
        }

        return session.getExport(FlightExportTicketHelper.descriptorToExportId(descriptor, logId));
    }

    // TODO
    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final ByteBuffer ticket,
            final String logId,
            @Nullable final Runnable onPublish) {
        final SessionState.ExportBuilder<T> toPublish =
                session.newExport(ExportTicketHelper.ticketToExportId(ticket, logId));
        if (onPublish != null) {
            session.nonExport()
                    .require(toPublish.getExport())
                    .submit(onPublish);
        }
        return toPublish;
    }

    // TODO
    @Override
    public <T> SessionState.ExportBuilder<T> publish(
            final SessionState session,
            final Flight.FlightDescriptor descriptor,
            final String logId,
            @Nullable final Runnable onPublish) {
        final SessionState.ExportBuilder<T> toPublish =
                session.newExport(FlightExportTicketHelper.descriptorToExportId(descriptor, logId));
        if (onPublish != null) {
            session.nonExport()
                    .require(toPublish.getExport())
                    .submit(onPublish);
        }
        return toPublish;
    }
}
