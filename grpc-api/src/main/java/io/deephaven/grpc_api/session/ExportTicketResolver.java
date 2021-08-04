/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.session;

import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.GrpcUtil;
import org.apache.arrow.flight.impl.Flight;
import org.jetbrains.annotations.Nullable;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

@Singleton
public class ExportTicketResolver extends TicketResolverBase {

    @Inject
    public ExportTicketResolver() {
        super(ExportTicketHelper.TICKET_PREFIX, ExportTicketHelper.FLIGHT_DESCRIPTOR_ROUTE);
    }

    @Override
    public String getLogNameFor(ByteBuffer ticket) {
        return ExportTicketHelper.toReadableString(ticket);
    }

    @Override
    public SessionState.ExportObject<Flight.FlightInfo> flightInfoFor(@Nullable final SessionState session, final Flight.FlightDescriptor descriptor) {
        if (session == null) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "No session to search");
        }

        final SessionState.ExportObject<?> export = resolve(session, descriptor);
        return session.<Flight.FlightInfo>nonExport()
                .require(export)
                .submit(() -> {
                    if (export.get() instanceof Table) {
                        return TicketRouter.getFlightInfo((Table) export.get(), descriptor, ExportTicketHelper.descriptorToArrowTicket(descriptor));
                    }

                    throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No such flight exists");
                });
    }

    @Override
    public void forAllFlightInfo(@Nullable final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // sessions do not expose tickets via list flights
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session, final ByteBuffer ticket) {
        if (session == null) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "No session to resolve from");
        }

        return session.getExport(ExportTicketHelper.ticketToExportId(ticket));
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(@Nullable final SessionState session, final Flight.FlightDescriptor descriptor) {
        if (session == null) {
            throw GrpcUtil.statusRuntimeException(Code.UNAUTHENTICATED, "No session to resolve from");
        }

        return session.getExport(ExportTicketHelper.descriptorToExportId(descriptor));
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session, final ByteBuffer ticket) {
        return session.newExport(ExportTicketHelper.ticketToExportId(ticket));
    }

    @Override
    public <T> SessionState.ExportBuilder<T> publish(final SessionState session, final Flight.FlightDescriptor descriptor) {
        return session.newExport(ExportTicketHelper.descriptorToExportId(descriptor));
    }
}
