/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.session;

import com.google.rpc.Code;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.GrpcUtil;
import org.apache.arrow.flight.impl.Flight;

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
    public Flight.FlightInfo flightInfoFor(final Flight.FlightDescriptor descriptor) {
        // sessions do not participate in resolving flight descriptors
        throw GrpcUtil.statusRuntimeException(Code.FAILED_PRECONDITION, "No such flight exists");
    }

    @Override
    public void forAllFlightInfo(final SessionState session, final Consumer<Flight.FlightInfo> visitor) {
        // sessions do not expose tickets via list flights
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(final SessionState session, final ByteBuffer ticket) {
        return session.getExport(ExportTicketHelper.ticketToExportId(ticket));
    }

    @Override
    public <T> SessionState.ExportObject<T> resolve(final SessionState session, final Flight.FlightDescriptor descriptor) {
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
