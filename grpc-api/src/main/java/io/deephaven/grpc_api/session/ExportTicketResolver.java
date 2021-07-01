/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.session;

import com.google.rpc.Code;
import io.deephaven.db.tables.Table;
import io.deephaven.grpc_api.util.ExportTicketHelper;
import io.deephaven.grpc_api.util.GrpcUtil;
import io.deephaven.proto.backplane.grpc.ExportNotification;
import org.apache.arrow.flight.impl.Flight;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.function.Consumer;

@Singleton
public class ExportTicketResolver extends TicketResolverBase {

    private final SessionService sessionService;

    @Inject
    public ExportTicketResolver(final SessionService sessionService) {
        super(ExportTicketHelper.TICKET_PREFIX, ExportTicketHelper.FLIGHT_DESCRIPTOR_ROUTE);
        this.sessionService = sessionService;
    }

    @Override
    public String getLogNameFor(ByteBuffer ticket) {
        return ExportTicketHelper.toReadableString(ticket);
    }

    @Override
    public Flight.FlightInfo flightInfoFor(final Flight.FlightDescriptor descriptor) {
        final SessionState session = sessionService.getOptionalSession();
        if (session != null) {
            final SessionState.ExportObject<?> export = session.getExportIfExists(ExportTicketHelper.descriptorToExportId(descriptor));
            if (export != null && export.tryRetainReference()) {
                try {
                    //noinspection SynchronizationOnLocalVariableOrMethodParameter
                    synchronized (export) {
                        if (export.getState() == ExportNotification.State.EXPORTED) {
                            final Object realExport = export.get();
                            if (realExport instanceof Table) {
                                return TicketRouter.getFlightInfo((Table) realExport, descriptor, ExportTicketHelper.descriptorToTicket(descriptor));
                            }
                        }
                    }
                } finally {
                    export.dropReference();
                }
            }
        }
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
