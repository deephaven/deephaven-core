package io.deephaven.client.impl;

import io.deephaven.grpc_api.util.ExportTicketHelper;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.vector.types.pojo.Schema;

import javax.inject.Inject;
import java.util.Objects;

public final class FlightClientImpl implements AutoCloseable {
    private final FlightClient client;

    @Inject
    public FlightClientImpl(FlightClient client) {
        this.client = Objects.requireNonNull(client);
    }

    public Schema getSchema(Export export) {
        // TODO(deephaven-core#988): Add more async support to org.apache.arrow.flight.FlightClient
        return client.getSchema(descriptor(export)).getSchema();
    }

    public FlightStream getStream(Export export) {
        return client.getStream(new Ticket(export.ticket().getTicket().toByteArray()));
    }

    public Iterable<FlightInfo> list() {
        return client.listFlights(Criteria.ALL);
    }

    @Override
    public void close() throws InterruptedException {
        client.close();
    }

    private static FlightDescriptor descriptor(Export export) {
        return adapt(ExportTicketHelper.ticketToDescriptor(export.ticket()));
    }

    private static FlightDescriptor adapt(
        org.apache.arrow.flight.impl.Flight.FlightDescriptor impl) {
        switch (impl.getType()) {
            case PATH:
                return FlightDescriptor.path(impl.getPathList());
            case CMD:
                return FlightDescriptor.command(impl.getCmd().toByteArray());
            default:
                throw new IllegalArgumentException("Unexpected type " + impl.getTypeValue());
        }
    }
}
