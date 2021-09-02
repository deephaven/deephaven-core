package io.deephaven.client.impl;

import io.deephaven.grpc_api.util.FlightExportTicketHelper;
import io.grpc.ManagedChannel;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightGrpcUtilsExtension;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Collections;
import java.util.Objects;

public final class FlightSession implements AutoCloseable {

    public static FlightSession of(SessionImpl session, BufferAllocator incomingAllocator,
            ManagedChannel channel) {
        final FlightClient client = FlightGrpcUtilsExtension.createFlightClientWithSharedChannel(
                incomingAllocator, channel, Collections.singletonList(new SessionMiddleware(session)));
        return new FlightSession(session, client);
    }

    private final SessionImpl session;
    private final FlightClient client;

    private FlightSession(SessionImpl session, FlightClient client) {
        this.session = Objects.requireNonNull(session);
        this.client = Objects.requireNonNull(client);
    }

    public Session session() {
        return session;
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
        return adapt(FlightExportTicketHelper.ticketToDescriptor(export.ticket(), "export"));
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
