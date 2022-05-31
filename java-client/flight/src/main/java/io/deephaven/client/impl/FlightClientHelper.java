package io.deephaven.client.impl;

import io.deephaven.qst.table.NewTable;
import org.apache.arrow.flight.AsyncPutListener;
import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightClient.ClientStreamListener;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.SchemaResult;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public class FlightClientHelper {
    public static FlightStream get(FlightClient client, HasTicketId ticket, CallOption... callOptions) {
        return client.getStream(ticket(ticket), callOptions);
    }

    public static void put(FlightClient client, HasPathId pathId, NewTable table, BufferAllocator allocator,
            CallOption... callOptions) {
        put(client, descriptor(pathId), table, allocator, callOptions);
    }

    public static void put(FlightClient client, HasPathId pathId, FlightStream input, CallOption... callOptions) {
        put(client, descriptor(pathId), input, callOptions);
    }

    public static void put(FlightClient client, FlightDescriptor descriptor, NewTable table, BufferAllocator allocator,
            CallOption... callOptions) {
        final VectorSchemaRoot root = VectorSchemaRootAdapter.of(table, allocator);
        final ClientStreamListener out = client.startPut(descriptor, root, new AsyncPutListener(), callOptions);
        out.putNext();
        root.clear();
        out.completed();
        out.getResult();
    }

    public static void put(FlightClient client, FlightDescriptor descriptor, FlightStream input,
            CallOption... callOptions) {
        final ClientStreamListener out =
                client.startPut(descriptor, input.getRoot(), new AsyncPutListener(), callOptions);
        while (input.next()) {
            out.putNext();
            input.getRoot().clear();
        }
        out.completed();
        out.getResult();
    }

    public static SchemaResult getSchema(FlightClient client, HasPathId pathId, CallOption... callOptions) {
        return client.getSchema(descriptor(pathId), callOptions);
    }

    private static Ticket ticket(HasTicketId ticket) {
        return new Ticket(ticket.ticketId().bytes());
    }

    private static FlightDescriptor descriptor(HasPathId pathId) {
        return FlightDescriptor.path(pathId.pathId().path());
    }
}
