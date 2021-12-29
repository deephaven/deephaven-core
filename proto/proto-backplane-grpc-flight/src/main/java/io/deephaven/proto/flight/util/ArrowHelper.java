package io.deephaven.proto.flight.util;

import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.flight.impl.Flight;

import java.util.List;

public class ArrowHelper {
    public static FlightDescriptor descriptor(org.apache.arrow.flight.impl.Flight.FlightDescriptor impl) {
        switch (impl.getType()) {
            case PATH:
                return FlightDescriptor.path(impl.getPathList());
            case CMD:
                return FlightDescriptor.command(impl.getCmd().toByteArray());
            default:
                throw new IllegalArgumentException("Unexpected type " + impl.getTypeValue());
        }
    }

    public static FlightDescriptor descriptor(List<String> path) {
        return FlightDescriptor.path(path);
    }

    public static Ticket ticket(Flight.Ticket ticket) {
        return new Ticket(ticket.getTicket().toByteArray());
    }
}
