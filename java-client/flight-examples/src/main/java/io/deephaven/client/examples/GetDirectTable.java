package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasTicketId;
import org.apache.arrow.flight.FlightStream;

abstract class GetDirectTable extends FlightExampleBase {

    public abstract HasTicketId ticketId();

    @Override
    protected void execute(FlightSession flight) throws Exception {
        try (final FlightStream stream = flight.stream(ticketId())) {
            System.out.println(stream.getSchema());
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
        }
    }
}
