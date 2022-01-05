package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "get-table", mixinStandardHelpOptions = true, description = "Get a table", version = "0.1.0")
class GetDirectTable extends FlightExampleBase {

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        try (final FlightStream stream = flight.stream(ticket)) {
            System.out.println(stream.getSchema());
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetDirectTable()).execute(args);
        System.exit(execute);
    }
}
