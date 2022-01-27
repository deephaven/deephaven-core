package io.deephaven.client.examples;

import io.deephaven.client.impl.ExportId;
import io.deephaven.client.impl.FetchedObject;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasTicketId;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.concurrent.ExecutionException;

@Command(name = "convert-to-table", mixinStandardHelpOptions = true, description = "Convert to table",
        version = "0.1.0")
class ConvertToTable extends FlightExampleBase {

    @Option(names = {"--type"}, required = true, description = "The ticket type.")
    String type;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        if ("Table".equals(type)) {
            showTable(flight, ticket);
        } else {
            final ExportId exportId = fetchObject(flight);
            showTable(flight, exportId);
        }
    }

    private void showTable(FlightSession flight, HasTicketId ticket) throws Exception {
        try (final FlightStream stream = flight.stream(ticket)) {
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
        }
    }

    private ExportId fetchObject(FlightSession flight) throws InterruptedException, ExecutionException {
        final FetchedObject fetchedObject = flight.session().fetchObject(type, ticket).get();
        if (fetchedObject.exportIds().size() != 1) {
            throw new IllegalStateException("Expected fetched object to have exactly one export");
        }
        final ExportId exportId = fetchedObject.exportIds().get(0);
        if (!"Table".equals(exportId.type().orElse(null))) {
            throw new IllegalStateException("Expected fetched object to export a Table");
        }
        if (fetchedObject.size() != 0) {
            throw new IllegalStateException("Expected fetched object to not have any bytes");
        }
        return exportId;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ConvertToTable()).execute(args);
        System.exit(execute);
    }
}
