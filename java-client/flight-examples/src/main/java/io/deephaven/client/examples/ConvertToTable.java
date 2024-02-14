/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.TableObject;
import io.deephaven.client.impl.TypedTicket;
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
            try (final TableObject tableExport = fetchTableExport(flight)) {
                showTable(flight, tableExport);
            }
        }
    }

    private void showTable(FlightSession flight, HasTicketId ticket) throws Exception {
        try (final FlightStream stream = flight.stream(ticket)) {
            while (stream.next()) {
                System.out.println(stream.getRoot().contentToTSVString());
            }
        }
    }

    private TableObject fetchTableExport(FlightSession flight) throws InterruptedException, ExecutionException {
        final ServerData fetchedObject = flight.session().fetch(new TypedTicket(type, ticket)).get();
        if (fetchedObject.exports().size() != 1) {
            throw new IllegalStateException("Expected fetched object to have exactly one export");
        }
        final ServerObject serverObject = fetchedObject.exports().get(0);
        if (!(serverObject instanceof TableObject)) {
            throw new IllegalStateException("Expected fetched object to export a Table");
        }
        if (fetchedObject.data().remaining() != 0) {
            throw new IllegalStateException("Expected fetched object to not have any bytes");
        }
        return (TableObject) serverObject;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ConvertToTable()).execute(args);
        System.exit(execute);
    }
}
