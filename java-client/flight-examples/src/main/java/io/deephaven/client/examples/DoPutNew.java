package io.deephaven.client.examples;

import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasTicket;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.proto.backplane.grpc.Ticket;
import io.deephaven.qst.table.TableSpec;
import org.apache.arrow.flight.FlightStream;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.util.concurrent.ExecutionException;

@Command(name = "do-put-new", mixinStandardHelpOptions = true,
        description = "Do Put New", version = "0.1.0")
class DoPutNew extends FlightExampleBase {

    @Option(names = {"-e", "--efficient"}, description = "Use the more efficient version",
            defaultValue = "false")
    boolean efficient;

    @Option(names = {"-n", "--num-rows"}, description = "The number of rows to doPut",
            defaultValue = "1000")
    long numRows;

    @Parameters(arity = "1", paramLabel = "VAR", description = "Variable name to publish.")
    String variableName;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        if (efficient) {
            moreEfficient(flight);
        } else {
            easy(flight);
        }
    }

    private TableSpec table() {
        return TableSpec.empty(numRows).view("X=i");
    }

    private void publish(FlightSession flight, HasTicket ticket) throws InterruptedException, ExecutionException {
        flight.session().publish(variableName, ticket).get();
    }

    private void easy(FlightSession flight) throws Exception {
        // This version is "prettier", but uses one extra ticket and round trip
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle);
                final TableHandle destHandle = flight.put(doGet)) {
            publish(flight, destHandle);
        }
    }

    private void moreEfficient(FlightSession flight) throws Exception {
        // This version is more efficient, but requires manual management of a ticket
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle)) {
            final Ticket ticket = flight.putTicket(doGet);
            try {
                publish(flight, () -> ticket);
            } finally {
                flight.release(ticket);
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoPutNew()).execute(args);
        System.exit(execute);
    }
}
