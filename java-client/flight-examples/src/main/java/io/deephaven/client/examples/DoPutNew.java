package io.deephaven.client.examples;

import io.deephaven.client.impl.ExportId;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
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

    enum Method {
        HANDLE, TICKET, DIRECT
    }

    @Option(names = {"-m", "--method"}, description = "The method to use. [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "HANDLE")
    Method method;

    @Option(names = {"-n", "--num-rows"}, description = "The number of rows to doPut",
            defaultValue = "1000")
    long numRows;

    @Parameters(arity = "1", paramLabel = "VAR", description = "Variable name to publish.")
    String variableName;

    @Override
    protected void execute(FlightSession flight) throws Exception {
        switch (method) {
            case HANDLE:
                handle(flight);
                break;
            case TICKET:
                ticket(flight);
                break;
            case DIRECT:
                direct(flight);
                break;
            default:
                throw new IllegalStateException("Unexpected method " + method);
        }
    }

    private TableSpec table() {
        return TableSpec.empty(numRows).view("X=i");
    }

    private void publish(FlightSession flight, HasTicketId ticketId) throws InterruptedException, ExecutionException {
        flight.session().publish(variableName, ticketId).get();
    }

    private void handle(FlightSession flight) throws Exception {
        // This version is "prettier", but uses one extra ticket and round trip
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle);
                final TableHandle destHandle = flight.putExport(doGet)) {
            publish(flight, destHandle);
        }
    }

    private void ticket(FlightSession flight) throws Exception {
        // This version is more efficient, but requires manual management of a ticket
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle)) {
            final ExportId exportId = flight.putExportManual(doGet);
            try {
                publish(flight, exportId);
            } finally {
                flight.release(exportId);
            }
        }
    }

    private void direct(FlightSession flight) throws Exception {
        try (final TableHandle sourceHandle = flight.session().execute(table());
                final FlightStream doGet = flight.stream(sourceHandle)) {
            flight.put(new ScopeId(variableName), doGet);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoPutNew()).execute(args);
        System.exit(execute);
    }
}
