package io.deephaven.client.examples;

import io.deephaven.client.impl.ExportId;
import io.deephaven.client.impl.FlightSession;
import io.deephaven.client.impl.HasTicketId;
import io.deephaven.client.impl.ScopeId;
import io.deephaven.client.impl.TableHandle;
import io.deephaven.qst.column.header.ColumnHeader;
import io.deephaven.qst.table.NewTable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.time.Instant;
import java.util.concurrent.ExecutionException;

@Command(name = "do-put-table", mixinStandardHelpOptions = true,
        description = "Do Put Table", version = "0.1.0")
class DoPutTable extends FlightExampleBase {

    enum Method {
        HANDLE, TICKET, DIRECT
    }

    @Option(names = {"-m", "--method"}, description = "The method to use. [ ${COMPLETION-CANDIDATES} ]",
            defaultValue = "HANDLE")
    Method method;

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

    private NewTable newTable() {
        return ColumnHeader.of(
                ColumnHeader.ofBoolean("Boolean"),
                ColumnHeader.ofByte("Byte"),
                ColumnHeader.ofChar("Char"),
                ColumnHeader.ofShort("Short"),
                ColumnHeader.ofInt("Int"),
                ColumnHeader.ofLong("Long"),
                ColumnHeader.ofFloat("Float"),
                ColumnHeader.ofDouble("Double"),
                ColumnHeader.ofString("String"),
                ColumnHeader.ofInstant("Instant"))
                .start(3)
                .row(true, (byte) 42, 'a', (short) 32_000, 1234567, 1234567890123L, 3.14f, 3.14d, "Hello, World",
                        Instant.now())
                .row(null, null, null, null, null, null, null, null, null, (Instant) null)
                .row(false, (byte) -42, 'b', (short) -32_000, -1234567, -1234567890123L, -3.14f, -3.14d, "Goodbye.",
                        Instant.ofEpochMilli(0))
                .newTable();
    }

    private void publish(FlightSession flight, HasTicketId ticketId) throws InterruptedException, ExecutionException {
        flight.session().publish(variableName, ticketId).get();
    }

    private void handle(FlightSession flight) throws Exception {
        // This version is "prettier", but uses one extra ticket and round trip
        try (final TableHandle destHandle = flight.putExport(newTable(), bufferAllocator)) {
            publish(flight, destHandle);
        }
    }

    private void ticket(FlightSession flight) throws Exception {
        // This version is more efficient, but requires manual management of an export ticket
        final ExportId exportId = flight.putExportManual(newTable(), bufferAllocator);
        try {
            publish(flight, exportId);
        } finally {
            flight.release(exportId);
        }
    }

    private void direct(FlightSession flight) {
        // This version is most efficient, but the RHS is ephemeral and can't be re-referenced
        flight.put(new ScopeId(variableName), newTable(), bufferAllocator);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new DoPutTable()).execute(args);
        System.exit(execute);
    }
}
