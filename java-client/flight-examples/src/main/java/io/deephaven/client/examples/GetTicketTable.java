package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "get-ticket-table", mixinStandardHelpOptions = true,
        description = "Get ticket table",
        version = "0.1.0")
class GetTicketTable extends FlightCannedTableBase implements TableCreationLogic {

    @Parameters(arity = "1", paramLabel = "TICKET", description = "The ticket.")
    String ticket;

    @Override
    protected TableCreationLogic logic() {
        return this;
    }

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> c) {
        return c.ticket(ticket);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetTicketTable()).execute(args);
        System.exit(execute);
    }
}
