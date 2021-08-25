package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "get-tsv", mixinStandardHelpOptions = true,
        description = "Send a QST, get the results, and convert to a TSV", version = "0.1.0")
class GetTsv extends FlightCannedTableBase {

    @Parameters(arity = "1", paramLabel = "QST", description = "QST file to send and get.",
            converter = TableConverter.class)
    TableSpec table;

    @Override
    protected TableCreationLogic logic() {
        return table.logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetTsv()).execute(args);
        System.exit(execute);
    }
}
