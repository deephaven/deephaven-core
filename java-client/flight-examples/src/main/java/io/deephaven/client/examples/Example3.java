package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "example-3", mixinStandardHelpOptions = true,
    description = "Canned example 3, sends a table, get the results, and convert to a TSV",
    version = "0.1.0")
class Example3 extends FlightCannedTableBase {

    @Override
    protected TableCreationLogic logic() {
        return ExampleFunction3.INSTANCE;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new Example3()).execute(args);
        System.exit(execute);
    }
}
