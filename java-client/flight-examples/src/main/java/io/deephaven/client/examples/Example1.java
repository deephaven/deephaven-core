package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "example-1", mixinStandardHelpOptions = true,
    description = "Canned example 1, sends a table, get the results, and convert to a TSV",
    version = "0.1.0")
class Example1 extends FlightCannedTableBase {

    @Override
    protected TableCreationLogic logic() {
        return ExampleFunction1.INSTANCE;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new Example1()).execute(args);
        System.exit(execute);
    }
}
