package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "example-2", mixinStandardHelpOptions = true,
    description = "Canned example 2, sends a table, get the results, and convert to a TSV",
    version = "0.1.0")
class Example2 extends FlightCannedTableBase {

    @Override
    protected TableCreationLogic logic() {
        return ExampleFunction2.INSTANCE;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new Example2()).execute(args);
        System.exit(execute);
    }
}
