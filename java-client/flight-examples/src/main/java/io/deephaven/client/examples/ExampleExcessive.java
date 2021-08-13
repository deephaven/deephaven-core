package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "example-excessive", mixinStandardHelpOptions = true,
    description = "Canned example 3, sends a table, get the results, and convert to a TSV",
    version = "0.1.0")
class ExampleExcessive extends FlightCannedTableBase {

    @Option(names = {"-c", "--count"}, description = "The amount of heads/tails",
        defaultValue = "256")
    long count;

    @Override
    protected TableCreationLogic logic() {
        return new ExcessiveHeadTail(count);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ExampleExcessive()).execute(args);
        System.exit(execute);
    }
}
