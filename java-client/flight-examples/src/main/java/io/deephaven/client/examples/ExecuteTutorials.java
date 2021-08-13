package io.deephaven.client.examples;

import io.deephaven.client.examples.tutorials.Tutorials;
import io.deephaven.qst.TableCreationLabeledLogic;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "tutorials", mixinStandardHelpOptions = true, description = "Execute tutorials",
    version = "0.1.0")
class ExecuteTutorials extends FlightCannedLabeledTableBase {

    @Override
    protected TableCreationLabeledLogic logic() {
        return Tutorials.INSTANCE;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ExecuteTutorials()).execute(args);
        System.exit(execute);
    }
}
