package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "excessive", mixinStandardHelpOptions = true,
        description = "Executes a deep query",
        version = "0.1.0")
class ExampleExcessive extends FlightCannedTableBase implements TableCreationLogic {

    @Option(names = {"-c", "--count"}, description = "The amount of heads/tails",
            defaultValue = "256")
    long count;

    @Override
    protected TableCreationLogic logic() {
        return this;
    }

    @Override
    public <T extends TableOperations<T, T>> T create(TableCreator<T> creation) {
        T base = creation.of(TableSpec.empty(count * 2)).view("I=i");
        for (long i = 0; i < count; ++i) {
            base = base.head(2 * count - 2 * i);
            base = base.tail(2 * count - 2 * i - 1);
        }
        return base;
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ExampleExcessive()).execute(args);
        System.exit(execute);
    }
}
