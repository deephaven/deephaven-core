package io.deephaven.client.examples;

import io.deephaven.api.TableOperations;
import io.deephaven.api.agg.Sum;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.util.Collections;

@Command(name = "large-sum", mixinStandardHelpOptions = true,
        description = "Canned example 3, sends a table, get the results, and convert to a TSV",
        version = "0.1.0")
class LargeSum extends FlightCannedTableBase {

    @Option(names = {"-c", "--count"}, description = "The number of sums, defaults to 1000000000",
            defaultValue = "1000000000")
    long count;

    @Override
    protected TableCreationLogic logic() {
        return this::create;
    }

    public <T extends TableOperations<T, T>> T create(TableCreator<T> c) {
        return c.of(TableSpec.empty(count))
                .view("I=i")
                .by(Collections.emptyList(), Collections.singleton(Sum.of("Sum=I")));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new LargeSum()).execute(args);
        System.exit(execute);
    }
}
