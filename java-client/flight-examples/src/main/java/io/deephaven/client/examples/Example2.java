package io.deephaven.client.examples;

import io.deephaven.api.ColumnName;
import io.deephaven.api.filter.FilterAnd;
import io.deephaven.api.filter.FilterCondition;
import io.deephaven.api.value.Value;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Collections;

@Command(name = "example-2", mixinStandardHelpOptions = true,
    description = "Canned example 2, sends a table, get the results, and convert to a TSV",
    version = "0.1.0")
class Example2 extends FlightCannedExampleBase {

    @Override
    protected TableSpec table() {
        return TableSpec.empty(100).view("I=i")
            .where(Collections
                .singletonList(FilterAnd.of(FilterCondition.gte(ColumnName.of("I"), Value.of(42L)),
                    FilterCondition.lt(ColumnName.of("I"), Value.of(55L)))));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new Example2()).execute(args);
        System.exit(execute);
    }
}
