//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.api.ColumnName;
import io.deephaven.api.RawString;
import io.deephaven.api.TableOperations;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.filter.FilterComparison;
import io.deephaven.api.literal.Literal;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.TableCreator;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;

import java.util.Collections;

@Command(name = "example-3", mixinStandardHelpOptions = true,
        description = "Canned example 3, sends a table, get the results, and convert to a TSV",
        version = "0.1.0")
class Example3 extends FlightCannedTableBase {

    @Override
    protected TableCreationLogic logic() {
        return Example3::create;
    }

    public static <T extends TableOperations<T, T>> T create(TableCreator<T> c) {
        return c.of(TableSpec.empty(100)).view("I=i").where(Filter.or(
                FilterComparison.lt(ColumnName.of("I"), Literal.of(42L)),
                FilterComparison.eq(ColumnName.of("I"), Literal.of(93L)),
                RawString.of("I % 2 == 0")));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new Example3()).execute(args);
        System.exit(execute);
    }
}
