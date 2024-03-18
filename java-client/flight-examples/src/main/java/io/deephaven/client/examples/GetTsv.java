//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.EmptyTable;
import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "get-tsv", mixinStandardHelpOptions = true,
        description = "Send a QST, get the results, and convert to a TSV", version = "0.1.0")
class GetTsv extends FlightCannedTableBase {

    TableSpec table = EmptyTable.of(42).view("I=ii");

    @Override
    protected TableCreationLogic logic() {
        return table.logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetTsv()).execute(args);
        System.exit(execute);
    }
}
