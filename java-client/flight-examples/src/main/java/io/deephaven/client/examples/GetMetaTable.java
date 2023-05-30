/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.MetaTable;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "get-meta-table", mixinStandardHelpOptions = true, description = "Get a meta table", version = "0.1.0")
class GetMetaTable extends FlightCannedTableBase {

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    protected TableCreationLogic logic() {
        return MetaTable.of(ticket.ticketId().table()).logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new GetMetaTable()).execute(args);
        System.exit(execute);
    }
}
