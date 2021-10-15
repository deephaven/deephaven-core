/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TicketTable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "subscribe-qs", mixinStandardHelpOptions = true,
        description = "Request a table from the QueryScope and subscribe over barrage", version = "0.1.0")
class SubscribeQueryScope extends SubscribeExampleBase {

    @Parameters(arity = "1", paramLabel = "field", description = "Query scope field name to subscribe to.")
    String fieldName;

    @Override
    protected TableCreationLogic logic() {
        return TicketTable.fromQueryScopeField(fieldName).logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new SubscribeQueryScope()).execute(args);
        System.exit(execute);
    }
}
