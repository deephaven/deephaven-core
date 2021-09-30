/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TicketTable;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

@Command(name = "subscribe-app-field", mixinStandardHelpOptions = true,
        description = "Request a table from an Application and subscribe over barrage", version = "0.1.0")
class SubscribeApplicationField extends SubscribeExampleBase {

    @Parameters(arity = "1", paramLabel = "app", description = "Application id to fetch from.")
    String appId;
    @Parameters(arity = "1", paramLabel = "field", description = "Application field name to subscribe to.")
    String fieldName;

    @Override
    protected TableCreationLogic logic() {
        return TicketTable.fromApplicationField(appId, fieldName).logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new SubscribeApplicationField()).execute(args);
        System.exit(execute);
    }
}
