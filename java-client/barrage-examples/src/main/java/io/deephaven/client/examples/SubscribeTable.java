/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.client.examples;

import io.deephaven.qst.TableCreationLogic;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

@Command(name = "subscribe-table", mixinStandardHelpOptions = true,
        description = "Request a table and subscribe over barrage", version = "0.1.0")
class SubscribeTable extends SubscribeExampleBase {

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Override
    protected TableCreationLogic logic() {
        return ticket.ticketId().table().logic();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new SubscribeTable()).execute(args);
        System.exit(execute);
    }
}
