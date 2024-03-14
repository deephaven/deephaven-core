//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;

import java.util.List;

@Command(name = "publish", mixinStandardHelpOptions = true,
        description = "Publish", version = "0.1.0")
class Publish extends SingleSessionExampleBase {

    // Note: this is not perfect, and will need to look into picocli usage to better support this in the future.
    // Right now, the two ticket types need to be the same, even though that's not a technical requirement.
    @ArgGroup(exclusive = false, multiplicity = "2")
    List<Ticket> tickets;

    @Override
    protected void execute(Session session) throws Exception {
        session.publish(tickets.get(0), tickets.get(1)).get();
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new Publish()).execute(args);
        System.exit(execute);
    }
}
