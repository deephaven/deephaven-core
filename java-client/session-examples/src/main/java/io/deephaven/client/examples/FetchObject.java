/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.ServerData;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.ObjectService.Fetchable;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.TypedTicket;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

@Command(name = "fetch-object", mixinStandardHelpOptions = true,
        description = "Fetch object", version = "0.1.0")
class FetchObject extends SingleSessionExampleBase {

    @Option(names = {"--type"}, required = true, description = "The ticket type.")
    String type;

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Option(names = {"-f", "--file"}, description = "The output file, otherwise goes to STDOUT.")
    Path file;

    @Option(names = {"-r", "--recursive"}, description = "If the program should recursively fetch.")
    boolean recursive;

    @Override
    protected void execute(Session session) throws Exception {
        try (
                final Fetchable fetchable = session.fetchable(new TypedTicket(type, ticket)).get();
                final ServerData dataAndExports = fetchable.fetch().get()) {
            show(0, type, dataAndExports);
        }
    }

    private void show(int depth, String type, ServerData dataAndExports)
            throws IOException, ExecutionException, InterruptedException {
        final String prefix = " ".repeat(depth);
        System.err.println(prefix + "type: " + type);
        System.err.println(prefix + "size: " + dataAndExports.data().remaining());
        for (ServerObject export : dataAndExports.exports()) {
            System.err.println(prefix + "exportId: " + export);
        }
        final byte[] data = new byte[dataAndExports.data().remaining()];
        dataAndExports.data().slice().get(data);
        if (file != null) {
            Files.write(file, data);
        } else {
            System.out.write(data);
        }
        if (recursive) {
            for (ServerObject serverObject : dataAndExports.exports()) {
                show(depth + 1, serverObject);
            }
        }
    }

    private void show(int depth, ServerObject obj)
            throws IOException, ExecutionException, InterruptedException {
        if (obj instanceof Fetchable) {
            final Fetchable fetchable = (Fetchable) obj;
            try (final ServerData fetched = fetchable.fetch().get()) {
                show(depth, fetchable.type(), fetched);
            }
        } else {
            final String prefix = " ".repeat(depth);
            System.err.println(prefix + obj + " is not fetchable");
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new FetchObject()).execute(args);
        System.exit(execute);
    }
}
