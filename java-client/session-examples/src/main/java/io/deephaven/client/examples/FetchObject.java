/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.ServerObject.Fetchable;
import io.deephaven.client.impl.FetchedObject;
import io.deephaven.client.impl.ServerObject;
import io.deephaven.client.impl.Session;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
        try (final FetchedObject obj = session.fetchObject(type, ticket).get()) {
            show(0, obj);
        }
    }

    private void show(int depth, FetchedObject fetchedObj)
            throws IOException, ExecutionException, InterruptedException {
        final String prefix = " ".repeat(depth);
        final String type = fetchedObj.type();
        System.err.println(prefix + "type: " + type);
        System.err.println(prefix + "size: " + fetchedObj.size());
        for (ServerObject export : fetchedObj.exports()) {
            System.err.println(prefix + "exportId: " + export);
        }
        if (file != null) {
            try (final OutputStream out = new BufferedOutputStream(Files.newOutputStream(file))) {
                fetchedObj.writeTo(out);
            }
        } else {
            fetchedObj.writeTo(System.out);
        }
        if (recursive) {
            for (ServerObject serverObject : fetchedObj.exports()) {
                show(depth + 1, serverObject);
            }
        }
    }

    private void show(int depth, ServerObject obj)
            throws IOException, ExecutionException, InterruptedException {
        if (obj instanceof Fetchable) {
            try (final FetchedObject fetched = ((Fetchable) obj).fetch().get()) {
                show(depth, fetched);
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
