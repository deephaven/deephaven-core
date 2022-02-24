package io.deephaven.client.examples;

import io.deephaven.client.impl.ExportId;
import io.deephaven.client.impl.FetchedObject;
import io.deephaven.client.impl.HasTicketId;
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
        show(session, type, ticket);
    }

    private void show(Session session, String type, HasTicketId ticket)
            throws IOException, ExecutionException, InterruptedException {
        if ("Table".equals(type)) {
            System.err.println("Unable to fetchObject for 'Table'");
            return;
        }
        final FetchedObject customObject = session.fetchObject(type, ticket).get();
        show(session, customObject);
    }

    private void show(Session session, FetchedObject customObject)
            throws IOException, ExecutionException, InterruptedException {
        final String type = customObject.type();
        System.err.println("type: " + type);
        System.err.println("size: " + customObject.size());
        for (ExportId exportId : customObject.exportIds()) {
            System.err.println("exportId: " + exportId);
        }
        if (file != null) {
            try (final OutputStream out = new BufferedOutputStream(Files.newOutputStream(file))) {
                customObject.writeTo(out);
            }
        } else {
            customObject.writeTo(System.out);
        }
        if (recursive) {
            for (ExportId exportId : customObject.exportIds()) {
                if (exportId.type().isPresent()) {
                    show(session, exportId.type().get(), exportId);
                }
            }
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new FetchObject()).execute(args);
        System.exit(execute);
    }
}
