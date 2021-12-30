package io.deephaven.client.examples;

import io.deephaven.client.impl.FetchedObject;
import io.deephaven.client.impl.Session;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

@Command(name = "fetch-object", mixinStandardHelpOptions = true,
        description = "Fetch object", version = "0.1.0")
class FetchObject extends SingleSessionExampleBase {

    @ArgGroup(exclusive = true, multiplicity = "1")
    Ticket ticket;

    @Option(names = {"-f", "--file"}, description = "The output file, otherwise goes to STDOUT.")
    Path file;

    @Override
    protected void execute(Session session) throws Exception {
        final FetchedObject customObject = session.fetchObject(ticket).get();
        final String type = customObject.type();
        System.err.println("type: " + type);
        System.err.println("size: " + customObject.size());
        if (file != null) {
            try (final OutputStream out = new BufferedOutputStream(Files.newOutputStream(file))) {
                customObject.writeTo(out);
            }
        } else {
            customObject.writeTo(System.out);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new FetchObject()).execute(args);
        System.exit(execute);
    }
}
