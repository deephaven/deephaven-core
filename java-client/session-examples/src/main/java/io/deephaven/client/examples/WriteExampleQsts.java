package io.deephaven.client.examples;

import io.deephaven.qst.table.TableSpec;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Callable;

@Command(name = "write-qsts", mixinStandardHelpOptions = true,
        description = "Write some example QSTs to disk", version = "0.1.0")
public class WriteExampleQsts implements Callable<Void> {

    @Option(names = {"-d", "--dir"}, description = "The output directory, defaults to .",
            defaultValue = ".")
    Path dir;

    @Override
    public Void call() throws Exception {
        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException(
                    String.format("'%s' is not a directory, or does not exist", dir));
        }

        writeObject(dir.resolve("hundred.qst"), TableSpec.empty(100).view("I=i"));

        writeObject(dir.resolve("chain-128.qst"), chain(128));
        writeObject(dir.resolve("chain-256.qst"), chain(256));
        writeObject(dir.resolve("chain-512.qst"), chain(512));
        writeObject(dir.resolve("chain-1024.qst"), chain(1024));
        writeObject(dir.resolve("chain-2048.qst"), chain(2048));
        writeObject(dir.resolve("chain-4096.qst"), chain(4096));
        writeObject(dir.resolve("chain-8192.qst"), chain(8192));

        return null;
    }

    private TableSpec chain(long size) {
        TableSpec table = TableSpec.empty(size * 2).view("I=i");
        for (long i = 0; i < size; ++i) {
            table = table.head(2 * size - 2 * i);
            table = table.tail(2 * size - 2 * i - 1);
        }
        return table;
    }

    private void writeObject(Path path, Object o) throws IOException {
        try (final OutputStream out = Files.newOutputStream(path, StandardOpenOption.CREATE);
                final ObjectOutputStream objectOut = new ObjectOutputStream(out)) {
            objectOut.writeObject(o);
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new WriteExampleQsts()).execute(args);
        System.exit(execute);
    }
}
