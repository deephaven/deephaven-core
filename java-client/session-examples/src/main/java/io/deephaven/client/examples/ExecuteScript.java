/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.client.examples;

import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.script.Changes;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.List;

@Command(name = "execute-script", mixinStandardHelpOptions = true,
        description = "Execute a script", version = "0.1.0")
class ExecuteScript extends ConsoleExampleBase {

    @Parameters(arity = "1+", paramLabel = "SCRIPT", description = "The script to send.")
    List<Path> scripts;

    @Override
    protected void execute(ConsoleSession consoleSession) throws Exception {
        for (Path path : scripts) {
            final Changes changes = consoleSession.executeScript(path);
            System.out.println(path);
            System.out.println(toPrettyString(changes));
        }
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ExecuteScript()).execute(args);
        System.exit(execute);
    }
}
