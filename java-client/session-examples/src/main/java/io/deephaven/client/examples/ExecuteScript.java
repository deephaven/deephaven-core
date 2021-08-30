package io.deephaven.client.examples;

import io.deephaven.client.impl.ConsoleSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;

@Command(name = "execute-script", mixinStandardHelpOptions = true,
        description = "Execute a script", version = "0.1.0")
class ExecuteScript extends ConsoleExampleBase {

    @Parameters(arity = "1", paramLabel = "SCRIPT", description = "The script to send.")
    Path script;

    @Override
    protected void execute(ConsoleSession consoleSession) throws Exception {
        System.out.println(toPrettyString(consoleSession.executeScript(script)));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ExecuteScript()).execute(args);
        System.exit(execute);
    }
}
