//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Executes code in a server console and prints the variables it created, updated, or removed. Without a CODE argument,
 * reads scripts from standard input as a REPL.
 */
@Command(name = "execute-code", mixinStandardHelpOptions = true,
        description = "Execute code", version = "0.1.0")
class ExecuteCode implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true, multiplicity = "1")
    ScriptTypeOptions scriptType;

    @Parameters(arity = "0..1", paramLabel = "CODE",
            description = "The code to send. If not specified, reads from standard input.")
    String code;

    @Override
    public Void call() throws Exception {
        // The scheduler runs the client's background work, such as refreshing the session token
        final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        // The factory holds the connection; each session it opens is one authenticated login on that connection
        final SessionFactoryConfig.Factory factory = SessionFactoryConfig.builder()
                .clientConfig(ConnectOptions.options(connectOptions).config())
                .sessionConfig(AuthenticationOptions.sessionConfig(authenticationOptions))
                .scheduler(scheduler)
                .build()
                .factory();
        // A console runs scripts in the server's global scope; its language must match the server's
        try (
                final Session session = factory.newSession();
                final ConsoleSession console = session.console(scriptType.consoleType()).get()) {
            if (code != null) {
                System.out.println(ChangesFormatter.toPrettyString(console.executeCode(code)));
                return null;
            }
            System.out.println(
                    "Console REPL mode. To execute the current script, enter Ctrl+d. To exit, execute an empty script.");
            System.out.println();
            while (true) {
                System.out.print(">>> ");
                System.out.flush();
                final String script = standardInput();
                if (script.isEmpty()) {
                    return null;
                }
                System.out.println();
                System.out.println(ChangesFormatter.toPrettyString(console.executeCode(script)));
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
    }

    private static String standardInput() {
        final Scanner scanner =
                new Scanner(new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8)));
        final List<String> lines = new ArrayList<>();
        while (scanner.hasNextLine()) {
            lines.add(scanner.nextLine());
            System.out.print(">>> ");
            System.out.flush();
        }
        return String.join(System.lineSeparator(), lines);
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ExecuteCode()).execute(args));
    }
}
