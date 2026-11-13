//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.examples;

import io.deephaven.client.impl.ConsoleSession;
import io.deephaven.client.impl.Session;
import io.deephaven.client.impl.SessionFactoryConfig;
import io.deephaven.client.impl.script.Changes;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Executes one or more script files in a server console, printing the variable changes after each.
 */
@Command(name = "execute-script", mixinStandardHelpOptions = true,
        description = "Execute a script", version = "0.1.0")
class ExecuteScript implements Callable<Void> {

    @ArgGroup(exclusive = false)
    ConnectOptions connectOptions;

    @ArgGroup(exclusive = true)
    AuthenticationOptions authenticationOptions;

    @ArgGroup(exclusive = true, multiplicity = "1")
    ScriptTypeOptions scriptType;

    @Parameters(arity = "1+", paramLabel = "SCRIPT", description = "The script to send.")
    List<Path> scripts;

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
            for (Path path : scripts) {
                final Changes changes = console.executeScript(path);
                System.out.println(path);
                System.out.println(ChangesFormatter.toPrettyString(changes));
            }
        } finally {
            factory.managedChannel().shutdownNow();
            scheduler.shutdownNow();
        }
        return null;
    }

    public static void main(String[] args) {
        System.exit(new CommandLine(new ExecuteScript()).execute(args));
    }
}
