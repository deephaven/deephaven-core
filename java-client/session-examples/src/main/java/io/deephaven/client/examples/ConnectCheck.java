package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import picocli.CommandLine;
import picocli.CommandLine.Command;

@Command(name = "connect-check", mixinStandardHelpOptions = true,
        description = "Connect check", version = "0.1.0")
class ConnectCheck extends SingleSessionExampleBase {

    @Override
    protected void execute(Session session) throws Exception {
        System.out.println("Connected to session: " + session);
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ConnectCheck()).execute(args);
        System.exit(execute);
    }
}
