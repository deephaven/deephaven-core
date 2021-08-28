package io.deephaven.client.examples;

import io.deephaven.client.impl.Session;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.nio.file.Path;

@Command(name = "execute-script", mixinStandardHelpOptions = true,
        description = "Execute a script", version = "0.1.0")
class ExecuteScript extends SingleSessionExampleBase {

    /*
     * static class Type {
     * 
     * @Option(names = {"-p", "--python"}, required = true, description = "Python script type") boolean python;
     * 
     * @Option(names = {"-g", "--groovy"}, required = true, description = "Groovy script type") boolean groovy;
     * 
     * @Option(names = {"--other"}, description = "Other script type") String other; }
     * 
     * @ArgGroup(exclusive = true) Type type;
     */

    @Parameters(arity = "1", paramLabel = "SCRIPT", description = "The script to send.")
    Path script;

    @Override
    protected void execute(Session session) throws Exception {
        System.out.println(session.executeScript(script));
    }

    public static void main(String[] args) {
        int execute = new CommandLine(new ExecuteScript()).execute(args);
        System.exit(execute);
    }
}
