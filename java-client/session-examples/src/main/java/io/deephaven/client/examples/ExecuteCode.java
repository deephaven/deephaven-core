package io.deephaven.client.examples;

import io.deephaven.client.impl.ConsoleSession;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.stream.Collectors;

@Command(name = "execute-code", mixinStandardHelpOptions = true,
        description = "Execute code", version = "0.1.0")
class ExecuteCode extends ConsoleExampleBase {

    @Parameters(arity = "0..1", paramLabel = "CODE",
            description = "The code to send. If not specified, reads from standard input.")
    String code;

    @Override
    protected void execute(ConsoleSession consoleSession) throws Exception {
        if (code == null) {
            System.out.println(
                    "Console REPL mode. To execute the current script, enter Ctrl+d. To exit, execute an empty script.");
            System.out.println();
            while (true) {
                System.out.print(">>> ");
                System.out.flush();
                code = standardInput();
                if (code.isEmpty()) {
                    return;
                }
                System.out.println();
                System.out.println(toPrettyString(consoleSession.executeCode(code)));
            }
        } else {
            System.out.println(toPrettyString(consoleSession.executeCode(code)));
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
        int execute = new CommandLine(new ExecuteCode()).execute(args);
        System.exit(execute);
    }
}
