//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.integration.ExampleRunner.Project;
import io.deephaven.client.integration.ExampleRunner.Result;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static io.deephaven.client.integration.ExampleRunner.Project.BARRAGE;
import static io.deephaven.client.integration.ExampleRunner.Project.FLIGHT;
import static io.deephaven.client.integration.ExampleRunner.Project.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs every runnable example from {@code java-client/*-examples} once against the Docker server and checks that it
 * exits cleanly and prints what it is expected to print. This guards the examples against rot; it is not a substitute
 * for API-level tests.
 *
 * <p>
 * The examples run one at a time because several publish fixed variable names on the server. A small setup step creates
 * the variables that the ticket-taking examples reference.
 *
 * <p>
 * Not covered: {@code message-stream-send-receive} needs the echo plugin, {@code fetch-object} and
 * {@code convert-to-table} need a plugin object, none of which the server image has; {@code do-put-spray} copies
 * between two servers; and {@code Example1..3}, {@code Sum} and {@code SubscribeQST} have no launcher script.
 */
class ExamplesSmokeTest {

    /** Generous for CI: a JVM start plus a gRPC connection, well under a minute even on a loaded runner. */
    private static final Duration TIMEOUT = Duration.ofSeconds(90);

    private static final String STATIC_TABLE = "smoke_static";
    private static final String TICKING_TABLE = "smoke_ticking";

    private static final Path OUTPUT_DIR = Paths.get("build", "example-output");

    private static ExampleRunner runner;

    /** One smoke row: which launcher to run, with what arguments, and what its stdout must contain. */
    private static final class Example {
        final Project project;
        final String script;
        final List<String> args;
        final Pattern expectedStdout;

        Example(Project project, String script, String expectedStdout, String... args) {
            this.project = project;
            this.script = script;
            this.args = Arrays.asList(args);
            this.expectedStdout = Pattern.compile(expectedStdout, Pattern.MULTILINE);
        }

        String displayName() {
            return script + (args.isEmpty() ? "" : " " + String.join(" ", args));
        }
    }

    private static Example example(Project project, String script, String expectedStdout, String... args) {
        return new Example(project, script, expectedStdout, args);
    }

    private static List<Example> examples() throws Exception {
        final Path script = Files.createTempFile("smoke", ".py");
        // The console reports changes to displayable variables such as tables, not to plain Python values
        Files.writeString(script, "from deephaven import empty_table\nsmoke_script_ran = empty_table(1)\n");
        return Arrays.asList(
                // session
                example(SESSION, "connect-check", "Connected to session"),
                example(SESSION, "print-configuration-constants", "^\\S+=.*$"),
                example(SESSION, "execute-code", "smoke_code_ran", "--python",
                        "from deephaven import empty_table\nsmoke_code_ran = empty_table(1)"),
                example(SESSION, "execute-script", "smoke_script_ran", "--python", script.toString()),
                example(SESSION, "publish", "", "--variable", STATIC_TABLE, "--variable", "smoke_published"),
                example(SESSION, "filter-table", "", "--variable", STATIC_TABLE, "I > 5"),
                example(SESSION, "table-manager", "Stage"),
                example(SESSION, "subscribe-fields", "Created: ", "-c", "1"),
                example(SESSION, "subscribe-to-logs", "", "-q", "-c", "1", "--timeout", "PT30S"),
                example(SESSION, "create-shared-id", "shared id: 0x", "--duration", "PT1S"),
                // flight
                example(FLIGHT, "get-tsv", "duration"),
                example(FLIGHT, "poll-tsv", "", "-c", "2"),
                example(FLIGHT, "list-tables", STATIC_TABLE),
                example(FLIGHT, "excessive", "duration", "-c", "16"),
                example(FLIGHT, "aggregate-all", "", "--cycles", "2", "--sleep-millis", "10"),
                example(FLIGHT, "agg-by", "", "--cycles", "2", "--sleep-millis", "10"),
                example(FLIGHT, "do-exchange", "", "--variable", STATIC_TABLE),
                example(FLIGHT, "do-put-new", "", "smoke_do_put_new"),
                example(FLIGHT, "do-put-table", "", "--variable", "smoke_do_put_table"),
                example(FLIGHT, "add-to-input-table", "Int -> Value", "--rows", "3", "--sleep-millis", "10"),
                example(FLIGHT, "add-to-blink-table", ""),
                example(FLIGHT, "kv-input-table", "", "smoke_key", "smoke_value"),
                example(FLIGHT, "get-table", "Table received: 10 rows", "--variable", STATIC_TABLE),
                example(FLIGHT, "get-schema", "I", "--variable", STATIC_TABLE),
                // barrage
                example(BARRAGE, "snapshot-table", "Table info", "--variable", STATIC_TABLE),
                example(BARRAGE, "subscribe-table", "Received table update", "--variable", TICKING_TABLE, "--updates",
                        "1"));
    }

    @BeforeAll
    static void createFixtures() throws Exception {
        runner = new ExampleRunner(OUTPUT_DIR);
        final Result result = runner.run(SESSION, "execute-code", TIMEOUT, "--python", String.join("\n",
                "from deephaven import empty_table, time_table",
                STATIC_TABLE + " = empty_table(10).update('I = ii')",
                TICKING_TABLE + " = time_table('PT0.2S')"));
        assertThat(result.exitCode).as("fixture setup: " + result.describe()).isZero();
    }

    @TestFactory
    Stream<DynamicTest> examplesRun() throws Exception {
        return examples().stream().map(e -> DynamicTest.dynamicTest(e.displayName(), () -> check(e)));
    }

    private static void check(Example e) throws Exception {
        final Result result = runner.run(e.project, e.script, TIMEOUT, e.args.toArray(new String[0]));
        assertThat(result.timedOut).as("timed out after " + TIMEOUT + ": " + result.describe()).isFalse();
        assertThat(result.exitCode).as(result.describe()).isZero();
        assertThat(result.stdout).as("stdout did not match /" + e.expectedStdout + "/: " + result.describe())
                .containsPattern(e.expectedStdout);
    }
}
