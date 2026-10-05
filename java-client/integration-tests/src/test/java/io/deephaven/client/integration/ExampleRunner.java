//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Runs an example launcher script from one of the {@code java-client/*-examples} distributions as a child process
 * against the test server, capturing its output.
 *
 * <p>
 * The test task sets {@code dh.port} to the server's host port and {@code dh.examples.<distribution>.bin} to each
 * distribution's {@code bin} directory. The child JVM is the one running the tests unless {@code dh.examples.javaHome}
 * names another, which is how the same examples can be exercised on other runtimes.
 */
final class ExampleRunner {

    /** The example distributions, by the name used in the {@code dh.examples.<distribution>.bin} properties. */
    enum Project {
        SESSION("java-client-session-examples"), FLIGHT("java-client-flight-examples"), BARRAGE(
                "java-client-barrage-examples");

        private final String key;

        Project(String key) {
            this.key = key;
        }

        Path binDir() {
            return Paths.get(requireProperty("dh.examples." + key + ".bin"));
        }
    }

    /** The outcome of one example run. */
    static final class Result {
        final int exitCode;
        final boolean timedOut;
        final String stdout;
        final String stderr;

        private Result(int exitCode, boolean timedOut, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.timedOut = timedOut;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        String describe() {
            return (timedOut ? "timed out" : "exit code " + exitCode)
                    + "\n--- stdout ---\n" + stdout
                    + "\n--- stderr ---\n" + stderr;
        }
    }

    private final Path outputDir;

    ExampleRunner(Path outputDir) {
        this.outputDir = outputDir;
    }

    /** The server target accepted by every example's {@code --target} option. */
    static String target() {
        return "dh+plain://localhost:" + requireProperty("dh.port");
    }

    static String requireProperty(String name) {
        final String value = System.getProperty(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Missing system property " + name + "; run via Gradle");
        }
        return value;
    }

    Result run(Project project, String script, Duration timeout, String... args) throws IOException,
            InterruptedException {
        final List<String> command = new ArrayList<>();
        command.add(project.binDir().resolve(script).toString());
        command.add("--target");
        command.add(target());
        command.addAll(Arrays.asList(args));

        Files.createDirectories(outputDir);
        final Path stdout = outputDir.resolve(script + ".out");
        final Path stderr = outputDir.resolve(script + ".err");

        final ProcessBuilder builder = new ProcessBuilder(command)
                .redirectOutput(stdout.toFile())
                .redirectError(stderr.toFile());
        builder.environment().put("JAVA_HOME",
                System.getProperty("dh.examples.javaHome", System.getProperty("java.home")));

        final Process process = builder.start();
        final boolean exited = process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS);
        if (!exited) {
            process.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
        }
        // Some examples write raw bytes to stdout (fetch-object), so decode leniently rather than strictly
        return new Result(
                exited ? process.exitValue() : -1,
                !exited,
                new String(Files.readAllBytes(stdout), StandardCharsets.UTF_8),
                new String(Files.readAllBytes(stderr), StandardCharsets.UTF_8));
    }
}
