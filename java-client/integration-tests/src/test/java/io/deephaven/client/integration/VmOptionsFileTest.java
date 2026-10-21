//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.integration.ExampleRunner.Project;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the launchers load the VM options file for the Java major version they run on, and fall back to the
 * generic {@code dh-default.vmoptions} for a version without its own file. The JVM keeps no record of the file it read
 * through {@code -XX:VMOptionsFile}, so each launcher also passes the same path as the {@code deephaven.vmoptions.file}
 * system property, which these tests read.
 *
 * <p>
 * The cases that use the real JDK run on the test JVM's JDK, so {@code -PtestRuntimeVersion} (and the nightly matrix)
 * selects the version they check. The other start script cases use a stand-in Java home whose {@code release} file
 * names a different version or is missing; its {@code java} is still the test JDK's, which accepts the generic options.
 */
@Timeout(120)
class VmOptionsFileTest {

    private static final String PROPERTY = "deephaven.vmoptions.file";
    private static final String GENERIC_FILE = "dh-default.vmoptions";

    /** A version with no file of its own. */
    private static final int UNLISTED_VERSION = 22;

    /** Any launcher will do; they all come from the same start script template. */
    private static final String LAUNCHER = "connect-check";

    private static final Pattern OPTS_VARIABLE = Pattern.compile("JAVA_OPTS and (\\w+) to pass");
    private static final Pattern SHOWN_PROPERTY =
            Pattern.compile("^\\s*" + Pattern.quote(PROPERTY) + " = (.*)$", Pattern.MULTILINE);

    private static final Path JAVA_HOME = Paths.get(System.getProperty("java.home"));
    private static final int JAVA_VERSION = Runtime.version().feature();

    /**
     * The file a Java major version loads. Mirrors {@code VERSIONED_JAVA_OPTS} in
     * {@code buildSrc/src/main/groovy/io.deephaven.java-toolchain-conventions.gradle}.
     */
    private static String expectedFile(int javaVersion) {
        return switch (javaVersion) {
            case 17, 21, 25 -> "dh-default-" + javaVersion + ".vmoptions";
            default -> GENERIC_FILE;
        };
    }

    @Test
    void gradleTestJvm() {
        assertThat(Paths.get(ExampleRunner.requireProperty(PROPERTY)))
                .isRegularFile()
                .hasFileName(expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptWithJavaHome() throws Exception {
        assertLoaded(launch(Map.of("JAVA_HOME", JAVA_HOME.toString())), expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptWithJavaOnPath() throws Exception {
        final String path = JAVA_HOME.resolve("bin") + File.pathSeparator + System.getenv("PATH");
        assertLoaded(launch(Map.of("PATH", path)), expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptFallsBackForUnlistedVersion(@TempDir Path dir) throws Exception {
        assertThat(etcDir().resolve("dh-default-" + UNLISTED_VERSION + ".vmoptions")).doesNotExist();
        final Path home = standInJavaHome(dir, "JAVA_VERSION=\"" + UNLISTED_VERSION + ".0.2\"");
        assertLoaded(launch(Map.of("JAVA_HOME", home.toString())), GENERIC_FILE);
    }

    @Test
    void startScriptAsksJavaWhenThereIsNoReleaseFile(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, null);
        assertLoaded(launch(Map.of("JAVA_HOME", home.toString())), expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptSkipsTheDefaultsWhenJavaOptsIsSet() throws Exception {
        assertThat(launch(Map.of("JAVA_HOME", JAVA_HOME.toString(), "JAVA_OPTS", "-Xss2m"))).isEmpty();
    }

    private static Path etcDir() {
        return Project.SESSION.binDir().resolveSibling("etc");
    }

    private static void assertLoaded(Optional<String> loaded, String expected) throws IOException {
        assertThat(loaded).as(PROPERTY).isPresent();
        assertThat(Paths.get(loaded.get()).toRealPath()).isEqualTo(etcDir().resolve(expected).toRealPath());
    }

    /**
     * A Java home whose {@code bin/java} links to the test JDK's, with a {@code release} file holding
     * {@code releaseContents}, or no release file when that is null.
     */
    private static Path standInJavaHome(Path dir, String releaseContents) throws IOException {
        final Path bin = Files.createDirectories(dir.resolve("bin"));
        Files.createSymbolicLink(bin.resolve("java"), JAVA_HOME.resolve("bin").resolve("java"));
        if (releaseContents != null) {
            Files.writeString(dir.resolve("release"), releaseContents + "\n");
        }
        return dir;
    }

    /**
     * Runs the launcher with {@code -XshowSettings:properties -version} in its options variable, so the JVM prints its
     * system properties and exits before the example's main. Returns the VM options file the launcher recorded, if any.
     */
    private static Optional<String> launch(Map<String, String> env) throws IOException, InterruptedException {
        final Path script = Project.SESSION.binDir().resolve(LAUNCHER);
        final Matcher optsVariable = OPTS_VARIABLE.matcher(Files.readString(script));
        assertThat(optsVariable.find()).as("options variable in " + script).isTrue();

        final ProcessBuilder builder = new ProcessBuilder(script.toString()).redirectErrorStream(true);
        builder.environment().keySet().removeAll(List.of("JAVA_HOME", "JAVA_OPTS", optsVariable.group(1)));
        builder.environment().putAll(env);
        builder.environment().put(optsVariable.group(1), "-XshowSettings:properties -version");

        // Output goes to a file rather than a pipe, so a stalled launcher cannot block the test before the timeout
        final Path outputFile = Files.createTempFile("vmoptions-launch", ".out");
        final Process process;
        final boolean exited;
        final String output;
        try {
            process = builder.redirectOutput(outputFile.toFile()).start();
            try {
                exited = process.waitFor(30, TimeUnit.SECONDS);
            } finally {
                // The script may have started its own java (to read the version) before exec'ing the JVM
                process.descendants().forEach(ProcessHandle::destroyForcibly);
                process.destroyForcibly();
            }
            output = Files.readString(outputFile);
        } finally {
            Files.deleteIfExists(outputFile);
        }
        assertThat(exited).as("launcher exited within 30 seconds; output: " + output).isTrue();
        assertThat(process.exitValue()).as(output).isZero();

        final Matcher shown = SHOWN_PROPERTY.matcher(output);
        return shown.find() ? Optional.of(shown.group(1).trim()) : Optional.empty();
    }
}
