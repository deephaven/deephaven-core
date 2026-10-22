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
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the launchers pass {@code -XX:VMOptionsFile} for the Java major version they run on, falling back to the
 * generic {@code dh-default.vmoptions} for a version without its own file.
 *
 * <p>
 * The start script cases run it with a stand-in {@code java} that prints the arguments it is given. The stand-in's Java
 * home has a {@code release} file copied from the test JDK, naming another version, or missing; calls the script makes
 * to read the version with {@code -XshowSettings} go to the test JDK. {@link ExamplesSmokeTest} starts the launchers on
 * the test JDK itself. The test JDK is chosen with {@code -PtestRuntimeVersion} (and by the nightly matrix).
 */
@Timeout(120)
class VmOptionsFileTest {

    private static final String VM_OPTIONS_FILE_ARG = "-XX:VMOptionsFile=";
    private static final String GENERIC_FILE = "dh-default.vmoptions";

    /** A version with no file of its own. */
    private static final int UNLISTED_VERSION = 22;

    /** Any launcher will do; they all come from the same start script template. */
    private static final String LAUNCHER = "connect-check";

    /** Prefixes each argument the stand-in {@code java} prints. */
    private static final String ARG_PREFIX = "stand-in java arg: ";

    private static final Pattern OPTS_VARIABLE = Pattern.compile("JAVA_OPTS and (\\w+) to pass");

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

    /**
     * This JVM's command line. ProcessHandle returns no arguments when the command line is long (for example, a long
     * classpath), so Linux's complete /proc/self/cmdline is preferred when it exists.
     */
    private static List<String> currentProcessArgs() throws IOException {
        final Path cmdline = Paths.get("/proc/self/cmdline");
        if (Files.isReadable(cmdline)) {
            return Arrays.asList(Files.readString(cmdline).split("\0"));
        }
        return Arrays.asList(ProcessHandle.current().info().arguments().orElseThrow());
    }

    @Test
    void gradleTestJvm() throws IOException {
        final List<String> args = currentProcessArgs();
        final Optional<String> loaded = vmOptionsFile(args);
        assertThat(loaded).as("%s in %s", VM_OPTIONS_FILE_ARG, args).isPresent();
        assertThat(Paths.get(loaded.get())).isRegularFile().hasFileName(expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptWithJavaHome(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.COPY);
        assertPasses(launchArgs(Map.of("JAVA_HOME", home.toString())), expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptWithJavaOnPath(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.COPY);
        final String path = home.resolve("bin") + File.pathSeparator + System.getenv("PATH");
        assertPasses(launchArgs(Map.of("PATH", path)), expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptFallsBackForUnlistedVersion(@TempDir Path dir) throws Exception {
        assertThat(etcDir().resolve("dh-default-" + UNLISTED_VERSION + ".vmoptions")).doesNotExist();
        final Path home = standInJavaHome(dir, Release.UNLISTED);
        assertPasses(launchArgs(Map.of("JAVA_HOME", home.toString())), GENERIC_FILE);
    }

    @Test
    void startScriptAsksJavaWhenThereIsNoReleaseFile(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.NONE);
        assertPasses(launchArgs(Map.of("JAVA_HOME", home.toString())), expectedFile(JAVA_VERSION));
    }

    @Test
    void startScriptSkipsTheDefaultsWhenJavaOptsIsSet(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.COPY);
        final List<String> args = launchArgs(Map.of("JAVA_HOME", home.toString(), "JAVA_OPTS", "-Xss2m"));
        assertThat(args).contains("-Xss2m");
        assertThat(vmOptionsFile(args)).as("%s in %s", VM_OPTIONS_FILE_ARG, args).isEmpty();
    }

    private static Path etcDir() {
        return Project.SESSION.binDir().resolveSibling("etc");
    }

    private static Optional<String> vmOptionsFile(List<String> args) {
        final List<String> files = args.stream()
                .filter(arg -> arg.startsWith(VM_OPTIONS_FILE_ARG))
                .map(arg -> arg.substring(VM_OPTIONS_FILE_ARG.length()))
                .collect(Collectors.toList());
        assertThat(files).as("%s arguments in %s", VM_OPTIONS_FILE_ARG, args).hasSizeLessThanOrEqualTo(1);
        return files.stream().findFirst();
    }

    private static void assertPasses(List<String> args, String expected) throws IOException {
        final Optional<String> passed = vmOptionsFile(args);
        assertThat(passed).as("%s in %s", VM_OPTIONS_FILE_ARG, args).isPresent();
        assertThat(Paths.get(passed.get()).toRealPath()).isEqualTo(etcDir().resolve(expected).toRealPath());
    }

    private enum Release {
        /** The test JDK's release file. */
        COPY,
        /** A release file naming {@link #UNLISTED_VERSION}. */
        UNLISTED,
        /** No release file. */
        NONE
    }

    /**
     * A Java home whose {@code bin/java} prints its arguments, except that it hands {@code -XshowSettings} calls (the
     * start script's way of asking the version when there is no release file) to the test JDK.
     */
    private static Path standInJavaHome(Path dir, Release release) throws IOException {
        final Path java = Files.createDirectories(dir.resolve("bin")).resolve("java");
        Files.writeString(java, String.join("\n",
                "#!/bin/sh",
                "case \"$1\" in -XshowSettings:*) exec '" + JAVA_HOME.resolve("bin").resolve("java")
                        + "' \"$@\" ;; esac",
                "for arg in \"$@\"; do printf '%s%s\\n' '" + ARG_PREFIX + "' \"$arg\"; done",
                ""));
        Files.setPosixFilePermissions(java, PosixFilePermissions.fromString("rwxr-xr-x"));
        switch (release) {
            case COPY -> Files.copy(JAVA_HOME.resolve("release"), dir.resolve("release"));
            case UNLISTED ->
                Files.writeString(dir.resolve("release"), "JAVA_VERSION=\"" + UNLISTED_VERSION + ".0.2\"\n");
            case NONE -> {
            }
        }
        return dir;
    }

    /** Runs the launcher with a stand-in {@code java} and returns the arguments it passed. */
    private static List<String> launchArgs(Map<String, String> env) throws IOException, InterruptedException {
        final Launch launch = launch(env, "");
        assertThat(launch.exitCode).as(launch.output).isZero();
        return launch.output.lines()
                .filter(line -> line.startsWith(ARG_PREFIX))
                .map(line -> line.substring(ARG_PREFIX.length()))
                .collect(Collectors.toList());
    }

    private static final class Launch {
        final int exitCode;
        final String output;

        Launch(int exitCode, String output) {
            this.exitCode = exitCode;
            this.output = output;
        }
    }

    /** Runs the launcher with {@code launcherOpts} in its options variable, capturing stdout and stderr together. */
    private static Launch launch(Map<String, String> env, String launcherOpts) throws IOException,
            InterruptedException {
        final Path script = Project.SESSION.binDir().resolve(LAUNCHER);
        final Matcher optsVariable = OPTS_VARIABLE.matcher(Files.readString(script));
        assertThat(optsVariable.find()).as("options variable in " + script).isTrue();

        final ProcessBuilder builder = new ProcessBuilder(script.toString()).redirectErrorStream(true);
        builder.environment().keySet().removeAll(List.of("JAVA_HOME", "JAVA_OPTS", optsVariable.group(1)));
        builder.environment().putAll(env);
        builder.environment().put(optsVariable.group(1), launcherOpts);

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
        return new Launch(process.exitValue(), output);
    }
}
