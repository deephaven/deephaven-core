//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.integration;

import io.deephaven.client.integration.ExampleRunner.Project;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
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
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the default VM options each Java major version gets from the launchers, against the options written out below.
 * Changing the options in {@code buildSrc/src/main/groovy/io.deephaven.java-toolchain-conventions.gradle} without
 * updating them here fails these tests. When the build overrides the defaults ({@code deephaven.javaOpts} or
 * {@code JAVA_OPTS}), every version gets the override instead, which the build passes as {@code dh.javaOptsOverride}.
 *
 * <p>
 * The start script cases run it with a stand-in {@code java} that prints the arguments it is given, and check the
 * options in the file it passes. The stand-in's Java home has a {@code release} file copied from the test JDK, naming
 * another version, or missing; calls the script makes to read the version with {@code -XshowSettings} go to the test
 * JDK. {@link ExamplesSmokeTest} starts the launchers on the test JDK itself. The test JDK is chosen with
 * {@code -PtestRuntimeVersion} (and by the nightly matrix).
 */
@Timeout(120)
class VmOptionsFileTest {

    /** A version with no file of its own. */
    private static final int UNLISTED_VERSION = 22;

    /** The build's override of the default options, possibly empty; null when there is none. */
    private static final List<String> OVERRIDE =
            Optional.ofNullable(System.getProperty("dh.javaOptsOverride")).map(VmOptionsFileTest::split).orElse(null);

    private static final List<String> GENERIC_OPTIONS = List.of(
            "-XX:+UseG1GC",
            "-XX:MaxGCPauseMillis=100",
            "-XX:+UseStringDeduplication",
            "--enable-native-access=ALL-UNNAMED");

    private static final List<String> GC_LOCKER_OPTIONS = List.of("-XX:GCLockerRetryAllocationCount=128");

    /** Every option any version gets by default. */
    private static final List<String> ALL_OPTIONS = concat(GENERIC_OPTIONS, GC_LOCKER_OPTIONS);

    /** The options a Java major version gets by default. */
    private static List<String> expectedOptions(int javaVersion) {
        if (OVERRIDE != null) {
            return OVERRIDE;
        }
        return switch (javaVersion) {
            case 17, 21 -> concat(GENERIC_OPTIONS, GC_LOCKER_OPTIONS);
            default -> GENERIC_OPTIONS;
        };
    }

    /** Each file the distribution ships, with its options. */
    private static final Map<String, List<String>> EXPECTED_FILES = Map.of(
            "dh-default.vmoptions", expectedOptions(UNLISTED_VERSION),
            "dh-default-17.vmoptions", expectedOptions(17),
            "dh-default-21.vmoptions", expectedOptions(21),
            "dh-default-25.vmoptions", expectedOptions(25));

    private static final String START_SCRIPT_ON_WINDOWS =
            "The start script is POSIX-only; a Windows launcher needs its own VM options tests";

    private static final String VM_OPTIONS_FILE_ARG = "-XX:VMOptionsFile=";

    /** Any launcher will do; they all come from the same start script template. */
    private static final String LAUNCHER = "connect-check";

    /** Prefixes each argument the stand-in {@code java} prints. */
    private static final String ARG_PREFIX = "stand-in java arg: ";

    private static final Pattern OPTS_VARIABLE = Pattern.compile("JAVA_OPTS and (\\w+) to pass");

    private static final Path JAVA_HOME = Paths.get(System.getProperty("java.home"));
    private static final int JAVA_VERSION = Runtime.version().feature();

    private static List<String> concat(List<String> first, List<String> second) {
        return Stream.concat(first.stream(), second.stream()).collect(Collectors.toList());
    }

    private static List<String> split(String options) {
        return options.isBlank() ? List.of() : Arrays.asList(options.trim().split("\\s+"));
    }

    /** The options in a VM options file. */
    private static List<String> optionsIn(Path vmOptionsFile) throws IOException {
        return split(Files.readString(vmOptionsFile));
    }

    @Test
    void distributionShipsTheExpectedOptions() throws IOException {
        try (Stream<Path> files = Files.list(etcDir())) {
            assertThat(files.map(f -> f.getFileName().toString()).filter(f -> f.endsWith(".vmoptions")))
                    .containsExactlyInAnyOrderElementsOf(EXPECTED_FILES.keySet());
        }
        for (Map.Entry<String, List<String>> file : EXPECTED_FILES.entrySet()) {
            assertThat(optionsIn(etcDir().resolve(file.getKey())))
                    .as(file.getKey())
                    .containsExactlyInAnyOrderElementsOf(file.getValue());
        }
    }

    @Test
    void gradleTestJvm() {
        final List<String> args = ManagementFactory.getRuntimeMXBean().getInputArguments();
        for (String option : concat(ALL_OPTIONS, expectedOptions(JAVA_VERSION))) {
            if (expectedOptions(JAVA_VERSION).contains(option)) {
                assertThat(args).contains(option);
            } else {
                assertThat(args).doesNotContain(option);
            }
        }
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = START_SCRIPT_ON_WINDOWS)
    void startScriptWithJavaHome(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.COPY);
        assertPasses(launchArgs(Map.of("JAVA_HOME", home.toString())), JAVA_VERSION);
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = START_SCRIPT_ON_WINDOWS)
    void startScriptWithJavaOnPath(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.COPY);
        final String path = home.resolve("bin") + File.pathSeparator + System.getenv("PATH");
        assertPasses(launchArgs(Map.of("PATH", path)), JAVA_VERSION);
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = START_SCRIPT_ON_WINDOWS)
    void startScriptFallsBackForUnlistedVersion(@TempDir Path dir) throws Exception {
        assertThat(etcDir().resolve("dh-default-" + UNLISTED_VERSION + ".vmoptions")).doesNotExist();
        final Path home = standInJavaHome(dir, Release.UNLISTED);
        assertPasses(launchArgs(Map.of("JAVA_HOME", home.toString())), UNLISTED_VERSION);
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = START_SCRIPT_ON_WINDOWS)
    void startScriptAsksJavaWhenThereIsNoReleaseFile(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.NONE);
        assertPasses(launchArgs(Map.of("JAVA_HOME", home.toString())), JAVA_VERSION);
    }

    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = START_SCRIPT_ON_WINDOWS)
    void startScriptSkipsTheDefaultsWhenJavaOptsIsSet(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.COPY);
        final List<String> args = launchArgs(Map.of("JAVA_HOME", home.toString(), "JAVA_OPTS", "-Xss2m"));
        assertThat(args).contains("-Xss2m");
        assertThat(vmOptionsFile(args)).as("%s in %s", VM_OPTIONS_FILE_ARG, args).isEmpty();
    }

    /**
     * With CDPATH set, cd prints the directory it changes to, which once corrupted the script's APP_HOME
     * (https://github.com/gradle/gradle/issues/25036).
     */
    @Test
    @DisabledOnOs(value = OS.WINDOWS, disabledReason = START_SCRIPT_ON_WINDOWS)
    void startScriptIgnoresCdPath(@TempDir Path dir) throws Exception {
        final Path home = standInJavaHome(dir, Release.COPY);
        assertPasses(launchArgs(Map.of("JAVA_HOME", home.toString(), "CDPATH", ".")), JAVA_VERSION);
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

    /** Checks that the launcher passed a file from the distribution holding the options for {@code javaVersion}. */
    private static void assertPasses(List<String> args, int javaVersion) throws IOException {
        final Optional<String> passed = vmOptionsFile(args);
        assertThat(passed).as("%s in %s", VM_OPTIONS_FILE_ARG, args).isPresent();
        final Path file = Paths.get(passed.get()).toRealPath();
        assertThat(file.getParent()).isEqualTo(etcDir().toRealPath());
        assertThat(optionsIn(file)).as(file.getFileName().toString())
                .containsExactlyInAnyOrderElementsOf(expectedOptions(javaVersion));
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

    /**
     * Runs the launcher as {@code bin/<launcher>} from its distribution directory, the way users run it, with
     * {@code launcherOpts} in its options variable, capturing stdout and stderr together.
     */
    private static Launch launch(Map<String, String> env, String launcherOpts) throws IOException,
            InterruptedException {
        final Path binDir = Project.SESSION.binDir();
        final Path script = binDir.resolve(LAUNCHER);
        final Matcher optsVariable = OPTS_VARIABLE.matcher(Files.readString(script));
        assertThat(optsVariable.find()).as("options variable in " + script).isTrue();

        final ProcessBuilder builder = new ProcessBuilder(binDir.getFileName().resolve(LAUNCHER).toString())
                .directory(binDir.getParent().toFile())
                .redirectErrorStream(true);
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
