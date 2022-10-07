/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.plugin.type.JsPlugin;
import io.deephaven.plugin.type.JsPluginBase;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/**
 * A {@link JsPlugin} implementation sourced from a distribution directory.
 */
public final class JsPluginDistribution extends JsPluginBase {

    private static final String PACKAGE_JSON = "package.json";

    /**
     * Creates a new js plugin distribution. Assumes that {@value PACKAGE_JSON} exists in {@code distributionDir}. The
     * {@link #name() name}, {@link #version() version}, and {@link #main() main} from {@value PACKAGE_JSON} will be
     * used.
     *
     * @param distributionDir the distribution directory
     * @return the js plugin distribution
     * @throws IOException if an I/O exception occurs
     */
    public static JsPluginDistribution fromDistribution(Path distributionDir) throws IOException {
        final Path packageJson = distributionDir.resolve(PACKAGE_JSON);
        try (final InputStream in = new BufferedInputStream(Files.newInputStream(packageJson))) {
            final PackageJson p = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(in, PackageJson.class);
            // Note: we could provide an option, or do it by default, to only copy over p.main, or p.main.parent, if we
            // wanted to minimize the amount of data we re-expose.
            return new JsPluginDistribution(distributionDir, p.name, p.version, p.main);
        }
    }

    private final Path distributionDir;
    private final String name;
    private final String version;
    private final String main;

    /**
     * Creates a new js plugin distribution.
     *
     * <p>
     * Note: unlike {@link #fromDistribution(Path)}, {@code distributionDir} does not need to contain
     * {@value PACKAGE_JSON}.
     *
     * @param distributionDir the distribution directory
     * @param name the name, see {@link JsPlugin#name()}
     * @param version the version, see {@link JsPlugin#version()}
     * @param main the main, see {@link JsPlugin#main()}
     */
    public JsPluginDistribution(Path distributionDir, String name, String version, String main) {
        this.distributionDir = Objects.requireNonNull(distributionDir);
        this.name = Objects.requireNonNull(name);
        this.version = Objects.requireNonNull(version);
        this.main = Objects.requireNonNull(main);
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public String version() {
        return version;
    }

    @Override
    public String main() {
        return main;
    }

    @Override
    public void copyTo(Path destination) throws IOException {
        copyRecursive(distributionDir, destination);
    }

    private static void copyRecursive(Path src, Path dst) throws IOException {
        Files.createDirectories(dst.getParent());
        Files.walkFileTree(src, new CopyRecursiveVisitor(src, dst));
    }

    static final class PackageJson {
        private final String name;
        private final String version;
        private final String main;

        @JsonCreator
        public PackageJson(
                @JsonProperty(value = "name", required = true) String name,
                @JsonProperty(value = "version", required = true) String version,
                @JsonProperty(value = "main", required = true) String main) {
            this.name = Objects.requireNonNull(name);
            this.version = Objects.requireNonNull(version);
            this.main = Objects.requireNonNull(main);
        }
    }

    private static class CopyRecursiveVisitor extends SimpleFileVisitor<Path> {
        private final Path src;
        private final Path dst;

        public CopyRecursiveVisitor(Path src, Path dst) {
            this.src = Objects.requireNonNull(src);
            this.dst = Objects.requireNonNull(dst);
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            Files.copy(dir, dst.resolve(src.relativize(dir).toString()));
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            Files.copy(file, dst.resolve(src.relativize(file).toString()));
            return FileVisitResult.CONTINUE;
        }
    }
}
