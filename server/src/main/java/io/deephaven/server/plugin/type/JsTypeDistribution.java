/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.plugin.type.JsTypeBase;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/**
 * A {@link io.deephaven.plugin.type.JsType} implementation sourced from a distribution directory.
 */
public final class JsTypeDistribution extends JsTypeBase {

    /**
     * Creates a new js type distribution from a package.json file. Assumes the parent of {@code packageJson} is the
     * distribution directory.
     *
     * @param packageJson the path
     * @return the js type distribution
     * @throws IOException if an I/O exception occurs
     */
    public static JsTypeDistribution fromPackageJson(Path packageJson) throws IOException {
        try (final BufferedInputStream in = new BufferedInputStream(Files.newInputStream(packageJson))) {
            final PackageJson p = new ObjectMapper()
                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                    .readValue(in, PackageJson.class);
            return new JsTypeDistribution(packageJson.getParent(), p.name, p.version, p.main);
        }
    }

    private final Path distributionDir;
    private final String name;
    private final String version;
    private final String main;

    public JsTypeDistribution(Path distributionDir, String name, String version, String main) {
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

    private static void copyRecursive(Path src, Path dest) throws IOException {
        Files.createDirectories(dest.getParent());
        Files.walkFileTree(src, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Files.copy(dir, dest.resolve(src.relativize(dir).toString()));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, dest.resolve(src.relativize(file).toString()));
                return FileVisitResult.CONTINUE;
            }
        });
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
            this.name = name;
            this.version = version;
            this.main = main;
        }
    }
}
