/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.plugin.type.JsTypeBase;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;

/**
 * A {@link io.deephaven.plugin.type.JsType} implementation sourced from a package.json file.
 */
public final class JsTypePackageJson extends JsTypeBase {

    static final class PackageJson {
        private final String name;
        private final String version;

        @JsonCreator
        public PackageJson(
                @JsonProperty(value = "name", required = true) String name,
                @JsonProperty(value = "version", required = true) String version) {
            this.name = name;
            this.version = version;
        }

        public String name() {
            return name;
        }

        public String version() {
            return version;
        }
    }

    /**
     * Creates a new instance.
     *
     * @param packageJsonFile the package.json file
     * @return the JS type
     * @throws IOException if an I/O exception occurs
     */
    public static JsTypePackageJson of(String packageJsonFile) throws IOException {
        final File file = new File(packageJsonFile);
        final byte[] bytes = Files.readAllBytes(Path.of(packageJsonFile));
        final PackageJson packageJson = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .readValue(bytes, PackageJson.class);
        return new JsTypePackageJson(file, packageJson, bytes);
    }

    private final File file;
    private final PackageJson packageJson;
    private final byte[] packageJsonContents;

    private JsTypePackageJson(File file, PackageJson packageJson, byte[] packageJsonContents) {
        this.file = Objects.requireNonNull(file);
        this.packageJson = Objects.requireNonNull(packageJson);
        this.packageJsonContents = Objects.requireNonNull(packageJsonContents);
    }

    @Override
    public String name() {
        return packageJson.name();
    }

    @Override
    public String version() {
        return packageJson.version();
    }

    @Override
    public void writeJsonPackageContentsTo(OutputStream out) throws IOException {
        out.write(packageJsonContents);
    }

    @Override
    public void copyTo(Path destination) throws IOException {
        copyRecursive(file.toPath().getParent(), destination);
    }

    private static void copyRecursive(Path src, Path dest) throws IOException {
        Files.createDirectories(dest.getParent());
        Files.walkFileTree(src, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                Files.copy(dir, dest.resolve(src.relativize(dir)));
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.copy(file, dest.resolve(src.relativize(file)));
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
