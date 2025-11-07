//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.iceberg.sqlite;

import io.deephaven.iceberg.util.IcebergCatalogAdapter;
import io.deephaven.iceberg.util.IcebergTools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.rules.TemporaryFolder;

/**
 * This is a junit extension that unpacks the sqlite db_resource directory into a temporary folder before each unit test
 * class, and then removes it after wards.
 */
public class DbResource implements BeforeAllCallback, AfterAllCallback {
    public static final String PATH_IN_JAR = "io/deephaven/iceberg/sqlite/db_resource";
    public static final String JAR_NAME = "iceberg-test-data.jar";
    private final TemporaryFolder folder = new TemporaryFolder();


    @Override
    public void afterAll(final ExtensionContext context) throws Exception {
        folder.delete();
    }

    @Override
    public void beforeAll(final ExtensionContext context) throws Exception {
        folder.create();
        unpackTo(JAR_NAME, PATH_IN_JAR, folder.getRoot().toPath());
    }

    private Path getRoot() {
        return folder.getRoot().toPath().resolve(PATH_IN_JAR);
    }

    /**
     * Open a catalog using the default resource directory. This should only be used when the resources are not
     * modified. For example, when reading from tables.
     */
    public IcebergCatalogAdapter openCatalog(final String catalogName) {
        return openCatalogImpl(catalogName, getRoot());
    }

    /**
     * Open a catalog using the provided root directory. This will copy the unpacked resources to the {@code rootDir}.
     * This method useful for creating a catalog when running tests which modify the resources, like writing to a table.
     */
    public IcebergCatalogAdapter openReadWriteCatalog(final String catalogName, final Path rootDir) throws IOException {
        FileUtils.copyDirectory(getRoot().toFile(), rootDir.toFile());
        return openCatalogImpl(catalogName, rootDir);
    }

    private static IcebergCatalogAdapter openCatalogImpl(final String catalogName, final Path rootDir) {
        final Map<String, String> properties = new HashMap<>();
        SqliteHelper.setJdbcCatalogProperties(properties, rootDir);
        SqliteHelper.setLocalFileIoProperties(properties, rootDir, catalogName, true);
        // Note: the catalogName is very important here, the JDBC catalog uses it for lookups. In this way, a single
        // dbFile can be used for multiple catalogs.
        return IcebergTools.createAdapter(catalogName, properties);
    }


    private static void unpackTo(String jarPath, String pathInJar, Path destination) throws IOException {
        final URL resource = DbResource.class.getClassLoader().getResource(jarPath);
        if (resource == null) {
            throw new FileNotFoundException(jarPath);
        }
        final URI resourceFile = URI.create(resource.getFile());
        unpackFromJar(resourceFile.getPath(), maybeStripLeadingSlash(pathInJar), destination);
    }

    private static void unpackFromJar(String jarPath, String pathInJar, Path destination) throws IOException {
        try (final ZipInputStream zif = new ZipInputStream(new FileInputStream(jarPath))) {
            for (ZipEntry current; (current = zif.getNextEntry()) != null;) {
                if (!current.getName().startsWith(pathInJar)) {
                    continue;
                }

                final Path resolvedPath = destination.resolve(current.getName()).normalize();
                if (!resolvedPath.startsWith(destination)) {
                    // see: https://snyk.io/research/zip-slip-vulnerability
                    throw new RuntimeException("Entry with an illegal path: "
                            + current.getName());
                }
                if (current.isDirectory()) {
                    Files.createDirectories(resolvedPath);
                } else {
                    Files.createDirectories(resolvedPath.getParent());
                    Files.copy(zif, resolvedPath);
                }
            }
        }
    }

    private static String maybeStripLeadingSlash(String part) {
        if (part.startsWith("/")) {
            return part.substring(1);
        }

        return part;
    }
}
