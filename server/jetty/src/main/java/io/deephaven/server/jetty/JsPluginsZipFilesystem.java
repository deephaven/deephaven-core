/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.configuration.CacheDir;
import io.deephaven.plugin.js.JsPluginManifestPath;
import io.deephaven.plugin.js.JsPluginPackagePath;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class JsPluginsZipFilesystem {
    private static final String ZIP_ROOT = "/";

    /**
     * Creates a new js plugins instance with a temporary zip filesystem.
     *
     * @return the js plugins
     * @throws IOException if an I/O exception occurs
     */
    public static JsPluginsZipFilesystem create() throws IOException {
        final Path tempDir =
                Files.createTempDirectory(CacheDir.get(), "." + JsPluginsZipFilesystem.class.getSimpleName());
        tempDir.toFile().deleteOnExit();
        final Path fsZip = tempDir.resolve("deephaven-js-plugins.zip");
        fsZip.toFile().deleteOnExit();
        final URI uri = URI.create(String.format("jar:file:%s!/", fsZip));
        final JsPluginsZipFilesystem jsPlugins = new JsPluginsZipFilesystem(uri);
        jsPlugins.init();
        return jsPlugins;
    }

    private final URI filesystem;
    private final List<JsPluginManifestEntry> entries;

    private JsPluginsZipFilesystem(URI filesystem) {
        this.filesystem = Objects.requireNonNull(filesystem);
        this.entries = new ArrayList<>();
    }

    public URI filesystem() {
        return filesystem;
    }

    public synchronized void copyFrom(JsPluginPackagePath srcPackagePath, JsPluginManifestEntry srcEntry)
            throws IOException {
        checkExisting(srcEntry);
        final Path srcDist = srcPackagePath.distributionPath(srcEntry.main());
        // TODO(deephaven-core#3005): js-plugins checksum-based caching
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of())) {
            final JsPluginManifestPath manifest = manifest(fs);
            final Path destDist = manifest
                    .packagePath(srcEntry.name())
                    .distributionPath(srcEntry.main());
            CopyHelper.copyRecursive(srcDist, destDist);
            entries.add(srcEntry);
            writeManifest(fs);
        }
    }

    private void checkExisting(JsPluginManifestEntry info) {
        for (JsPluginManifestEntry existing : entries) {
            if (info.name().equals(existing.name())) {
                // TODO(deephaven-core#3048): Improve JS plugin support around plugins with conflicting names
                throw new IllegalArgumentException(String.format(
                        "js plugin with name '%s' already exists. See https://github.com/deephaven/deephaven-core/issues/3048",
                        existing.name()));
            }
        }
    }

    private void init() throws IOException {
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of("create", "true"))) {
            writeManifest(fs);
        }
    }

    private void writeManifest(FileSystem fs) throws IOException {
        final Path manifestJson = manifest(fs).manifestJson();
        final Path manifestJsonTmp = manifestJson.resolveSibling(manifestJson.getFileName().toString() + ".tmp");
        // jackson impl does buffering internally
        try (final OutputStream out = Files.newOutputStream(manifestJsonTmp)) {
            new ObjectMapper().writeValue(out, JsPluginManifest.of(entries));
            out.flush();
        }
        Files.move(manifestJsonTmp, manifestJson,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.COPY_ATTRIBUTES,
                StandardCopyOption.ATOMIC_MOVE);
    }

    private static JsPluginManifestPath manifest(FileSystem fs) {
        return JsPluginManifestPath.of(fs.getPath(ZIP_ROOT));
    }
}
