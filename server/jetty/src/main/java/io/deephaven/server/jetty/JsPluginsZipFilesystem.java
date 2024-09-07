//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import io.deephaven.configuration.CacheDir;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.server.plugin.js.JsPluginManifest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.server.jetty.Json.OBJECT_MAPPER;
import static io.deephaven.server.plugin.js.JsPluginManifest.MANIFEST_JSON;

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
        final URI uri = URI.create(String.format("jar:%s!/", fsZip.toUri()));
        final JsPluginsZipFilesystem jsPlugins = new JsPluginsZipFilesystem(uri);
        jsPlugins.init();
        return jsPlugins;
    }

    private final URI filesystem;
    private final List<JsPlugin> plugins;

    private JsPluginsZipFilesystem(URI filesystem) {
        this.filesystem = Objects.requireNonNull(filesystem);
        this.plugins = new ArrayList<>();
    }

    public URI filesystem() {
        return filesystem;
    }

    public synchronized void add(JsPlugin plugin) throws IOException {
        checkExisting(plugin.name());
        // TODO(deephaven-core#3005): js-plugins checksum-based caching
        // Note: FileSystem#close is necessary to write out contents for ZipFileSystem
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of())) {
            final Path manifestRoot = manifestRoot(fs);
            final Path dstPath = manifestRoot.resolve(plugin.name());
            // This is using internal knowledge that paths() must be PathsInternal and extends PathsMatcher.
            final PathMatcher pathMatcher = (PathMatcher) plugin.paths();
            // If listing and traversing the contents of development directories (and skipping the copy) becomes
            // too expensive, we can add logic here wrt PathsInternal/PathsPrefix to specify a dirMatcher. Or,
            // properly route directly from the filesystem via Jetty.
            CopyHelper.copyRecursive(plugin.path(), dstPath, pathMatcher);
            plugins.add(plugin);
            writeManifest(fs);
        }
    }

    private void checkExisting(String name) {
        for (JsPlugin existing : plugins) {
            if (name.equals(existing.name())) {
                // TODO(deephaven-core#3048): Improve JS plugin support around plugins with conflicting names
                throw new IllegalArgumentException(String.format(
                        "js plugin with name '%s' already exists. See https://github.com/deephaven/deephaven-core/issues/3048",
                        existing.name()));
            }
        }
    }

    private synchronized void init() throws IOException {
        // Note: FileSystem#close is necessary to write out contents for ZipFileSystem
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of("create", "true"))) {
            writeManifest(fs);
        }
    }

    private void writeManifest(FileSystem fs) throws IOException {
        final Path manifestJson = manifestRoot(fs).resolve(MANIFEST_JSON);
        final Path manifestJsonTmp = manifestJson.resolveSibling(manifestJson.getFileName().toString() + ".tmp");
        // jackson impl does buffering internally
        try (final OutputStream out = Files.newOutputStream(manifestJsonTmp)) {
            OBJECT_MAPPER.writeValue(out, manifest());
            out.flush();
        }
        Files.move(manifestJsonTmp, manifestJson,
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.COPY_ATTRIBUTES,
                StandardCopyOption.ATOMIC_MOVE);
    }

    private JsPluginManifest manifest() {
        return JsPluginManifest.from(plugins);
    }

    private static Path manifestRoot(FileSystem fs) {
        return fs.getPath(ZIP_ROOT);
    }
}
