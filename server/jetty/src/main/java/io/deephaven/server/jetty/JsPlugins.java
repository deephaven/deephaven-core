/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty;

import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.js.JsPlugin.Visitor;
import io.deephaven.plugin.js.JsPluginManifestPath;
import io.deephaven.plugin.js.JsPluginPackagePath;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.Objects;
import java.util.function.Consumer;

import static io.deephaven.server.jetty.Json.OBJECT_MAPPER;

class JsPlugins implements Consumer<JsPlugin> {
    static final String JS_PLUGINS = "js-plugins";

    static JsPlugins create() throws IOException {
        return new JsPlugins(JsPluginsZipFilesystem.create());
    }

    private final JsPluginsZipFilesystem zipFs;

    private JsPlugins(JsPluginsZipFilesystem zipFs) {
        this.zipFs = Objects.requireNonNull(zipFs);
    }

    public URI filesystem() {
        return zipFs.filesystem();
    }

    @Override
    public void accept(JsPlugin jsPlugin) {
        final IOException ioException = jsPlugin.walk(new CopyToZipFilesystem());
        if (ioException != null) {
            throw new UncheckedIOException(ioException);
        }
    }

    private class CopyToZipFilesystem implements Visitor<IOException> {

        @Override
        public IOException visit(JsPluginPackagePath srcPackagePath) {
            try {
                copy(srcPackagePath, zipFs);
                return null;
            } catch (IOException e) {
                return e;
            }
        }

        @Override
        public IOException visit(JsPluginManifestPath srcManifestPath) {
            try {
                copyAll(srcManifestPath, zipFs);
                return null;
            } catch (IOException e) {
                return e;
            }
        }
    }

    private static void copy(JsPluginPackagePath srcPackagePath, JsPluginsZipFilesystem dest)
            throws IOException {
        copy(srcPackagePath, dest, null);
    }

    private static void copy(JsPluginPackagePath srcPackagePath, JsPluginsZipFilesystem dest,
            JsPluginManifestEntry expected)
            throws IOException {
        final JsPluginManifestEntry srcEntry = entry(srcPackagePath);
        if (expected != null && !expected.equals(srcEntry)) {
            throw new IllegalStateException(String.format(
                    "Inconsistency between manifest.json and package.json, expected=%s, actual=%s", expected,
                    srcEntry));
        }
        dest.copyFrom(srcPackagePath, srcEntry);
    }

    private static void copyAll(JsPluginManifestPath srcManifestPath, JsPluginsZipFilesystem dest) throws IOException {
        final JsPluginManifest manifestInfo = manifest(srcManifestPath);
        for (JsPluginManifestEntry manifestEntry : manifestInfo.plugins()) {
            final JsPluginPackagePath packagePath = srcManifestPath.packagePath(manifestEntry.name());
            copy(packagePath, dest, manifestEntry);
        }
    }

    private static JsPluginManifest manifest(JsPluginManifestPath manifest) throws IOException {
        // jackson impl does buffering internally
        try (final InputStream in = Files.newInputStream(manifest.manifestJson())) {
            return OBJECT_MAPPER.readValue(in, JsPluginManifest.class);
        }
    }

    private static JsPluginManifestEntry entry(JsPluginPackagePath packagePath) throws IOException {
        // jackson impl does buffering internally
        try (final InputStream in = Files.newInputStream(packagePath.packageJson())) {
            return OBJECT_MAPPER.readValue(in, JsPluginManifestEntry.class);
        }
    }
}
