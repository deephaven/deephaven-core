package io.deephaven.server.jetty;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.configuration.CacheDir;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.server.jetty.JsPlugins.MANIFEST_JSON;

class JsPluginsZipFilesystem {

    private static final String PLUGINS = "plugins";
    private static final String ZIP_BASE = "/";

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
    private final List<JsPlugin> plugins;

    private JsPluginsZipFilesystem(URI filesystem) {
        this.filesystem = Objects.requireNonNull(filesystem);
        this.plugins = new ArrayList<>();
    }

    /**
     * Creates a {@link DefaultServlet} servlet holder.
     *
     * @param name the name
     * @return the servlet holder
     */
    public ServletHolder servletHolder(String name) {
        final ServletHolder jsPlugins = new ServletHolder(name, DefaultServlet.class);
        // Note, the URI needs explicitly be parseable as a directory URL ending in "!/", a requirement of the jetty
        // resource creation implementation, see
        // org.eclipse.jetty.util.resource.Resource.newResource(java.lang.String, boolean)
        jsPlugins.setInitParameter("resourceBase", filesystem.toString());
        jsPlugins.setInitParameter("pathInfoOnly", "true");
        jsPlugins.setInitParameter("dirAllowed", "false");
        jsPlugins.setAsyncSupported(true);
        return jsPlugins;
    }

    public synchronized void addFromManifestBase(Path srcManifestBase, JsPlugin info) throws IOException {
        checkExisting(info);
        final Path srcDist = info.distributionDirFromManifestBase(srcManifestBase);
        // TODO(deephaven-core#3005): js-plugins checksum-based caching
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of())) {
            final Path destManifestBase = fs.getPath(ZIP_BASE);
            final Path destDist = info.distributionDirFromManifestBase(destManifestBase);
            CopyHelper.copyRecursive(srcDist, destDist);
            plugins.add(info);
            writeManifest(fs);
        }
    }

    public synchronized void addFromPackageBase(Path srcPackageBase, JsPlugin info) throws IOException {
        checkExisting(info);
        final Path srcDist = info.distributionDirFromPackageBase(srcPackageBase);
        // TODO(deephaven-core#3005): js-plugins checksum-based caching
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of())) {
            final Path destManifestBase = fs.getPath(ZIP_BASE);
            final Path destDist = info.distributionDirFromManifestBase(destManifestBase);
            CopyHelper.copyRecursive(srcDist, destDist);
            plugins.add(info);
            writeManifest(fs);
        }
    }

    private void checkExisting(JsPlugin info) {
        for (JsPlugin existing : plugins) {
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
        final Path manifestPath = fs.getPath(ZIP_BASE, MANIFEST_JSON);
        // jackson impl does buffering internally
        try (final OutputStream out = Files.newOutputStream(manifestPath)) {
            new ObjectMapper().writeValue(out, Map.of(PLUGINS, plugins));
            out.flush();
        }
    }
}
