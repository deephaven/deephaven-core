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
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static io.deephaven.server.jetty.JsPlugins.MANIFEST_JSON;

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

    public synchronized void addFromPackageRoot(Path packageRootSrc, JsPlugin info) throws IOException {
        checkExisting(info);
        final Path distributionSrc = info.distributionDirFromPackageRoot(packageRootSrc);
        // TODO(deephaven-core#3005): js-plugins checksum-based caching
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of())) {
            final Path manifestRootDest = fs.getPath(ZIP_ROOT);
            final Path distributionDest = info.distributionDirFromManifestRoot(manifestRootDest);
            CopyHelper.copyRecursive(distributionSrc, distributionDest);
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
        final Path tmpManifestPath = fs.getPath(ZIP_ROOT, MANIFEST_JSON + ".tmp");
        // jackson impl does buffering internally
        try (final OutputStream out = Files.newOutputStream(tmpManifestPath)) {
            new ObjectMapper().writeValue(out, JsManifest.of(plugins));
            out.flush();
        }
        Files.move(tmpManifestPath, fs.getPath(ZIP_ROOT, MANIFEST_JSON),
                StandardCopyOption.REPLACE_EXISTING,
                StandardCopyOption.COPY_ATTRIBUTES,
                StandardCopyOption.ATOMIC_MOVE);
    }
}
