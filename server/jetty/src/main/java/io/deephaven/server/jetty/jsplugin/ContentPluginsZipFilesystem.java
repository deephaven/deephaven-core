/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.plugin.type.ContentPlugin;
import io.deephaven.plugin.type.ContentPluginRegistration;
import io.deephaven.plugin.type.JsPlugin;
import io.deephaven.plugin.type.JsPluginInfo;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.BufferedOutputStream;
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

/**
 * A {@link ContentPlugins} and {@link ContentPluginRegistration} zip-based filesystem implementation for Jetty.
 */
public final class ContentPluginsZipFilesystem
        implements ContentPlugins, ContentPluginRegistration, ContentPlugin.Visitor<IOException> {

    private static final String PLUGINS = "plugins";
    private static final String MANIFEST_JSON = "manifest.json";

    /**
     * Creates a new js plugins instance with a temporary zip filesystem.
     *
     * @return the js plugins
     * @throws IOException if an I/O exception occurs
     */
    public static ContentPluginsZipFilesystem create() throws IOException {
        final Path tempDir = Files.createTempDirectory(ContentPluginsZipFilesystem.class.getName());
        tempDir.toFile().deleteOnExit();
        final Path fsZip = tempDir.resolve("deephaven-js-plugins.zip");
        fsZip.toFile().deleteOnExit();
        final URI uri = URI.create(String.format("jar:file:%s!/", fsZip));
        final ContentPluginsZipFilesystem jsPlugins = new ContentPluginsZipFilesystem(uri);
        jsPlugins.init();
        return jsPlugins;
    }

    private final URI filesystem;
    private final List<JsPluginInfo> plugins;

    private ContentPluginsZipFilesystem(URI filesystem) {
        this.filesystem = Objects.requireNonNull(filesystem);
        this.plugins = new ArrayList<>();
    }

    /**
     * Creates a {@link DefaultServlet} servlet holder.
     *
     * @param name the name
     * @return the servlet holder
     */
    @Override
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

    @Override
    public void register(ContentPlugin contentPlugin) throws IOException {
        final IOException e = contentPlugin.walk(this);
        if (e != null) {
            throw e;
        }
    }

    @Override
    public synchronized IOException visit(JsPlugin jsPlugin) {
        for (JsPluginInfo info : plugins) {
            if (jsPlugin.info().name().equals(info.name())) {
                // TODO(deephaven-core#3048): Improve JS plugin support around plugins with conflicting names
                throw new IllegalArgumentException(String.format(
                        "js plugin with name '%s' already exists. See https://github.com/deephaven/deephaven-core/issues/3048",
                        jsPlugin.info().name()));
            }
        }
        // TODO(deephaven-core#3005): js-plugins checksum-based caching
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of())) {
            final Path pluginPath = fs.getPath("/", jsPlugin.info().name());
            jsPlugin.copyTo(pluginPath);
            plugins.add(jsPlugin.info());
            writeManifest(fs);
        } catch (IOException e) {
            return e;
        }
        return null;
    }

    private void init() throws IOException {
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of("create", "true"))) {
            writeManifest(fs);
        }
    }

    private void writeManifest(FileSystem fs) throws IOException {
        final Path manifestPath = fs.getPath("/", MANIFEST_JSON);
        try (final OutputStream out = new BufferedOutputStream(Files.newOutputStream(manifestPath))) {
            new ObjectMapper().writeValue(out, Map.of(PLUGINS, plugins));
            out.flush();
        }
    }
}
