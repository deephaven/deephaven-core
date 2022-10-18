/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.deephaven.plugin.type.JsPlugin;
import io.deephaven.plugin.type.JsPluginRegistration;
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
 * A {@link JsPluginRegistration} for Jetty.
 *
 * @see #servletHolder(String)
 */
public final class JsPlugins implements JsPluginRegistration {

    private static final String PLUGINS = "plugins";
    private static final String NAME = "name";
    private static final String VERSION = "version";
    private static final String MAIN = "main";
    private static final String MANIFEST_JSON = "manifest.json";

    /**
     * Creates a new js plugins instance with a temporary zip filesystem.
     *
     * @return the js plugins
     * @throws IOException if an I/O exception occurs
     */
    public static JsPlugins create() throws IOException {
        final Path tempDir = Files.createTempDirectory(JsPlugins.class.getName());
        tempDir.toFile().deleteOnExit();
        final Path fsZip = tempDir.resolve("fs.zip");
        fsZip.toFile().deleteOnExit();
        // Note, the URI needs explicitly be parseable as a directory URL ending in "!/", a requirement of the jetty
        // resource creation implementation, see
        // org.eclipse.jetty.util.resource.Resource.newResource(java.lang.String, boolean)
        final URI uri = URI.create(String.format("jar:file:%s!/", fsZip));
        final JsPlugins jsPlugins = new JsPlugins(uri);
        jsPlugins.init();
        return jsPlugins;
    }

    private final URI filesystem;
    private final List<Map<String, String>> plugins;

    private JsPlugins(URI filesystem) {
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
        jsPlugins.setInitParameter("resourceBase", filesystem.toString());
        jsPlugins.setInitParameter("pathInfoOnly", "true");
        jsPlugins.setInitParameter("dirAllowed", "false");
        // etag support picked up via io.deephaven.server.jetty.JettyBackedGrpcServer
        // jsPlugins.setInitParameter("etags", "true");
        jsPlugins.setAsyncSupported(true);
        return jsPlugins;
    }

    @Override
    public synchronized void register(JsPlugin jsPlugin) throws IOException {
        for (Map<String, String> plugin : plugins) {
            if (jsPlugin.name().equals(plugin.get(NAME))) {
                throw new IllegalArgumentException(
                        String.format("js plugin with name '%s' already exists", jsPlugin.name()));
            }
        }
        try (final FileSystem fs = FileSystems.newFileSystem(filesystem, Map.of())) {
            final Path pluginPath = fs.getPath("/", jsPlugin.name());
            jsPlugin.copyTo(pluginPath);
            plugins.add(Map.of(NAME, jsPlugin.name(), VERSION, jsPlugin.version(), MAIN, jsPlugin.main()));
            writeManifest(fs);
        }
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
