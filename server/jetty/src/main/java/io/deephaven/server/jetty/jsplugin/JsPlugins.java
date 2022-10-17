/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.deephaven.plugin.type.JsPlugin;
import io.deephaven.plugin.type.JsPluginRegistration;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link JsPluginRegistration} for Jetty.
 *
 * @see #servletHolder(String)
 */
public final class JsPlugins implements JsPluginRegistration {

    public static final String PLUGINS = "plugins";
    private static final String NAME = "name";
    private static final String VERSION = "version";
    private static final String MAIN = "main";
    private static final String MANIFEST_JSON = "manifest.json";

    /**
     * Creates a new js plugins instance with an in-memory filesystem.
     *
     * @return the js plugins
     * @throws IOException if an I/O exception occurs
     */
    public static JsPlugins create() throws IOException {
        final FileSystem fs = Jimfs.newFileSystem(Configuration.forCurrentPlatform());
        final JsPlugins jsPlugins = new JsPlugins(fs.getRootDirectories().iterator().next());
        jsPlugins.writeManifest(); // empty manifest
        return jsPlugins;
    }

    private final Path path;
    private final List<Map<String, String>> plugins;

    private JsPlugins(Path path) {
        this.path = Objects.requireNonNull(path);
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
        jsPlugins.setInitParameter("resourceBase", path.toUri().toString());
        jsPlugins.setInitParameter("pathInfoOnly", "true");
        jsPlugins.setInitParameter("dirAllowed", "false");
        jsPlugins.setInitParameter("etags", "true");
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
        jsPlugin.copyTo(path.resolve(jsPlugin.name()));
        plugins.add(Map.of(NAME, jsPlugin.name(), VERSION, jsPlugin.version(), MAIN, jsPlugin.main()));
        writeManifest();
    }

    private void writeManifest() throws IOException {
        // note: would this be better as a custom servlet instead? (this current way is certainly easy...)
        try (final OutputStream out = new BufferedOutputStream(Files.newOutputStream(path.resolve(MANIFEST_JSON)))) {
            new ObjectMapper().writeValue(out, Map.of(PLUGINS, plugins));
            out.flush();
        }
    }
}
