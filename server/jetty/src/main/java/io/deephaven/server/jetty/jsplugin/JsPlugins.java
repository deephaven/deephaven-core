/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import io.deephaven.plugin.type.JsType;
import io.deephaven.plugin.type.JsTypeRegistration;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link JsTypeRegistration} for Jetty.
 *
 * @see #servletHolder(String)
 */
public final class JsPlugins implements JsTypeRegistration {

    /**
     * Creates a new js plugins instance with a temporary directory.
     *
     * @return the js plugins
     * @throws IOException if an I/O exception occurs
     */
    public static JsPlugins create() throws IOException {
        final FileSystem fs = Jimfs.newFileSystem(Configuration.unix());
        final JsPlugins jsPlugins = new JsPlugins(fs.getPath("/"));
        jsPlugins.writeManifest(); // empty manifest
        return jsPlugins;
    }

    private final Path path;
    private final Map<String, JsType> plugins;

    private JsPlugins(Path path) {
        this.path = Objects.requireNonNull(path);
        this.plugins = new LinkedHashMap<>();
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
        jsPlugins.setAsyncSupported(true);
        return jsPlugins;
    }

    @Override
    public synchronized void register(JsType jsType) {
        if (plugins.containsKey(jsType.name())) {
            throw new IllegalArgumentException(
                    String.format("js plugin with name '%s' already exists", jsType.name()));
        }
        try {
            jsType.copyTo(path.resolve(jsType.name()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        plugins.put(jsType.name(), jsType);
        try {
            writeManifest();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void writeManifest() throws IOException {
        // note: would this be better as a custom servlet instead? (this current way is certainly easy...)
        try (final OutputStream out = new BufferedOutputStream(Files.newOutputStream(path.resolve("manifest.json")))) {
            final List<Map<String, String>> pluginsList = new ArrayList<>(plugins.size());
            for (JsType value : plugins.values()) {
                final Map<String, String> p = new LinkedHashMap<>(3);
                p.put("name", value.name());
                p.put("version", value.version());
                p.put("main", value.main());
                pluginsList.add(p);
            }
            new ObjectMapper().writeValue(out, Collections.singletonMap("plugins", pluginsList));
            out.flush();
        }
    }
}
