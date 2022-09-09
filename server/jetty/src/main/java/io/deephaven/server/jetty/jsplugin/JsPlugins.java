/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.jetty.jsplugin;

import io.deephaven.plugin.type.JsType;
import io.deephaven.plugin.type.JsTypeRegistration;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletHolder;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
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
        final JsPlugins jsPlugins = new JsPlugins(Files.createTempDirectory(JsPlugins.class.getName()));
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
        jsPlugins.setInitParameter("resourceBase", path.toString());
        jsPlugins.setInitParameter("pathInfoOnly", "true");
        jsPlugins.setInitParameter("dirAllowed", "true");
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
            out.write("{\"plugins\":[".getBytes(StandardCharsets.UTF_8));
            boolean isFirst = true;
            for (JsType value : plugins.values()) {
                if (!isFirst) {
                    out.write(',');
                }
                value.writeJsonPackageContentsTo(out);
                isFirst = false;
            }
            out.write("]}".getBytes(StandardCharsets.UTF_8));
            out.flush();
        }
    }
}
