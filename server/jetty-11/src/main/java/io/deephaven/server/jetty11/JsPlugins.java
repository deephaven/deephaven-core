//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty11;

import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.js.JsPluginRegistration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.Objects;

/**
 * Jetty-specific implementation of {@link JsPluginRegistration} to collect plugins and advertise their contents to
 * connecting client.
 */
public class JsPlugins implements JsPluginRegistration {
    static final String JS_PLUGINS = "js-plugins";

    public static JsPlugins create() throws IOException {
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
    public void register(JsPlugin jsPlugin) {
        try {
            zipFs.add(jsPlugin);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
