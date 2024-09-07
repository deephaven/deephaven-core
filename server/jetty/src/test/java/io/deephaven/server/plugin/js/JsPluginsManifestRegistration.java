//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import io.deephaven.plugin.Registration;
import io.deephaven.plugin.js.JsPlugin;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

public class JsPluginsManifestRegistration implements Registration {

    private final Path path;

    public JsPluginsManifestRegistration(Path path) {
        this.path = Objects.requireNonNull(path);
    }

    @Override
    public void registerInto(Callback callback) {
        final List<JsPlugin> plugins;
        try {
            plugins = JsPluginsFromManifest.of(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (JsPlugin plugin : plugins) {
            callback.register(plugin);
        }
    }
}
