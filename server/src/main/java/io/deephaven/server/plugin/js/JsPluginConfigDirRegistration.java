/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.js;

import dagger.Binds;
import dagger.multibindings.IntoSet;
import io.deephaven.configuration.ConfigDir;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.js.JsPlugin;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static io.deephaven.server.plugin.js.JsPluginManifest.MANIFEST_JSON;


/**
 * Registers the {@link JsPlugin JS plugins} sourced from the {@link JsPluginManifest manifest} root located at
 * {@link ConfigDir} / {@value JS_PLUGINS} (if {@value io.deephaven.server.plugin.js.JsPluginManifest#MANIFEST_JSON}
 * exists).
 */
public final class JsPluginConfigDirRegistration implements Registration {

    public static final String JS_PLUGINS = "js-plugins";

    /**
     * Binds {@link JsPluginConfigDirRegistration} into the set of {@link Registration}.
     */
    @dagger.Module
    public interface Module {
        @Binds
        @IntoSet
        Registration bindsRegistration(JsPluginConfigDirRegistration registration);
    }

    @Inject
    JsPluginConfigDirRegistration() {}

    @Override
    public void registerInto(Callback callback) {
        // <configDir>/js-plugins/ (manifest root)
        final Path manifestRoot = ConfigDir.get()
                .map(p -> p.resolve(JS_PLUGINS).resolve(MANIFEST_JSON))
                .filter(Files::exists)
                .map(Path::getParent)
                .orElse(null);
        if (manifestRoot == null) {
            return;
        }
        final List<JsPlugin> plugins;
        try {
            plugins = JsPluginsFromManifest.of(manifestRoot);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (JsPlugin plugin : plugins) {
            callback.register(plugin);
        }
    }
}
