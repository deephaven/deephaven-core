/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.js;

import dagger.Binds;
import dagger.multibindings.IntoSet;
import io.deephaven.configuration.Configuration;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.js.JsPlugin;

import javax.inject.Inject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Registers the {@link JsPlugin JS plugins} sourced from the {@link JsPluginManifest manifest} root configuration
 * property {@value JsPluginManifestRegistration#JS_PLUGIN_RESOURCE_BASE}.
 */
public final class JsPluginManifestRegistration implements Registration {

    public static final String JS_PLUGIN_RESOURCE_BASE = JsPluginModule.DEEPHAVEN_JS_PLUGINS_PREFIX + "resourceBase";

    /**
     * Binds {@link JsPluginManifestRegistration} into the set of {@link Registration}.
     */
    @dagger.Module
    public interface Module {
        @Binds
        @IntoSet
        Registration bindsRegistration(JsPluginManifestRegistration registration);
    }

    @Inject
    JsPluginManifestRegistration() {}

    @Override
    public void registerInto(Callback callback) {
        // deephaven.jsPlugins.resourceBase (manifest root)
        final String resourceBase = Configuration.getInstance().getStringWithDefault(JS_PLUGIN_RESOURCE_BASE, null);
        if (resourceBase == null) {
            return;
        }
        final List<JsPlugin> plugins;
        try {
            plugins = JsPluginsFromManifest.of(Path.of(resourceBase));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        for (JsPlugin plugin : plugins) {
            callback.register(plugin);
        }
    }
}
