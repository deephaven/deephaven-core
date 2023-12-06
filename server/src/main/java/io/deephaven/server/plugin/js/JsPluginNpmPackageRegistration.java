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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static io.deephaven.server.plugin.js.JsPluginManifestRegistration.JS_PLUGIN_RESOURCE_BASE;
import static io.deephaven.server.plugin.js.JsPluginModule.DEEPHAVEN_JS_PLUGINS_PREFIX;

/**
 * Registers the {@link JsPlugin JS plugins} sourced from the NPM package roots as specified via the configuration
 * properties that start with {@value io.deephaven.server.plugin.js.JsPluginModule#DEEPHAVEN_JS_PLUGINS_PREFIX}. This
 * configuration is meant for development-oriented use-cases.
 */
public final class JsPluginNpmPackageRegistration implements Registration {
    /**
     * Binds {@link JsPluginNpmPackageRegistration} into the set of {@link Registration}.
     */
    @dagger.Module
    public interface Module {
        @Binds
        @IntoSet
        Registration bindsRegistration(JsPluginNpmPackageRegistration registration);
    }

    @Inject
    JsPluginNpmPackageRegistration() {}

    @Override
    public void registerInto(Callback callback) {
        // deephaven.jsPlugins.<part> (package root)
        final Configuration config = Configuration.getInstance();
        final Set<String> parts = partsThatStartWith(DEEPHAVEN_JS_PLUGINS_PREFIX, config);
        for (String part : parts) {
            final String propertyName = DEEPHAVEN_JS_PLUGINS_PREFIX + part;
            if (JS_PLUGIN_RESOURCE_BASE.equals(propertyName)) {
                // handled by jsPluginsResourceBase
                continue;
            }
            final String packageRoot = config.getStringWithDefault(propertyName, null);
            if (packageRoot == null) {
                continue;
            }
            final JsPlugin plugin;
            try {
                plugin = JsPluginFromNpmPackage.of(Path.of(packageRoot));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            callback.register(plugin);
        }
    }

    private static Set<String> partsThatStartWith(String prefix, Configuration configuration) {
        final Set<String> parts = new HashSet<>();
        final Iterator<Object> it = configuration.getProperties(prefix).keys().asIterator();
        while (it.hasNext()) {
            final Object next = it.next();
            if (next instanceof String) {
                parts.add(firstPart((String) next));
            }
        }
        return parts;
    }

    private static String firstPart(String x) {
        final int index = x.indexOf('.');
        return index == -1 ? x : x.substring(0, index);
    }
}
