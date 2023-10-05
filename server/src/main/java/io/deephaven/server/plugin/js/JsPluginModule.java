/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.js;

import dagger.Module;
import dagger.Provides;
import dagger.multibindings.ElementsIntoSet;
import io.deephaven.configuration.ConfigDir;
import io.deephaven.configuration.Configuration;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.js.JsPluginManifestPath;
import io.deephaven.plugin.js.JsPluginPackagePath;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * Provides the {@link JsPluginManifestPath manifest path} of {@value JS_PLUGIN_RESOURCE_BASE} if the configuration
 * property is set. Provides the {@link JsPluginManifestPath manifest path} of {@link ConfigDir} / {@value JS_PLUGINS}
 * if {@value JsPluginManifestPath#MANIFEST_JSON} exists. Provides the {@link JsPluginPackagePath package path} for all
 * configuration properties that start with {@value DEEPHAVEN_JS_PLUGINS_PREFIX} and have a single part after.
 */
@Module
public interface JsPluginModule {

    String DEEPHAVEN_JS_PLUGINS_PREFIX = "deephaven.jsPlugins.";
    String JS_PLUGIN_RESOURCE_BASE = DEEPHAVEN_JS_PLUGINS_PREFIX + "resourceBase";
    String JS_PLUGINS = "js-plugins";

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesResourceBaseRegistration() {
        return jsPluginsResourceBase()
                .map(Registration.class::cast)
                .map(Set::of)
                .orElseGet(Set::of);
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesConfigDirRegistration() {
        return jsPluginsConfigDir()
                .map(Registration.class::cast)
                .map(Set::of)
                .orElseGet(Set::of);
    }

    @Provides
    @ElementsIntoSet
    static Set<Registration> providesPackageRoots() {
        return Set.copyOf(jsPluginsPackageRoots());
    }

    // deephaven.jsPlugins.resourceBase (manifest root)
    private static Optional<JsPluginManifestPath> jsPluginsResourceBase() {
        final String resourceBase = Configuration.getInstance().getStringWithDefault(JS_PLUGIN_RESOURCE_BASE, null);
        return Optional.ofNullable(resourceBase)
                .map(Path::of)
                .map(JsPluginManifestPath::of);
    }

    // <configDir>/js-plugins/ (manifest root)
    private static Optional<JsPluginManifestPath> jsPluginsConfigDir() {
        return ConfigDir.get()
                .map(JsPluginModule::resolveJsPlugins)
                .map(JsPluginManifestPath::of)
                .filter(JsPluginModule::manifestJsonExists);
    }

    private static Path resolveJsPlugins(Path p) {
        return p.resolve(JS_PLUGINS);
    }

    private static boolean manifestJsonExists(JsPluginManifestPath path) {
        return Files.exists(path.manifestJson());
    }

    // deephaven.jsPlugins.<part> (package root)
    private static Set<JsPluginPackagePath> jsPluginsPackageRoots() {
        final Configuration config = Configuration.getInstance();
        final Set<String> parts = partsThatStartWith(DEEPHAVEN_JS_PLUGINS_PREFIX, config);
        final Set<JsPluginPackagePath> packageRoots = new HashSet<>(parts.size());
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
            packageRoots.add(JsPluginPackagePath.of(Path.of(packageRoot)));
        }
        return packageRoots;
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
