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
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static io.deephaven.server.plugin.js.JsPluginManifestRegistration.JS_PLUGIN_RESOURCE_BASE;
import static io.deephaven.server.plugin.js.JsPluginModule.DEEPHAVEN_JS_PLUGINS_PREFIX;

/**
 * Registers the {@link JsPlugin JS plugins} sourced from the NPM package roots as specified via the configuration
 * properties that start with {@value io.deephaven.server.plugin.js.JsPluginModule#DEEPHAVEN_JS_PLUGINS_PREFIX}. This
 * configuration is meant for development-oriented use-cases and is done on a "best-effort" basis.
 *
 * <p>
 * The configuration value of the above property corresponds to the {@link JsPlugin#path()} directory. A
 * {@value PACKAGE_JSON} must exist in this directory (as specified via
 * <a href="https://docs.npmjs.com/cli/v6/configuring-npm/package-json">package-json</a>). The {@value NAME} json value
 * corresponds to {@link JsPlugin#name()}, the {@value VERSION} json value corresponds to {@link JsPlugin#version()},
 * and the {@value MAIN} json value corresponds to {@link JsPlugin#main()}. Furthermore, the top-level directory of the
 * {@value MAIN} json value will be used to set {@link JsPlugin#paths()} using
 * {@link io.deephaven.plugin.js.Paths#ofPrefixes(Path)}; for example, a {@value MAIN} of "build/index.js" will limit
 * the resources to the "build/" directory; a {@value MAIN} of "dist/bundle/index.js" will limit the resources to the
 * "dist/" directory.
 */
public final class JsPluginNpmPackageRegistration implements Registration {
    public static final String PACKAGE_JSON = "package.json";
    public static final String NAME = "name";
    public static final String VERSION = "version";
    public static final String MAIN = "main";

    // TODO(deephaven-core#4817): JS Plugins development is slow
    // We may wish to make parsing NPM package.json easier with a bespoke "deephaven" field, or try to exactly match
    // the semantics of the existing "files" field. This may not be necessary if the top-level directory of "main" works
    // well enough, or if we have a non-copying jetty route-based impl.

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
            URI uri = URI.create(packageRoot);
            if (uri.getScheme() == null) {
                uri = URI.create("file:" + packageRoot);
            }
            final FileSystem fileSystem = getOrCreateFileSystem(uri);
            final JsPlugin plugin;
            try {
                plugin = JsPluginFromNpmPackage.of(fileSystem.provider().getPath(uri));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            callback.register(plugin);
        }
    }

    private static FileSystem getOrCreateFileSystem(URI uri) {
        if ("file".equalsIgnoreCase(uri.getScheme())) {
            return FileSystems.getDefault();
        }
        try {
            return FileSystems.getFileSystem(uri);
        } catch (FileSystemNotFoundException e) {
            // ignore
        }
        try {
            return FileSystems.newFileSystem(uri, Map.of());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
