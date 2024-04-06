//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import io.deephaven.plugin.js.JsPlugin;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class JsPluginsFromManifest {

    static List<JsPlugin> of(Path manifestRoot) throws IOException {
        final JsPluginManifest manifest = JsPluginManifest.read(manifestRoot.resolve(JsPluginManifest.MANIFEST_JSON));
        final List<JsPlugin> plugins = new ArrayList<>(manifest.plugins().size());
        for (JsPluginManifestEntry entry : manifest.plugins()) {
            final Path pluginPath = manifestRoot.resolve(entry.name());
            final Path pluginMain = pluginPath.relativize(pluginPath.resolve(entry.main()));
            final JsPlugin plugin = JsPlugin.builder()
                    .name(entry.name())
                    .version(entry.version())
                    .main(pluginMain)
                    .path(pluginPath)
                    .build();
            // We expect manifests to be "production" use cases - they should already be packed as appropriate.
            // Additionally, there is no strict requirement that they have package.json anyways.
            plugins.add(plugin);
        }
        return plugins;
    }
}
