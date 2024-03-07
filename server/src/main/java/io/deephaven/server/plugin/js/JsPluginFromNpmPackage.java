//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.js.JsPlugin.Builder;
import io.deephaven.plugin.js.Paths;

import java.io.IOException;
import java.nio.file.Path;

class JsPluginFromNpmPackage {

    static JsPlugin of(Path packageRoot) throws IOException {
        final Path packageJsonPath = packageRoot.resolve(JsPluginNpmPackageRegistration.PACKAGE_JSON);
        final NpmPackage packageJson = NpmPackage.read(packageJsonPath);
        final Path main = packageRoot.relativize(packageRoot.resolve(packageJson.main()));
        final Paths paths;
        if (main.getNameCount() > 1) {
            // We're requiring that all of the necessary files to serve be under the top-level directory as sourced from
            // package.json/main. For example, "build/index.js" -> "build", "dist/bundle/index.js" -> "dist". This
            // supports development use cases where the top-level directory may be interspersed with unrelated
            // development files (node_modules, .git, etc).
            //
            // Note: this logic only comes into play for development use cases where plugins are configured via
            // deephaven.jsPlugins.myPlugin=/path/to/my/js
            paths = Paths.ofPrefixes(main.subpath(0, 1));
        } else {
            paths = Paths.all();
        }
        final Builder builder = JsPlugin.builder()
                .name(packageJson.name())
                .version(packageJson.version())
                .main(main)
                .path(packageRoot)
                .paths(paths);
        return builder.build();
    }
}
