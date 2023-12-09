/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.js;

import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.js.JsPlugin.Builder;
import io.deephaven.plugin.js.Paths;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

class JsPluginFromNpmPackage {

    // https://docs.npmjs.com/cli/v6/configuring-npm/package-json#files
    private static final List<String> INCLUDED = List.of(
            "package.json",
            "README",
            "README.md",
            "LICENSE",
            "LICENSE.md",
            "LICENCE",
            "LICENCE.md",
            "CHANGES",
            "CHANGES.md",
            "CHANGELOG",
            "CHANGELOG.md",
            "HISTORY",
            "HISTORY.md",
            "NOTICE",
            "NOTICE.md");

    static JsPlugin of(Path packageRoot) throws IOException {
        final Path packageJsonPath = packageRoot.resolve(JsPluginNpmPackageRegistration.PACKAGE_JSON);
        final NpmPackage packageJson = NpmPackage.read(packageJsonPath);
        final Path main = packageRoot.relativize(packageRoot.resolve(packageJson.main()));
        final Paths paths;
        if (main.getNameCount() > 1) {
            // We're requiring that all of the necessary files (besides INCLUDED) be under the top-level directory
            // as sourced from package.json/main. Typically, "dist" or "build".
            // To be technically correct, we'd also need to read .npmignore and fully parse package.json/files, and
            // additionally ensure that any jetty routing matched up precisely - this is harder to do in general, so we
            // are making these simplifying assumptions.
            //
            // Note: this logic really only comes into play or development use cases where plugins are configured via
            // deephaven.jsPlugins.myPlugin=/path/to/my/js
            // For python delivering JS plugins, they'll be able to explicitly configure JsPlugin as appropriate; that
            // said, we may want to prefer they create JsPlugin via this code path for consistency purposes.
            final Path topLevelDirectory = main.subpath(0, 1);
            final List<Path> prefixes = new ArrayList<>(INCLUDED.size() + 2);
            prefixes.add(topLevelDirectory);
            prefixes.add(packageRoot.relativize(packageJsonPath));
            for (String included : INCLUDED) {
                prefixes.add(packageRoot.relativize(packageRoot.resolve(included)));
            }
            paths = Paths.ofPrefixes(prefixes);
        } else {
            paths = Paths.all();
        }
        final Builder builder = JsPlugin.builder()
                .name(packageJson.name())
                .version(packageJson.version())
                .main(main)
                .root(packageRoot)
                .paths(paths);
        return builder.build();
    }
}
