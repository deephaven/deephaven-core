//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.js;

import dagger.Module;
import io.deephaven.configuration.ConfigDir;

/**
 * Includes the modules {@link JsPluginManifestRegistration.Module}, {@link JsPluginConfigDirRegistration.Module}, and
 * {@link JsPluginNpmPackageRegistration.Module}; these modules add various means of configuration support for producing
 * and registering {@link io.deephaven.plugin.js.JsPlugin}.
 */
@Module(includes = {
        JsPluginManifestRegistration.Module.class,
        JsPluginConfigDirRegistration.Module.class,
        JsPluginNpmPackageRegistration.Module.class,
})
public interface JsPluginModule {

    String DEEPHAVEN_JS_PLUGINS_PREFIX = "deephaven.jsPlugins.";
}
