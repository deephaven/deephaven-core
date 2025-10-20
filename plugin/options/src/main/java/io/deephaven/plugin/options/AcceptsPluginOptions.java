//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.options;

/**
 * If a plugin (of type {@link Registration}, @{link Plugin}, @{link JsPlugin} or @{link ObjectType}) implements this
 * interface, then after the {@link PluginModule} service loads the plugin, it calls the {@link #setPluginOptions}
 * method to provide the {@link PluginOptions} derived from dagger.
 */
public interface AcceptsPluginOptions {
    /**
     * Pass the dagger configured PluginOptions to this plugin.
     * 
     * @param pluginOptions the dagger configured PluginOptions
     */
    void setPluginOptions(PluginOptions pluginOptions);
}
