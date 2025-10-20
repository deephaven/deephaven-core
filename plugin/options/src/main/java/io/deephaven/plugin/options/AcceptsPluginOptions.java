//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.options;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.Registration;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.type.ObjectType;

/**
 * If a plugin (of type {@link Registration}, {@link Plugin}, {@link JsPlugin} or {@link ObjectType}) implements this
 * interface, then after it is service loaded; the {@link #setPluginOptions} method is called to provide the
 * {@link PluginOptions} derived from dagger.
 */
public interface AcceptsPluginOptions {
    /**
     * Pass the dagger configured PluginOptions to this plugin.
     * 
     * @param pluginOptions the dagger configured PluginOptions
     */
    void setPluginOptions(PluginOptions pluginOptions);
}
