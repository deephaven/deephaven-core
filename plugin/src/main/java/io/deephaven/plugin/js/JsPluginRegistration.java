//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.js;

/**
 * Observes registration of {@link JsPlugin} instances.
 */
public interface JsPluginRegistration {
    /**
     * Handles registration of a {@link JsPlugin} instance.
     * 
     * @param plugin the registered plugin
     */
    void register(JsPlugin plugin);
}
