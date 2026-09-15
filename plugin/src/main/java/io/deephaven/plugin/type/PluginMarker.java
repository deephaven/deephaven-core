//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.type;

/**
 * A generic marker object for plugin exports that can be used by multiple plugin types.
 * <p>
 * IMPORTANT: The pluginName field is required because ObjectTypeLookup.findObjectType() returns the FIRST plugin where
 * isType() returns true. Without plugin-specific identification in isType(), multiple plugins using PluginMarker would
 * conflict, and whichever is registered first would intercept all PluginMarker instances.
 * <p>
 * Markers are matched by pluginName rather than by identity. Code that exports markers is expected to reuse a single
 * instance per plugin name rather than creating one per request.
 */
public class PluginMarker {
    private final String pluginName;

    /**
     * Creates a marker for the given plugin name.
     *
     * @param pluginName the plugin name identifier (should match the plugin's name() method)
     * @throws IllegalArgumentException if pluginName is null or empty
     */
    public PluginMarker(String pluginName) {
        if (pluginName == null || pluginName.isEmpty()) {
            throw new IllegalArgumentException("pluginName cannot be null or empty");
        }
        this.pluginName = pluginName;
    }

    /**
     * Gets the plugin name this marker is intended for. This should match the ObjectType.name() of the target plugin.
     *
     * @return the plugin name identifier
     */
    public String getPluginName() {
        return pluginName;
    }

    @Override
    public String toString() {
        return "PluginMarker{pluginName='" + pluginName + "'}";
    }
}

