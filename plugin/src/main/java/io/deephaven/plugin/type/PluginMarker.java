//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.type;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A generic marker object for plugin exports that can be shared across multiple plugin types.
 * <p>
 * IMPORTANT: The pluginName field is required because ObjectTypeLookup.findObjectType()
 * returns the FIRST plugin where isType() returns true. Without plugin-specific identification
 * in isType(), multiple plugins using PluginMarker would conflict, and whichever is registered
 * first would intercept all PluginMarker instances.
 * <p>
 * This class uses a singleton pattern - one instance per pluginName.
 */
public class PluginMarker {
    private static final Map<String, PluginMarker> INSTANCES = new ConcurrentHashMap<>();

    private final String pluginName;

    /**
     * Private constructor - use forPluginName() to get singleton instances.
     *
     * @param pluginName the plugin name identifier (should match the plugin's name() method)
     */
    private PluginMarker(String pluginName) {
        this.pluginName = pluginName;
    }

    /**
     * Gets the singleton PluginMarker instance for the specified plugin name.
     *
     * @param pluginName the plugin name identifier (should match the plugin's name() method)
     * @return the singleton PluginMarker for this plugin name
     * @throws IllegalArgumentException if pluginName is null or empty
     */
    public static PluginMarker forPluginName(String pluginName) {
        if (pluginName == null || pluginName.isEmpty()) {
            throw new IllegalArgumentException("pluginName cannot be null or empty");
        }
        return INSTANCES.computeIfAbsent(pluginName, PluginMarker::new);
    }

    /**
     * Gets the plugin name this marker is intended for.
     * This should match the ObjectType.name() of the target plugin.
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

