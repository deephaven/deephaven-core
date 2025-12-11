//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.type;

/**
 * A generic marker object for plugin exports that can be shared across multiple plugin types.
 *
 * IMPORTANT: The pluginType field is required because ObjectTypeLookup.findObjectType()
 * returns the FIRST plugin where isType() returns true. Without plugin-specific identification
 * in isType(), multiple plugins using PluginMarker would conflict, and whichever is registered
 * first would intercept all PluginMarker instances.
 *
 * This class uses a singleton pattern - one instance per pluginType.
 */
public class PluginMarker {
    /**
     * Static type identifier for PluginMarker objects.
     * Uses the fully qualified class name as a standard cross-language identifier.
     */
    public static final String TYPE_ID = "io.deephaven.plugin.type.PluginMarker";

    private static final java.util.Map<String, PluginMarker> INSTANCES = new java.util.concurrent.ConcurrentHashMap<>();

    private final String pluginType;

    /**
     * Private constructor - use forPluginType() to get singleton instances.
     *
     * @param pluginType the plugin type identifier (should match the plugin's name() method)
     */
    private PluginMarker(String pluginType) {
        this.pluginType = pluginType;
    }

    /**
     * Gets the singleton PluginMarker instance for the specified plugin type.
     *
     * @param pluginType the plugin type identifier (should match the plugin's name() method)
     * @return the singleton PluginMarker for this plugin type
     * @throws IllegalArgumentException if pluginType is null or empty
     */
    public static PluginMarker forPluginType(String pluginType) {
        if (pluginType == null || pluginType.isEmpty()) {
            throw new IllegalArgumentException("pluginType cannot be null or empty");
        }
        return INSTANCES.computeIfAbsent(pluginType, PluginMarker::new);
    }

    /**
     * Gets the static type identifier for all PluginMarker objects.
     *
     * @return the static type identifier
     */
    public String getTypeId() {
        return TYPE_ID;
    }

    /**
     * Gets the plugin type this marker is intended for.
     * This should match the ObjectType.name() of the target plugin.
     *
     * @return the plugin type identifier
     */
    public String getPluginType() {
        return pluginType;
    }

    @Override
    public String toString() {
        return "PluginMarker{typeId='" + TYPE_ID + "', pluginType='" + pluginType + "'}";
    }
}

