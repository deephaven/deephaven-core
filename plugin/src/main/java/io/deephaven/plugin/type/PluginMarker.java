//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plugin.type;

/**
 * A generic marker object for plugin exports that can be shared across multiple plugin types.
 * The actual plugin routing is handled by TypedTicket.type, which is validated against
 * ObjectType.name() during the ConnectRequest phase. This marker simply indicates "this
 * exported object is a plugin placeholder" rather than containing actual plugin-specific data.
 */
public class PluginMarker {
    /**
     * Static type identifier for PluginMarker objects.
     * Uses the fully qualified class name as a standard cross-language identifier.
     */
    public static final String TYPE_ID = "io.deephaven.plugin.type.PluginMarker";

    /**
     * Singleton instance for all plugin marker exports.
     * Since plugin-specific routing is handled by TypedTicket.type, we don't need
     * per-plugin marker instances.
     */
    public static final PluginMarker INSTANCE = new PluginMarker();

    /**
     * Private constructor - use INSTANCE singleton.
     */
    private PluginMarker() {
    }

    /**
     * Gets the static type identifier for all PluginMarker objects.
     *
     * @return the static type identifier
     */
    public String getTypeId() {
        return TYPE_ID;
    }

    @Override
    public String toString() {
        return "PluginMarker{typeId='" + TYPE_ID + "'}";
    }
}

