package io.deephaven.plugin;

import io.deephaven.plugin.type.ObjectTypePlugin;

/**
 * The generic plugin interface which extends all of the specific plugin interfaces.
 *
 * @see ObjectTypePlugin
 */
public interface Plugin extends ObjectTypePlugin {

    /**
     * The generic registration entrypoint.
     *
     * @param callback the callback.
     */
    void registerInto(PluginCallback callback);
}
