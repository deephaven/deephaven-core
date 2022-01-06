package io.deephaven.plugin;

import io.deephaven.plugin.type.ObjectTypeCallback;

/**
 * The plugin base. Implementations should override the appropriate registration methods as necessary.
 */
public abstract class PluginBase implements Plugin {

    @Override
    public final void registerInto(PluginCallback callback) {
        registerInto((ObjectTypeCallback) callback);
    }

    @Override
    public void registerInto(ObjectTypeCallback callback) {
        // override as necessary
    }
}
