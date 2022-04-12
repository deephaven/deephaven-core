package io.deephaven.plugin;

public abstract class PluginBase implements Plugin {

    @Override
    public final void registerInto(Callback callback) {
        callback.register(this);
    }
}
