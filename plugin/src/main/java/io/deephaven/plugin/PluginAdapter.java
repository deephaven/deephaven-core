package io.deephaven.plugin;

import java.util.Objects;

/**
 * Adapts a {@link Plugin} into a {@link Registration}.
 */
public final class PluginAdapter implements Registration {

    private final Plugin plugin;

    public PluginAdapter(Plugin plugin) {
        this.plugin = Objects.requireNonNull(plugin);
    }

    @Override
    public void registerInto(Callback callback) {
        callback.register(plugin);
    }

    @Override
    public String toString() {
        return "PluginAdapter(" + plugin + ")";
    }
}
