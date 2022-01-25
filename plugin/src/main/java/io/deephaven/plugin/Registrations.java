package io.deephaven.plugin;

import java.util.Objects;

/**
 * A {@link Registration} that passes-through pre-built {@link Plugin plugins}.
 */
public class Registrations implements Registration {
    private final Iterable<? extends Plugin> plugins;

    public Registrations(Iterable<? extends Plugin> plugins) {
        this.plugins = Objects.requireNonNull(plugins);
    }

    @Override
    public final void registerInto(Callback callback) {
        for (Plugin plugin : plugins) {
            callback.register(plugin);
        }
    }
}
