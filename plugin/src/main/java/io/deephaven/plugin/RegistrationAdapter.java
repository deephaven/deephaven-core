package io.deephaven.plugin;

import java.util.Objects;

/**
 * Adapts a {@link Plugin} into a {@link Registration}.
 */
public final class RegistrationAdapter implements Registration {

    private final Plugin plugin;

    public RegistrationAdapter(Plugin plugin) {
        this.plugin = Objects.requireNonNull(plugin);
    }

    @Override
    public void registerInto(Callback callback) {
        callback.register(plugin);
    }

    @Override
    public String toString() {
        return "RegistrationAdapter(" + plugin + ")";
    }
}
