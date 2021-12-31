package io.deephaven.server.plugin.python;

import io.deephaven.plugin.PluginCallback;

public final class Loader {
    public static void allRegisterInto(PluginCallback callback) {
        try (final Module module = Module.of()) {
            module.all_plugins_register_into(new CallbackAdapter(callback));
        }
    }
}
