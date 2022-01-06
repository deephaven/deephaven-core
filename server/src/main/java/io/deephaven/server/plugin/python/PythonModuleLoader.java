package io.deephaven.server.plugin.python;

import io.deephaven.plugin.Plugin;
import io.deephaven.plugin.PluginCallback;

public final class PythonModuleLoader {

    /**
     * Registers all {@link Plugin plugins} found via python entrypoints "deephaven.plugin". See the deephaven-plugin
     * python package for more information.
     *
     * @param callback the plugin callback
     */
    public static void allRegisterInto(PluginCallback callback) {
        try (final Module module = Module.of()) {
            module.all_plugins_register_into(new CallbackAdapter(callback));
        }
    }
}
