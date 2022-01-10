package io.deephaven.server.plugin.python;

import io.deephaven.plugin.Registration;
import io.deephaven.plugin.Registration.Callback;

public final class PythonModuleLoader {

    /**
     * Registers all {@link Registration plugins} found via python entrypoints "deephaven.plugin". See the
     * deephaven-plugin python package for more information.
     *
     * @param callback the plugin callback
     */
    public static void allRegisterInto(Callback callback) {
        try (final Module module = Module.of()) {
            module.all_plugins_register_into(new CallbackAdapter(callback));
        }
    }
}
