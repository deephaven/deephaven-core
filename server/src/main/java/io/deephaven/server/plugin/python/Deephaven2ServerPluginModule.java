package io.deephaven.server.plugin.python;

import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;

interface Deephaven2ServerPluginModule extends AutoCloseable {

    String MODULE = "deephaven2.server.plugin";

    static Deephaven2ServerPluginModule of() {
        final PyModule module = PyModule.importModule(MODULE);
        if (module == null) {
            throw new IllegalStateException(String.format("Unable to find `%s` module", MODULE));
        }
        return (Deephaven2ServerPluginModule) module.createProxy(CallableKind.FUNCTION,
                Deephaven2ServerPluginModule.class);
    }

    void initialize_all_and_register_into(CallbackAdapter callback);

    @Override
    void close();
}
