package io.deephaven.server.plugin.python;

import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;

interface Module extends AutoCloseable {

    static Module of() {
        final PyModule module = PyModule.importModule("deephaven2.server.plugin");
        if (module == null) {
            throw new IllegalStateException("Unable to find `deephaven.server.plugin` module");
        }
        return (Module) module.createProxy(CallableKind.FUNCTION, Module.class);
    }

    void all_plugins_register_into(CallbackAdapter callback);

    @Override
    void close();
}
