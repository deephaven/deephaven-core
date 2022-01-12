package io.deephaven.server.plugin.python;

import org.jpy.PyLib.CallableKind;
import org.jpy.PyModule;

interface Module extends AutoCloseable {

    String MODULE = "deephaven2.server.plugin";

    static Module of() {
        final PyModule module = PyModule.importModule(MODULE);
        if (module == null) {
            throw new IllegalStateException(String.format("Unable to find `%s` module", MODULE));
        }
        return (Module) module.createProxy(CallableKind.FUNCTION, Module.class);
    }

    void initialize_all_and_register_into(CallbackAdapter callback);

    @Override
    void close();
}
