package io.deephaven.server.plugin.python;

import io.deephaven.plugin.PluginCallback;
import org.jpy.PyObject;

class CallbackAdapter {

    private final PluginCallback callback;

    public CallbackAdapter(PluginCallback callback) {
        this.callback = callback;
    }

    @SuppressWarnings("unused")
    public void registerObjectType(String name, PyObject objectTypeAdapter) {
        callback.registerObjectType(new ObjectTypeAdapter(name, objectTypeAdapter));
    }
}
