package io.deephaven.server.plugin.python;

import io.deephaven.plugin.Registration.Callback;
import org.jpy.PyObject;

class CallbackAdapter {

    private final Callback callback;

    public CallbackAdapter(Callback callback) {
        this.callback = callback;
    }

    @SuppressWarnings("unused")
    public void registerObjectType(String name, PyObject objectTypeAdapter) {
        callback.register(new ObjectTypeAdapter(name, objectTypeAdapter));
    }
}
