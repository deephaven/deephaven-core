/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.python;

import io.deephaven.plugin.Registration.Callback;
import io.deephaven.plugin.js.JsPlugin;
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

    @SuppressWarnings("unused")
    public void registerJsPlugin(JsPlugin jsPlugin) {
        callback.register(jsPlugin);
    }
}
