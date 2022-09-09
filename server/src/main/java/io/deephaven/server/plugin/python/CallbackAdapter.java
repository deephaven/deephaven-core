/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.python;

import io.deephaven.plugin.Registration.Callback;
import io.deephaven.server.plugin.type.JsTypePackageJson;
import org.jpy.PyObject;

import java.io.IOException;
import java.util.Objects;

class CallbackAdapter {

    private final Callback callback;

    public CallbackAdapter(Callback callback) {
        this.callback = Objects.requireNonNull(callback);
    }

    @SuppressWarnings("unused")
    public void registerObjectType(String name, PyObject objectTypeAdapter) {
        callback.register(new ObjectTypeAdapter(name, objectTypeAdapter));
    }

    @SuppressWarnings("unused")
    public void registerJsType(String path) throws IOException {
        callback.register(JsTypePackageJson.of(path));
    }
}
