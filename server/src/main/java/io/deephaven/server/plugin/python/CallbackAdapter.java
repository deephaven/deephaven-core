/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.server.plugin.python;

import io.deephaven.plugin.Registration.Callback;
import io.deephaven.plugin.type.JsPluginInfo;
import io.deephaven.server.plugin.type.JsPluginDistribution;
import org.jpy.PyObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Objects;

class CallbackAdapter {

    private final Callback callback;

    public CallbackAdapter(Callback callback) {
        this.callback = Objects.requireNonNull(callback);
    }

    @SuppressWarnings("unused")
    public void registerObjectTypePlugin(String name, PyObject objectTypeAdapter) {
        callback.register(new ObjectTypeAdapter(name, objectTypeAdapter));
    }

    @SuppressWarnings("unused")
    public void registerJsPlugin(String path, String name, String version, String main) throws IOException {
        try {
            callback.register(JsPluginDistribution.of(Path.of(path), JsPluginInfo.of(name, version, main)));
        } catch (UncheckedIOException e) {
            throw e.getCause();
        }
    }
}
