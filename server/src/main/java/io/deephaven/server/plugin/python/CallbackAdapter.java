//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.plugin.python;

import io.deephaven.plugin.Registration.Callback;
import io.deephaven.plugin.js.JsPlugin;
import io.deephaven.plugin.options.PluginOptions;
import org.jpy.PyObject;

class CallbackAdapter {

    private final Callback callback;
    private final PluginOptions pluginOptions;

    public CallbackAdapter(Callback callback, PluginOptions pluginOptions) {
        this.callback = callback;
        this.pluginOptions = pluginOptions;
    }

    @SuppressWarnings("unused")
    public void registerObjectType(String name, PyObject objectTypeAdapter) {
        callback.register(new ObjectTypeAdapter(name, objectTypeAdapter));
    }

    /**
     * The authorization transformer, exposed so that Python plugins may manually apply the authorization transform to
     * objects they export to the client (see the {@code deephaven.plugin_authorization} Python module).
     */
    @SuppressWarnings("unused")
    public PluginOptions.AuthorizationTransformer authorizationTransformer() {
        return pluginOptions.authorizationTransformer();
    }

    @SuppressWarnings("unused")
    public void registerJsPlugin(JsPlugin jsPlugin) {
        callback.register(jsPlugin);
    }
}
